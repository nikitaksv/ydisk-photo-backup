package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func queueFixture(previews, originals int) *liveQueue {
	q := &liveQueue{tasks: map[string]*liveTask{}, workers: 12}
	for stage, n := range []int{previews, originals} {
		for i := 0; i < n; i++ {
			name := []string{"previews", "originals"}[stage]
			t := &liveTask{id: fmt.Sprint(i), stage: name, order: int64(i)}
			q.tasks[taskKey(t.id, name)] = t
			heap.Push(&q.queues[stage][0], t)
		}
	}
	return q
}

func TestLiveWorkersSplitAndStealWithoutDuplicates(t *testing.T) {
	q := queueFixture(20, 20)
	seen := map[string]bool{}
	for i := 0; i < 12; i++ {
		tasks, zip := q.take(i%2, false, time.Now())
		if zip || len(tasks) != 1 || stageIndex(tasks[0].stage) != i%2 {
			t.Fatal("50/50 allocation", i, tasks)
		}
		key := taskKey(tasks[0].id, tasks[0].stage)
		if seen[key] {
			t.Fatal("duplicate dispatch", key)
		}
		seen[key] = true
	}
	st := q.snapshot()
	if st.PreviewWorkers != 6 || st.OriginalWorkers != 6 {
		t.Fatal(st)
	}
	for _, counts := range [][2]int{{0, 12}, {12, 0}} {
		q = queueFixture(counts[0], counts[1])
		expected := 0
		if counts[0] == 0 {
			expected = 1
		}
		for i := 0; i < 12; i++ {
			tasks, _ := q.take(i%2, false, time.Now())
			if len(tasks) != 1 || stageIndex(tasks[0].stage) != expected {
				t.Fatal("idle worker did not steal", i, counts)
			}
		}
	}
	q = queueFixture(1, 1)
	q.queues[0][0][0].due = time.Now().Add(time.Hour)
	tasks, _ := q.take(0, false, time.Now())
	if len(tasks) != 1 || tasks[0].stage != "originals" {
		t.Fatal("retry monopolized worker")
	}
}

func TestLiveZIPReservesBatchAndMissingFallsBackDirectly(t *testing.T) {
	s := testStore(t)
	q := queueFixture(1, 500)
	for i := 0; i < 500; i++ {
		r, _ := resource(Object{"type": "file", "path": fmt.Sprint(i), "name": fmt.Sprint(i) + ".jpg"}, "")
		if e := s.save(r); e != nil {
			t.Fatal(e)
		}
	}
	tasks, zip := q.take(1, true, time.Now())
	if !zip || len(tasks) != 500 {
		t.Fatal("ZIP did not reserve 500", len(tasks), zip)
	}
	stolen, _ := q.take(1, true, time.Now())
	if len(stolen) != 1 || stolen[0].stage != "previews" {
		t.Fatal("ZIP waiter did not help previews")
	}
	if e := s.done(Result{ID: "0", Quality: "original", Bytes: 1}); e != nil {
		t.Fatal(e)
	}
	if e := q.finish(s, tasks, true, errZIPDisabled); e != nil {
		t.Fatal(e)
	}
	st := q.snapshot()
	if st.Originals != 499 || st.OriginalWorkers != 0 {
		t.Fatal(st)
	}
	for i := 0; i < 499; i++ {
		batch, useZIP := q.take(1, true, time.Now())
		if useZIP || len(batch) != 1 {
			t.Fatal("missing ZIP item was not sent directly", i)
		}
	}
}

func TestLivePreviewAndOriginalOverlapAndCancel(t *testing.T) {
	s := testStore(t)
	var calls atomic.Int32
	started := make(chan string, 4)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		started <- r.URL.Path
		<-r.Context().Done()
	}))
	defer server.Close()
	r, _ := resource(Object{"type": "file", "path": "a", "name": "a.png", "meta": Object{"sizes": []any{
		Object{"name": "XL", "url": server.URL + "/preview/a", "width": 500, "height": 500},
		Object{"name": "ORIGINAL", "url": server.URL + "/disk/a"},
	}}}, "")
	if e := s.save(r); e != nil {
		t.Fatal(e)
	}
	if _, e := s.DB.Exec(`CREATE TABLE IF NOT EXISTS download_retries(file_id TEXT,quality TEXT,attempts INTEGER,next_attempt INTEGER,PRIMARY KEY(file_id,quality))`); e != nil {
		t.Fatal(e)
	}
	q := &liveQueue{tasks: map[string]*liveTask{}, workers: 2}
	if e := q.reload(s); e != nil {
		t.Fatal(e)
	}
	c := &Client{HTTP: server.Client(), Base: server.URL, BatchOriginals: true, Continuous: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runLiveWorkers(ctx, s, c, q, 2, func(string) {}) }()
	paths := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case p := <-started:
			paths[p] = true
		case <-time.After(3 * time.Second):
			t.Fatal("queues did not overlap", paths)
		}
	}
	if !paths["/preview/a"] || !paths["/disk/a"] {
		t.Fatal(paths)
	}
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("workers leaked on cancellation")
	}
}

func TestLiveZIPDisabledNeverCallsArchiveAPIAndRetriesPersist(t *testing.T) {
	s := testStore(t)
	data := picture(400, 400)
	var archives atomic.Int32
	c := apiFixture(t, func(Object, string) Object { archives.Add(1); return Object{} }, map[string][]byte{"/preview/a": data}, nil)
	c.BatchOriginals = true
	c.Continuous = true
	r, _ := resource(Object{"type": "file", "path": "a", "name": "a.png", "preview": c.Base + "/preview/a"}, "")
	s.save(r)
	s.set("inventory_next", time.Now().Add(time.Hour).Format(time.RFC3339Nano))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runContinuous(ctx, s, c, 4, func(string) {}) }()
	deadline := time.After(5 * time.Second)
	for {
		var n int
		if e := s.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='download_retries'").Scan(&n); e != nil {
			t.Fatal(e)
		}
		if n > 0 {
			s.DB.QueryRow("SELECT COUNT(*) FROM download_retries WHERE quality='original' AND attempts=1 AND next_attempt>?", time.Now().UnixNano()).Scan(&n)
			st, _ := s.stats()
			if n == 1 && st.Preview == 1 {
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("retry deadline not persisted")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if archives.Load() != 0 {
		t.Fatal("disabled ZIP made a server request", archives.Load())
	}
	st, _ := s.stats()
	if st.Preview != 1 {
		t.Fatal(st)
	}
	q := &liveQueue{tasks: map[string]*liveTask{}}
	if e := q.reload(s); e != nil {
		t.Fatal(e)
	}
	if tasks, _ := q.take(1, false, time.Now()); len(tasks) != 0 {
		t.Fatal("restart ignored retry deadline")
	}
}

func TestLiveZIPPartialArchiveKeepsSuccessAndDirectFallback(t *testing.T) {
	s := testStore(t)
	image := picture(400, 300)
	var zipCalls atomic.Int32
	c := apiFixture(t, func(_ Object, url string) Object { zipCalls.Add(1); return Object{"download_url": url + "/zip"} }, map[string][]byte{"/zip": zipBytes("a.png", image), "/disk/b": image}, nil)
	c.BatchOriginals = true
	c.Continuous = true
	c.ZIPEnabled.Store(true)
	var resources []Resource
	for _, id := range []string{"a", "b"} {
		r, _ := resource(Object{"type": "file", "path": id, "name": id + ".png", "size": len(image), "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": c.Base + "/disk/" + id}}}}, "")
		s.save(r)
		resources = append(resources, r)
	}
	if e := liveZIP(context.Background(), s, c, resources, func(string) {}); e != nil {
		t.Fatal(e)
	}
	if e := c.processBatchStage(context.Background(), s, resources, "originals", func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Original != 2 || st.ZIPArchives != 1 || zipCalls.Load() != 1 {
		t.Fatal(st, zipCalls.Load())
	}
}

func TestLiveNewFilesJoinWhileWorkerIsBusy(t *testing.T) {
	s := testStore(t)
	if _, e := s.DB.Exec(`CREATE TABLE IF NOT EXISTS download_retries(file_id TEXT,quality TEXT,attempts INTEGER,next_attempt INTEGER,PRIMARY KEY(file_id,quality))`); e != nil {
		t.Fatal(e)
	}
	r, _ := resource(Object{"type": "file", "path": "old", "name": "old.jpg"}, "")
	s.save(r)
	q := &liveQueue{tasks: map[string]*liveTask{}}
	q.reload(s)
	first, _ := q.take(0, false, time.Now())
	if len(first) != 1 {
		t.Fatal(first)
	}
	r, _ = resource(Object{"type": "file", "path": "new", "name": "new.jpg"}, "")
	s.save(r)
	if e := q.reload(s); e != nil {
		t.Fatal(e)
	}
	tasks, _ := q.take(0, false, time.Now())
	if len(tasks) != 1 || tasks[0].id != "new" {
		t.Fatal("new file waited for a pass", tasks)
	}
}

func TestLiveInventoryRefreshDoesNotPauseMedia(t *testing.T) {
	s := testStore(t)
	data := picture(400, 300)
	scanning := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models-v2" {
			select {
			case scanning <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		select {
		case <-scanning:
			scanning <- struct{}{}
		case <-r.Context().Done():
			return
		}
		w.Write(data)
	}))
	defer server.Close()
	defer close(release)
	c := &Client{HTTP: server.Client(), Base: server.URL, BatchOriginals: true, Continuous: true}
	r, _ := resource(Object{"type": "file", "path": "a", "name": "a.png", "size": len(data), "meta": Object{"sizes": []any{
		Object{"name": "XL", "url": server.URL + "/preview/a"}, Object{"name": "ORIGINAL", "url": server.URL + "/disk/a"},
	}}}, "")
	s.save(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runContinuous(ctx, s, c, 2, func(string) {}) }()
	deadline := time.After(4 * time.Second)
	for {
		st, _ := s.stats()
		if st.Original == 1 && st.Preview == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("metadata refresh blocked media")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("metadata worker leaked")
	}
}

func TestIncrementalCatalogRefreshesChangedNewAndIncompleteClusters(t *testing.T) {
	s := testStore(t)
	for _, id := range []string{"stable", "grown", "revised", "unknown"} {
		r, _ := resource(Object{"type": "file", "path": id + "/0", "name": "0.jpg", "size": 10}, id)
		if id == "unknown" {
			r, _ = resource(Object{"path": id + "/0"}, id)
		}
		s.save(r)
		s.set("cluster_size:"+id, "1")
		s.set("cluster_revision:"+id, "version=1;")
	}
	var requested []string
	c := apiFixture(t, func(d Object, _ string) Object {
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			return Object{"photoslice_id": "snapshot"}
		case "intapi/photo-get-snapshot":
			return Object{"total": 5, "items": []any{
				Object{"id": "stable", "size": 1, "version": "1"}, Object{"id": "grown", "size": 2, "version": "1"},
				Object{"id": "revised", "size": 1, "version": "2"}, Object{"id": "unknown", "size": 1, "version": "1"}, Object{"id": "new", "size": 1, "version": "1"},
			}}
		default:
			clusters := obj(obj(d["requestParams"])["clusters"])
			var id string
			for key := range clusters {
				id = key
			}
			requested = append(requested, id)
			count := 1
			if id == "grown" {
				count = 2
			}
			var files, refs []any
			for i := 0; i < count; i++ {
				path := fmt.Sprintf("%s/%d", id, i)
				files = append(files, Object{"path": path, "type": "file", "name": "a.jpg", "size": 10})
				refs = append(refs, path)
			}
			return Object{"clusters": Object{"fetched": []any{Object{"id": id, "items": refs}}}, "resources": Object{"fetched": files}}
		}
	}, nil, nil)
	if e := c.collectCatalog(context.Background(), s, true, func(string) {}); e != nil {
		t.Fatal(e)
	}
	if len(requested) != 4 {
		b, _ := json.Marshal(requested)
		t.Fatal(string(b))
	}
	for _, id := range requested {
		if id == "stable" {
			t.Fatal("unchanged cluster was fetched")
		}
	}
	st, _ := s.stats()
	if st.Confirmed != 6 || st.Unconfirmed != 0 || st.Inventory != "complete" {
		t.Fatal(st)
	}
}
