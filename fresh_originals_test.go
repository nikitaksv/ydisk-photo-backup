package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFreshOriginalsRenewExpiredLinksOncePerCluster(t *testing.T) {
	s := testStore(t)
	data := picture(600, 400)
	var initCalls, clusterCalls atomic.Int32
	var c *Client
	c = apiFixture(t, func(d Object, base string) Object {
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			initCalls.Add(1)
			return Object{"photoslice_id": "snapshot"}
		case "intapi/photo-get-clusters-with-resources":
			clusterCalls.Add(1)
			return Object{"resources": Object{"fetched": []any{Object{"path": "photo", "name": "photo.png", "size": len(data), "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": base + "/disk/fresh"}}}}}}, "clusters": Object{"fetched": []any{Object{"id": "day", "items": []any{"photo"}}}}}
		default:
			t.Error("unexpected method", str(d["apiMethod"]))
			return nil
		}
	}, map[string][]byte{"/disk/fresh": data}, nil)
	c.BatchOriginals = true
	c.FreshLinks = newLinkRefresher()
	r, _ := resource(Object{"path": "photo", "name": "photo.png", "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": c.Base + "/disk/" + strings.Repeat("a", 64) + "/" + fmt.Sprintf("%08x", time.Now().Add(-time.Hour).Unix()) + "/old"}}}}, "day")
	if e := s.save(r); e != nil {
		t.Fatal(e)
	}
	if e := s.set("cluster_size:day", "1"); e != nil {
		t.Fatal(e)
	}
	if e := s.done(Result{ID: r.ID, Quality: "preview", Bytes: 123}); e != nil {
		t.Fatal(e)
	}
	if e := s.done(Result{ID: r.ID, Quality: "thumbnail", Bytes: 45}); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, e := c.FreshLinks.refresh(context.Background(), c, s, []Resource{r})
			if e != nil || len(got) != 1 || got[0].Candidates[0].URL != c.Base+"/disk/fresh" {
				t.Errorf("renewal failed: %v", e)
			}
		}()
	}
	wg.Wait()
	if initCalls.Load() != 1 || clusterCalls.Load() != 1 {
		t.Fatal("duplicate refresh", initCalls.Load(), clusterCalls.Load())
	}
	if e := c.processBatch(context.Background(), s, []Resource{r}, func(string) {}); e != nil {
		t.Fatal(e)
	}
	versions, e := s.versions(r.ID)
	if e != nil || len(versions) != 3 || versions["original"].Bytes != int64(len(data)) || versions["preview"].Bytes != 123 || versions["thumbnail"].Bytes != 45 {
		t.Fatal("old copies lost or original missing", versions, e)
	}
}

func TestFreshOriginalRefreshFailureQueuesInsteadOfIndividualZIP(t *testing.T) {
	s := testStore(t)
	c := apiFixture(t, func(d Object, base string) Object {
		if str(d["apiMethod"]) == "intapi/photo-init-snapshot" {
			return Object{"error": Object{"code": "temporary"}}
		}
		t.Error("unexpected individual ZIP")
		return nil
	}, map[string][]byte{"/preview/p": picture(600, 400)}, nil)
	c.BatchOriginals = true
	c.FreshLinks = newLinkRefresher()
	r, _ := resource(Object{"path": "photo", "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": c.Base + "/disk/old"}, Object{"name": "XL", "url": c.Base + "/preview/p"}}}}, "day")
	s.save(r)
	if e := c.processBatch(context.Background(), s, []Resource{r}, func(string) {}); e != nil {
		t.Fatal(e)
	}
	v, _ := s.versions(r.ID)
	if _, ok := v["original"]; ok {
		t.Fatal("stale original accepted")
	}
	if _, ok := v["preview"]; !ok {
		t.Fatal("preview not rescued")
	}
	if s.zipStatus().Queued != 1 {
		t.Fatal("missing persistent ZIP queue")
	}
}

func TestOriginalZIPAccumulates500AndKeepsPartialSuccess(t *testing.T) {
	s := testStore(t)
	var calls atomic.Int32
	var count atomic.Int32
	data := picture(400, 400)
	c := apiFixture(t, func(d Object, base string) Object {
		if str(d["apiMethod"]) != "mpfs/bulk-download-prepare" {
			t.Error("unexpected method")
		}
		calls.Add(1)
		count.Store(int32(len(arr(obj(d["requestParams"])["items"]))))
		return Object{"download_url": base + "/batch"}
	}, map[string][]byte{"/batch": zipBytes("photo-0.png", data)}, nil)
	c.BatchOriginals = true
	c.zipFallbackStart = time.Now().UnixNano()
	for i := 0; i < 500; i++ {
		r := Resource{ID: fmt.Sprint(i), Name: fmt.Sprintf("photo-%d.png", i), Raw: Object{"path": fmt.Sprint(i), "name": fmt.Sprintf("photo-%d.png", i)}}
		s.save(r)
		s.queueOriginalZIP(r.ID)
		if i == 498 {
			if e := c.drainOriginalZIP(context.Background(), s, false, false, func(string) {}); e != nil || calls.Load() != 0 {
				t.Fatal("ZIP started before 500", e)
			}
		}
	}
	if e := c.drainOriginalZIP(context.Background(), s, false, false, func(string) {}); e != nil {
		t.Fatal(e)
	}
	if count.Load() != 500 || calls.Load() != 1 {
		t.Fatal("wrong batch", count.Load(), calls.Load())
	}
	st, _ := s.stats()
	if st.Original != 1 || st.Done != 1 || s.zipStatus().Missing != 499 || s.zipStatus().Queued != 499 {
		t.Fatal("partial success discarded", st, s.zipStatus())
	}
	if e := c.drainOriginalZIP(context.Background(), s, true, false, func(string) {}); e != nil || calls.Load() != 1 {
		t.Fatal("missing members retried in same pass", e)
	}
	// A later pass must get a fresh server ZIP, not silently reuse the old
	// incomplete archive for an identical set of IDs.
	b := newFallbackZIPBatch([]Resource{{ID: "x"}, {ID: "y"}}, 1)
	if b.ID == newFallbackZIPBatch([]Resource{{ID: "x"}, {ID: "y"}}, 2).ID {
		t.Fatal("stale ZIP reused across passes")
	}
}

func TestOriginalZIPFlushesTailAndNeverSingleFile(t *testing.T) {
	s := testStore(t)
	var calls atomic.Int32
	c := apiFixture(t, func(d Object, base string) Object {
		calls.Add(1)
		if len(arr(obj(d["requestParams"])["items"])) != 2 {
			t.Error("expected tail of two")
		}
		return Object{"download_url": base + "/batch"}
	}, map[string][]byte{"/batch": zipBytes("a.png", picture(400, 400))}, nil)
	c.BatchOriginals = true
	for _, name := range []string{"a.png", "b.png"} {
		s.save(Resource{ID: name, Name: name, Raw: Object{"path": name, "name": name}})
		s.queueOriginalZIP(name)
	}
	if e := c.drainOriginalZIP(context.Background(), s, false, false, func(string) {}); e != nil || calls.Load() != 0 {
		t.Fatal(e)
	}
	if e := c.drainOriginalZIP(context.Background(), s, true, false, func(string) {}); e != nil || calls.Load() != 1 {
		t.Fatal(e)
	}
	s.save(Resource{ID: "c", Name: "c.png", Raw: Object{"path": "c", "name": "c.png"}})
	s.queueOriginalZIP("c")
	if e := c.drainOriginalZIP(context.Background(), s, true, false, func(string) {}); e != nil || calls.Load() != 1 {
		t.Fatal("individual ZIP requested", e)
	}
}

func TestOriginalZIPQueuePersistsAnd429DoesNotFailFiles(t *testing.T) {
	s := testStore(t)
	c := apiFixture(t, func(d Object, base string) Object { return nil }, nil, map[string]int{"/models-v2": 429})
	// apiFixture handles models-v2 itself; use a transport to return 429.
	c.HTTP.Transport = originalTestTransport(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}")), Request: req}, nil
	})
	c.BatchOriginals = true
	for _, id := range []string{"a", "b"} {
		s.save(Resource{ID: id, Name: id + ".png", Raw: Object{"path": id, "name": id + ".png"}})
		s.queueOriginalZIP(id)
	}
	if e := c.drainOriginalZIP(context.Background(), s, true, false, func(string) {}); !errors.Is(e, errZIPDeferred) {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Failed != 0 || s.zipStatus().Queued != 2 || c.rateStatus().Events != 1 {
		t.Fatal("429 treated as file absence", st, s.zipStatus())
	}
	var errorsCount int
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors").Scan(&errorsCount)
	if errorsCount != 0 {
		t.Fatal("429 stored as file error")
	}
	root := s.Root
	s.DB.Close()
	reopened, e := openStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.DB.Close()
	if reopened.zipStatus().Queued != 2 {
		t.Fatal("queue lost on restart")
	}
}

func TestOriginalZIPRecoversDownloadedArchiveOffline(t *testing.T) {
	s := testStore(t)
	data := picture(400, 400)
	r := Resource{ID: "a", Name: "a.png", Raw: Object{"path": "a", "name": "a.png"}}
	s.save(r)
	s.queueOriginalZIP(r.ID)
	b := newFallbackZIPBatch([]Resource{r}, 1)
	s.rememberZIPBatch(b)
	full, _ := localArchive(s.Root, b.Archive)
	os.MkdirAll(filepath.Dir(full), 0700)
	os.WriteFile(full, zipBytes("a.png", data), 0600)
	if e := recoverOriginalZIP(context.Background(), s, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Original != 1 || s.zipStatus().Queued != 0 {
		t.Fatal("offline archive not recovered", st)
	}
}

func TestOriginalSizedPreviewIsNotOriginal(t *testing.T) {
	data := picture(600, 400)
	c := apiFixture(t, func(Object, string) Object { t.Error("individual ZIP requested"); return nil }, map[string][]byte{"/preview/large": data}, nil)
	c.BatchOriginals = true
	r, _ := resource(Object{"path": "a", "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": c.Base + "/preview/large?size=ORIGINAL"}}}}, "")
	if len(r.Candidates) != 1 || r.Candidates[0].Quality != "preview" {
		t.Fatal("wrong classification", r.Candidates)
	}
	if _, e := c.downloadSelected(context.Background(), testStore(t), r, "original"); e == nil {
		t.Fatal("preview mislabeled original")
	}
	got, e := c.downloadSelected(context.Background(), testStore(t), r, "preview")
	if e != nil || got.Quality != "preview" {
		t.Fatal(got, e)
	}
}

func TestOriginalZIPSeparatesDuplicatesAndVideos(t *testing.T) {
	s := testStore(t)
	for _, r := range []Resource{{ID: "a", Name: "same.png"}, {ID: "b", Name: "same.png"}, {ID: "v", Name: "video.mp4"}} {
		r.Raw = Object{"path": r.ID, "name": r.Name}
		s.save(r)
		s.queueOriginalZIP(r.ID)
	}
	photos, e := s.queuedOriginals(time.Now().UnixNano(), false)
	if e != nil || len(photos) != 2 {
		t.Fatal(photos, e)
	}
	videos, e := s.queuedOriginals(time.Now().UnixNano(), true)
	if e != nil || len(videos) != 1 {
		t.Fatal(videos, e)
	}
	if len(splitZIPBatches(photos, 500)) != 2 {
		t.Fatal("duplicate names combined")
	}
}

type originalTestTransport func(*http.Request) (*http.Response, error)

func (f originalTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
