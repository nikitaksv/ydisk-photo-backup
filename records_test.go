package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnconfirmedRecordsAreNotFileFailuresOrDownloadWork(t *testing.T) {
	s := testStore(t)
	for _, raw := range []Object{
		{"path": "saved", "name": "saved.jpg"},
		{"path": "pending", "name": "pending.jpg", "type": "file"},
		{"path": "failed", "name": "failed.jpg", "meta": Object{"size": 100}},
		{"id": "ref-a", "itemId": "ref-a", "width": 1000, "height": 1000},
		{"path": "ref-b"},
	} {
		r, e := resource(raw, "day")
		if e != nil {
			t.Fatal(e)
		}
		if e = s.save(r); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.done(Result{ID: "saved", Quality: "preview", Bytes: 10}); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"failed", "ref-a", "ref-b"} {
		s.failed(id, errors.New("old download error"))
	}
	st, e := s.stats()
	if e != nil || st.Records != 5 || st.Total != 5 || st.Confirmed != 3 || st.Done != 1 || st.ConfirmedNoCopy != 2 || st.Unconfirmed != 2 || st.Failed != 1 || st.Pending != 1 || st.NoOriginal != 3 || st.NoPreview != 2 {
		t.Fatal(st, e)
	}
	for filter, want := range map[string]int{"all": 3, "confirmed": 3, "saved": 1, "no-copy": 2, "unconfirmed": 2, "records-all": 5, "failed": 1, "pending": 1, "missing-original": 3} {
		rows, n, e := s.list(filter, 0, 50)
		if e != nil || n != want {
			t.Fatal(filter, n, e)
		}
		if filter == "unconfirmed" {
			for _, r := range rows {
				if r.Confirmed || r.RecordState != "unconfirmed" || strings.Contains(r.Error, "old download error") {
					t.Fatal(r)
				}
			}
		}
	}
	q, e := s.downloadQueue("1=1")
	if e != nil || len(q) != 3 {
		t.Fatal(q, e)
	}
	for _, r := range q {
		if strings.HasPrefix(r.ID, "ref-") {
			t.Fatal("reference entered download workers", q)
		}
	}
	for _, id := range []string{"pending", "ref-a", "ref-b"} {
		if e = s.queueOriginalZIP(id); e != nil {
			t.Fatal(e)
		}
	}
	if s.zipStatus().Queued != 1 {
		t.Fatal(s.zipStatus())
	}
	z, e := s.zipQueue()
	if e != nil || len(z) != 3 {
		t.Fatal(z, e)
	}
	// A later metadata response promotes the same identifier, without dropping
	// the reference, counting a new file, or inventing a downloaded copy.
	r, _ := resource(Object{"path": "ref-a", "name": "real.jpg", "type": "file", "size": 123}, "day")
	if e = s.save(r); e != nil {
		t.Fatal(e)
	}
	st, e = s.stats()
	if e != nil || st.Records != 5 || st.Confirmed != 4 || st.Unconfirmed != 1 || st.Done != 1 || st.Pending != 2 || st.Failed != 1 {
		t.Fatal(st, e)
	}
}

func TestRESTReconciliationPreservesIdentityAndMissingReferences(t *testing.T) {
	s := testStore(t)
	for _, id := range []string{"/disk/a.jpg", "/disk/b.jpg", "/disk/c.jpg"} {
		r, _ := resource(Object{"id": id, "itemId": id, "width": 800}, "day")
		s.save(r)
	}
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "OAuth test-oauth-token" || r.Header.Get("Range") != "" {
			t.Error("wrong OAuth destination or REST Range")
		}
		if r.URL.Path != "/v1/disk/resources" {
			t.Fatal("unexpected path", r.URL.Path)
		}
		if r.URL.Query().Get("path") == "disk:/b.jpg" {
			w.WriteHeader(404)
			return
		}
		fmt.Fprint(w, `{"type":"file","name":"real.jpg","size":123,"md5":"01234567890123456789012345678901","created":"2024-01-02T03:04:05+03:00","preview":"https://preview.invalid/image"}`)
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), RESTBase: server.URL, WebDAVToken: "test-oauth-token"}
	n, e := c.reconcileREST(context.Background(), s)
	if e != nil || n != 3 || calls.Load() != 3 {
		t.Fatal(n, e, calls.Load())
	}
	st, e := s.stats()
	if e != nil || st.Records != 3 || st.Confirmed != 2 || st.Done != 0 || st.Unconfirmed != 1 {
		t.Fatal(st, e)
	}
	r, e := s.load("/disk/a.jpg")
	if e != nil || r.Size != 123 || str(r.Raw["itemId"]) != "/disk/a.jpg" || str(r.Raw["path"]) != "/disk/a.jpg" || len(r.Candidates) == 0 {
		t.Fatal(r.Size, e)
	}
	var result string
	if e = s.DB.QueryRow("SELECT result FROM record_checks WHERE file_id=?", "/disk/b.jpg").Scan(&result); e != nil || result != "http_404" {
		t.Fatal(result, e)
	}
	if ids, e := s.ids("1=1"); e != nil || len(ids) != 3 {
		t.Fatal("404 reference deleted", len(ids), e)
	}
}

func TestOriginalsRunBeforeDeferredRecordQueries(t *testing.T) {
	s := testStore(t)
	s.set("inventory", "complete")
	var originals atomic.Int32
	image := picture(700, 500)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/client/disk":
			fmt.Fprint(w, `{"sk":"test","uid":"123"}`)
		case "/preview/image":
			w.Write(image)
		case "/disk/image":
			originals.Add(1)
			w.Write(image)
		case "/v1/disk/resources":
			v, e := s.versions("known")
			if e != nil || v["original"].ID == "" || originals.Load() != 1 {
				t.Error("reference lookup delayed original download")
			}
			w.WriteHeader(404)
		case "/models-v2":
			json.NewEncoder(w).Encode(Object{"photoslice_id": "s"})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), Base: server.URL, RESTBase: server.URL, WebDAVToken: "test-oauth-token", Batch: 1}
	known, _ := resource(Object{"path": "known", "name": "known.png", "size": len(image), "meta": Object{"sizes": []any{Object{"name": "XL", "url": server.URL + "/preview/image"}, Object{"name": "ORIGINAL", "url": server.URL + "/disk/image"}}}}, "")
	ref, _ := resource(Object{"id": "/disk/missing.jpg", "itemId": "/disk/missing.jpg"}, "day")
	s.save(known)
	s.save(ref)
	e := runInventory(context.Background(), s, c, 2, "resume", func(string) {})
	if e == nil || originals.Load() != 1 {
		t.Fatal(e, originals.Load())
	}
	st, _ := s.stats()
	if st.Done != 1 || st.Unconfirmed != 1 || st.Failed != 0 {
		t.Fatal(st)
	}
}

func TestETAExcludesUnconfirmedReferences(t *testing.T) {
	now := time.Now()
	samples := []transferSample{{at: now.Add(-time.Minute), done: 10}, {at: now, done: 70}}
	r := transferRates(samples, Stats{Total: 3000, Records: 3000, Confirmed: 80, Done: 70, ConfirmedNoCopy: 10, Unconfirmed: 2920}, true, false)
	if r.Remaining != 10 || r.ETASeconds == nil || *r.ETASeconds != 10 || r.ETAScope != "confirmed_files" {
		t.Fatal(r)
	}
}

func TestLegacyCatalogMigrationKeepsCopiesAndReferenceClassification(t *testing.T) {
	root := t.TempDir()
	db, e := sql.Open("sqlite", filepath.Join(root, "state.db"))
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec(`CREATE TABLE files(id TEXT PRIMARY KEY,cluster TEXT NOT NULL,name TEXT NOT NULL,raw TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',result TEXT NOT NULL DEFAULT '{}',error TEXT NOT NULL DEFAULT '',updated TEXT NOT NULL)`)
	if e != nil {
		t.Fatal(e)
	}
	for _, row := range []struct{ id, raw, status, result string }{
		{"saved", `{"path":"saved"}`, "done", `{"id":"saved","quality":"preview","bytes":10}`},
		{"known", `{"path":"known","type":"file","size":100}`, "pending", `{}`},
		{"ref", `{"id":"ref","itemId":"ref","width":1000}`, "failed", `{}`},
	} {
		if _, e = db.Exec("INSERT INTO files(id,cluster,name,raw,status,result,updated) VALUES(?,'day',?,?,?,?,?)", row.id, row.id, row.raw, row.status, row.result, "now"); e != nil {
			t.Fatal(e)
		}
	}
	db.Close()
	s, e := openStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.DB.Close()
	st, e := s.stats()
	if e != nil || st.Records != 3 || st.Confirmed != 2 || st.Done != 1 || st.Unconfirmed != 1 || st.Failed != 0 || st.ConfirmedNoCopy != 1 {
		t.Fatal(st, e)
	}
	if e = s.DB.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = openStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.DB.Close()
	st, e = s.stats()
	if e != nil || st.Confirmed != 2 || st.Done != 1 || st.Unconfirmed != 1 {
		t.Fatal("classification changed after reopening", st, e)
	}
}

func TestFullClusterIndexDoesNotSkipMetadataPages(t *testing.T) {
	for _, windowedIndex := range []bool{false, true} {
		t.Run(fmt.Sprint(windowedIndex), func(t *testing.T) {
			s := testStore(t)
			var calls atomic.Int32
			c := apiFixture(t, func(d Object, _ string) Object {
				calls.Add(1)
				cl := obj(obj(d["requestParams"])["clusters"])
				requested := arr(obj(cl["day"])["range"])
				start, end := int(num(requested[0])), int(num(requested[1]))
				var index, fetched []any
				for i := 0; i < 367; i++ {
					id := fmt.Sprintf("/disk/%s-%d.jpg", fmt.Sprint(windowedIndex), i)
					if !windowedIndex || i >= start && i <= end {
						index = append(index, Object{"id": id, "itemId": id, "width": 1000})
					}
					if i >= start && i <= end {
						fetched = append(fetched, Object{"path": id, "name": "file.jpg", "type": "file", "size": 100})
					}
				}
				return Object{"clusters": Object{"fetched": []any{Object{"id": "day", "items": index}}}, "resources": Object{"fetched": fetched}}
			}, nil, nil)
			if e := c.collectCluster(context.Background(), s, "snapshot", "day", 367); e != nil {
				t.Fatal(e)
			}
			if calls.Load() != 2 {
				t.Fatal("metadata pages skipped or repeated", calls.Load())
			}
			st, e := s.stats()
			if e != nil || st.Unconfirmed != 0 || st.Confirmed != 367 {
				t.Fatal(st, e)
			}
		})
	}
}
