package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPreviewPassFallbackAndResumeWithoutOriginalRequests(t *testing.T) {
	s := testStore(t)
	var originalCalls, thumbnailCalls, bestCalls, smallerCalls atomic.Int32
	preview, thumb := picture(700, 500), picture(90, 90)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/client/disk":
			fmt.Fprint(w, `{"sk":"test","uid":"123"}`)
		case "/preview/best":
			bestCalls.Add(1)
			w.Write(preview)
		case "/preview/smaller":
			smallerCalls.Add(1)
			w.Write(picture(400, 400))
		case "/thumbnail/fallback":
			thumbnailCalls.Add(1)
			w.Write(thumb)
		default:
			if strings.HasPrefix(r.URL.Path, "/disk/") || r.URL.Path == "/models-v2" {
				originalCalls.Add(1)
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), Base: server.URL, Batch: 1, BatchOriginals: true}
	for _, id := range []string{"best", "fallback", "missing", "saved-original"} {
		previewURL := "/missing"
		if id == "best" {
			previewURL = "/preview/best"
		}
		thumbURL := "/missing"
		if id == "best" || id == "fallback" || id == "saved-original" {
			thumbURL = "/thumbnail/fallback"
		}
		sizes := []any{
			Object{"name": "ORIGINAL", "url": server.URL + "/disk/" + id},
			Object{"name": "XL", "url": server.URL + previewURL, "width": 700, "height": 500},
			Object{"name": "S", "url": server.URL + thumbURL, "width": 90, "height": 90},
		}
		if id == "best" {
			sizes = append(sizes, Object{"name": "M", "url": server.URL + "/preview/smaller", "width": 400, "height": 400})
		}
		r, _ := resource(Object{"path": id, "name": id + ".png", "meta": Object{"sizes": sizes}}, "")
		if e := s.save(r); e != nil {
			t.Fatal(e)
		}
	}
	// Existing originals survive and suppress an unnecessary fallback thumbnail.
	if e := s.done(Result{ID: "saved-original", Quality: "original", Bytes: 1}); e != nil {
		t.Fatal(e)
	}
	for pass := 0; pass < 2; pass++ {
		if e := runDownloadStage(context.Background(), s, c, 3, "previews", func(string) {}); e != nil {
			t.Fatal(e)
		}
	}
	st, e := s.stats()
	if e != nil || originalCalls.Load() != 0 || bestCalls.Load() != 1 || smallerCalls.Load() != 0 || thumbnailCalls.Load() != 1 || st.Preview != 1 || st.Thumbnail != 1 || st.Original != 1 || st.Pending != 1 || st.Failed != 0 {
		t.Fatalf("stats=%+v error=%v original=%d best=%d smaller=%d thumbnail=%d", st, e, originalCalls.Load(), bestCalls.Load(), smallerCalls.Load(), thumbnailCalls.Load())
	}
	var originalErrors int
	if e := s.DB.QueryRow("SELECT COUNT(*) FROM version_errors WHERE quality='original'").Scan(&originalErrors); e != nil || originalErrors != 0 {
		t.Fatal("preview pass recorded an original failure", originalErrors, e)
	}
}

func TestPreviewPassCoversFilesDiscoveredWhileDownloading(t *testing.T) {
	s := testStore(t)
	c := apiFixture(t, func(Object, string) Object { t.Fatal("preview pass requested an original"); return nil }, map[string][]byte{"/preview": picture(500, 400)}, nil)
	makeResource := func(id string) Resource {
		r, _ := resource(Object{"path": id, "name": id + ".png", "preview": c.Base + "/preview"}, "")
		return r
	}
	if e := s.save(makeResource("first")); e != nil {
		t.Fatal(e)
	}
	discovered := false
	if e := runDownloadStage(context.Background(), s, c, 2, "previews", func(msg string) {
		if strings.Contains(msg, "first.png · preview") && !discovered {
			discovered = true
			if e := s.save(makeResource("discovered")); e != nil {
				t.Error(e)
			}
		}
	}); e != nil {
		t.Fatal(e)
	}
	st, e := s.stats()
	if e != nil || st.Total != 2 || st.Preview != 2 || st.Original != 0 {
		t.Fatal("newly discovered file skipped before original pass", st, e)
	}
}
