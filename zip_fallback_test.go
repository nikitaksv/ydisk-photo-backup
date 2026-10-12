package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestZIPMode429FallsBackAfterPreviewsWithoutExtraThumbnails(t *testing.T) {
	s := testStore(t)
	s.set("inventory", "complete")
	original := picture(800, 600)
	preview := picture(400, 400)
	thumb := picture(100, 100)
	var prepares atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/client/disk":
			fmt.Fprint(w, `{"sk":"test","uid":"123"}`)
		case "/models-v2":
			prepares.Add(1)
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"code":"mpfs"}}`)
		case "/disk/original":
			w.Write(original)
		case "/preview/large":
			w.Write(preview)
		case "/preview/thumb":
			w.Write(thumb)
		default:
			t.Error("unexpected endpoint", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	r := zipTestResource("photounlim:/a", "a.png", original)
	r.Raw["meta"] = Object{"sizes": []any{Object{"name": "ORIGINAL", "url": server.URL + "/disk/original"}, Object{"name": "XL", "url": server.URL + "/preview/large", "width": 400, "height": 400}, Object{"name": "S", "url": server.URL + "/preview/thumb", "width": 100, "height": 100}}}
	s.save(r)
	c := &Client{HTTP: server.Client(), Base: server.URL, Interval: time.Millisecond, CooldownBase: time.Millisecond, CooldownMax: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e := runInventory(ctx, s, c, 1, "zip", func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Done != 1 || st.Original != 1 || st.Preview != 1 || st.Thumbnail != 0 || st.Failed != 0 || prepares.Load() != 1 {
		t.Fatal(st, prepares.Load())
	}
	var count int
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors WHERE error LIKE '%429%'").Scan(&count)
	if count != 0 {
		t.Fatal("429 recorded against photo")
	}
}

func TestZIPQueueExcludesVideosAndSavedOriginals(t *testing.T) {
	s := testStore(t)
	for _, r := range []Resource{{ID: "photo", Name: "photo.jpg", Raw: Object{"type": "file", "id": "photo", "name": "photo.jpg"}}, {ID: "video", Name: "video.MOV", Raw: Object{"type": "file", "id": "video", "name": "video.MOV"}}, {ID: "mime", Name: "unknown", Raw: Object{"type": "file", "id": "mime", "name": "unknown", "mimetype": "video/mp4"}}, {ID: "saved", Name: "saved.jpg", Raw: Object{"type": "file", "id": "saved", "name": "saved.jpg"}}} {
		s.save(r)
	}
	s.done(Result{ID: "saved", Quality: "original", Archive: "saved.zip", Bytes: 1})
	queue, e := s.zipQueue()
	if e != nil || len(queue) != 1 || queue[0].ID != "photo" || queue[0].Raw != nil {
		t.Fatal(queue, e)
	}
}

func TestZIPManifestRemovesSignedURLsAndTokens(t *testing.T) {
	r := Object{"name": "photo.jpg", "token": "SECRET", "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": "https://example.org/disk/photo?token=SECRET"}}, "created": "2020-01-01T00:00:00Z"}}
	b, e := json.Marshal(sanitizedAPI(r))
	if e != nil || strings.Contains(string(b), "SECRET") || strings.Contains(string(b), "example.org") {
		t.Fatal("secret appeared in ZIP manifest")
	}
}
