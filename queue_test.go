package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPhotoQueueBeforeVideoRegardlessOfReadyOriginal(t *testing.T) {
	s := testStore(t)
	ready := Object{"sizes": []any{Object{"name": "ORIGINAL", "url": "https://downloader.invalid/disk/ready"}}}
	for _, raw := range []Object{
		{"path": "a-video", "name": "movie.MOV", "meta": ready},
		{"path": "b-video", "name": "opaque", "mimetype": "video/mp4"},
		{"path": "c-video", "name": "opaque", "meta": Object{"mimetype": "VIDEO/QUICKTIME"}},
		{"path": "d-video", "name": "opaque", "media_type": "video"},
		{"path": "e-video", "name": "clip.m2ts"},
		{"path": "a-photo", "name": "lost.HEIC"},
		{"path": "z-photo", "name": "photo.jpg", "meta": ready},
		{"path": "gif-photo", "name": "animated.gif"},
	} {
		r, e := resource(raw, "")
		if e != nil {
			t.Fatal(e)
		}
		if e := s.save(r); e != nil {
			t.Fatal(e)
		}
	}
	queue, e := s.downloadQueue("1=1")
	if e != nil || len(queue) != 8 {
		t.Fatal(queue, e)
	}
	if queue[0].ID != "z-photo" {
		t.Fatal("ready photo priority lost", queue)
	}
	for i, item := range queue {
		if item.Video != (i >= 3) || item.Video != strings.HasSuffix(item.ID, "video") {
			t.Fatal("video overtook a photo or was misclassified", queue)
		}
	}
}

func TestFilesWithNoCopyPrecedeRepeatedQualityChecks(t *testing.T) {
	s := testStore(t)
	for _, raw := range []Object{
		{"path": "old-photo", "name": "old.jpg", "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": "https://downloader.invalid/disk/ready"}}}},
		{"path": "new-photo", "name": "new.jpg"},
		{"path": "new-video", "name": "new.mp4"},
	} {
		r, e := resource(raw, "")
		if e != nil {
			t.Fatal(e)
		}
		if e := s.save(r); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.done(Result{ID: "old-photo", Quality: "preview", Bytes: 10}); e != nil {
		t.Fatal(e)
	}
	queue, e := s.downloadQueue("1=1")
	if e != nil || len(queue) != 3 || queue[0].ID != "new-photo" || queue[1].ID != "old-photo" || queue[2].ID != "new-video" {
		t.Fatal(queue, e)
	}
}

func TestVideoWaitsForAllPhotoVersionsAndCancellationReleasesBarrier(t *testing.T) {
	for _, cancelRun := range []bool{false, true} {
		name := "complete"
		if cancelRun {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			s.set("inventory", "complete")
			data := picture(500, 500)
			blocked := make(chan struct{})
			release := make(chan struct{})
			fastDone := make(chan struct{})
			videoStarted := make(chan struct{}, 1)
			var releaseOnce, fastOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/client/disk":
					w.Write([]byte(`{"sk":"test-sk","uid":"1234"}`))
					return
				case "/preview/slow":
					close(blocked)
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				case "/disk/video":
					videoStarted <- struct{}{}
					for _, id := range []string{"slow", "fast"} {
						versions, e := s.versions(id)
						if e != nil || len(versions) != 3 {
							t.Errorf("video started before %s finished: %d versions, %v", id, len(versions), e)
						}
					}
				}
				w.Write(data)
			}))
			t.Cleanup(server.Close)
			c := &Client{HTTP: server.Client(), Base: server.URL, Batch: 1}
			for _, id := range []string{"slow", "fast", "video"} {
				fileName := id + ".jpg"
				if id == "video" {
					fileName = "movie.mp4"
				}
				r, e := resource(Object{"path": id, "name": fileName, "size": len(data), "meta": Object{"sizes": []any{
					Object{"name": "ORIGINAL", "url": server.URL + "/disk/" + id},
					Object{"name": "XXXL", "url": server.URL + "/preview/" + id, "width": 500, "height": 500},
					Object{"name": "S", "url": server.URL + "/thumbnail/" + id, "width": 100, "height": 100},
				}}}, "")
				if e != nil {
					t.Fatal(e)
				}
				if e := s.save(r); e != nil {
					t.Fatal(e)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- runInventory(ctx, s, c, 2, "resume", func(msg string) {
					if strings.Contains(msg, "fast.jpg · thumbnail") {
						fastOnce.Do(func() { close(fastDone) })
					}
				})
			}()
			for _, event := range []chan struct{}{blocked, fastDone} {
				select {
				case <-event:
				case <-time.After(5 * time.Second):
					t.Fatal("photo pipeline did not start")
				}
			}
			select {
			case <-videoStarted:
				t.Fatal("idle worker started video while another photo was downloading")
			case <-time.After(100 * time.Millisecond):
			}
			if cancelRun {
				cancel()
			} else {
				unblock()
			}
			select {
			case e := <-done:
				if cancelRun && !errors.Is(e, context.Canceled) || !cancelRun && e != nil {
					t.Fatal(e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("phase barrier did not release")
			}
			if cancelRun {
				select {
				case <-videoStarted:
					t.Fatal("cancelled photo phase started video")
				default:
				}
			} else {
				select {
				case <-videoStarted:
				default:
					t.Fatal("video phase was skipped")
				}
			}
			unblock()
		})
	}
}
