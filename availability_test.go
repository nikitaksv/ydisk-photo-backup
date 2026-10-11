package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMissingVersionsFallThroughOnceAndContinue(t *testing.T) {
	var mu sync.Mutex
	counts := make(map[string]int)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()
		switch r.URL.Path {
		case "/models-v2":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Object{"download_url": srv.URL + "/original"})
		case "/original":
			w.WriteHeader(404)
		case "/preview":
			w.WriteHeader(410)
		case "/thumb":
			w.Write(picture(90, 90))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL}
	s := testStore(t)
	r, _ := resource(Object{"path": "photounlim:/a.png", "name": "a.png", "preview": srv.URL + "/preview", "thumbnail": srv.URL + "/thumb"}, "day")
	s.save(r)
	start := time.Now()
	if e := c.processBatch(context.Background(), s, []Resource{r}, func(string) {}); e != nil {
		t.Fatal(e)
	}
	if time.Since(start) > time.Second {
		t.Fatal("negative responses were retried")
	}
	st, _ := s.stats()
	if st.Done != 1 || st.OnlyThumbnail != 1 {
		t.Fatalf("%+v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range []string{"/models-v2", "/original", "/preview", "/thumb"} {
		if counts[path] != 1 {
			t.Fatalf("%s: %d requests", path, counts[path])
		}
	}
}

func TestBulkProbeTimeoutSkipsRepeatedOriginalProbes(t *testing.T) {
	var originals atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models-v2" {
			io.Copy(io.Discard, r.Body)
			originals.Add(1)
			<-r.Context().Done()
			return
		}
		w.Write(picture(90, 90))
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, HeaderWait: 40 * time.Millisecond}
	s := testStore(t)
	var batch []Resource
	for _, name := range []string{"a.png", "b.png"} {
		r, _ := resource(Object{"path": "photounlim:/" + name, "name": name, "thumbnail": srv.URL + "/thumb"}, "d")
		s.save(r)
		batch = append(batch, r)
	}
	start := time.Now()
	if e := c.processBatch(context.Background(), s, batch, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if originals.Load() != 1 || st.Done != 2 || st.OnlyThumbnail != 2 || time.Since(start) > time.Second {
		t.Fatalf("requests=%d stats=%+v", originals.Load(), st)
	}
	var n int
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors WHERE quality='original' AND error LIKE '%доступность не подтверждена%'").Scan(&n)
	if n != 2 {
		t.Fatal("timeout was confused with confirmed absence", n)
	}
}

func TestFirstByteTimeoutFallsBackWithoutRepeatingURL(t *testing.T) {
	var stalled atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/models-v2":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Object{"download_url": srv.URL + "/original"})
		case "/original":
			w.WriteHeader(404)
		case "/stalled":
			stalled.Add(1)
			w.Header().Set("Content-Type", "image/png")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, FirstByteWait: 40 * time.Millisecond}
	s := testStore(t)
	r, _ := resource(Object{"path": "photounlim:/a.png", "name": "a.png", "preview": srv.URL + "/stalled"}, "day")
	s.save(r)
	start := time.Now()
	if e := c.processBatch(context.Background(), s, []Resource{r}, func(string) {}); e != nil {
		t.Fatal(e)
	}
	if stalled.Load() != 1 || time.Since(start) > time.Second {
		t.Fatal("stalled URL was checked repeatedly", stalled.Load())
	}
	st, _ := s.stats()
	if st.Failed != 1 || st.Pending != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestHealthyStreamCanOutlastProbeTimeout(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 10; i++ {
			w.Write(bytes.Repeat([]byte("x"), 1024))
			w.(http.Flusher).Flush()
			if e := waitContext(r.Context(), 20*time.Millisecond); e != nil {
				return
			}
		}
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, HeaderWait: 60 * time.Millisecond, FirstByteWait: 60 * time.Millisecond, IdleWait: 80 * time.Millisecond, APITimeout: 40 * time.Millisecond}
	start := time.Now()
	d, e := c.mediaHashed(context.Background(), srv.URL+"/media", filepath.Join(t.TempDir(), "media"))
	if e != nil || d.Bytes != 10240 || time.Since(start) < 150*time.Millisecond {
		t.Fatalf("healthy stream cut off: %+v %v", d, e)
	}
}

func TestAPIResponseBodyHasShortDeadline(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"download_url":`))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, APITimeout: 40 * time.Millisecond, FirstByteWait: time.Second, IdleWait: time.Second}
	start := time.Now()
	_, e := c.call(context.Background(), "mpfs/bulk-download-prepare", nil)
	if !isProbeTimeout(e) || time.Since(start) > time.Second {
		t.Fatalf("API body did not time out promptly: %v", e)
	}
}

func TestIdleTimerIgnoresTimeSpentWritingLocalData(t *testing.T) {
	var cancelled atomic.Int32
	b := &progressBody{body: io.NopCloser(bytes.NewReader([]byte("abc"))), cancel: func() { cancelled.Add(1) }, firstWait: 20 * time.Millisecond, idleWait: 20 * time.Millisecond}
	defer b.Close()
	buf := make([]byte, 1)
	if _, e := b.Read(buf); e != nil {
		t.Fatal(e)
	}
	time.Sleep(70 * time.Millisecond)
	if _, e := b.Read(buf); e != nil || cancelled.Load() != 0 {
		t.Fatalf("local work counted as network stall: %v", e)
	}
}

func TestSinglePrepareStillRefreshesSessionKey(t *testing.T) {
	var calls atomic.Int32
	c := apiFixture(t, func(d Object, url string) Object {
		if calls.Add(1) == 1 {
			return Object{"error": Object{"code": "CKEY", "ckey": "newkey"}}
		}
		if str(d["sk"]) != "newkey" {
			t.Error("new session key not used")
		}
		return Object{"download_url": url + "/media"}
	}, nil, nil)
	if _, e := c.call(context.Background(), "mpfs/bulk-download-prepare", nil); e != nil || calls.Load() != 2 {
		t.Fatalf("%v calls=%d", e, calls.Load())
	}
}
