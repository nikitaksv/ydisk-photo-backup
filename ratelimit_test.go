package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryAfterBothFormatsAndLongDelay(t *testing.T) {
	now := time.Date(2026, 10, 11, 14, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{"1200": 20 * time.Minute, now.Add(2 * time.Minute).Format(http.TimeFormat): 2 * time.Minute, "-1": 0, "invalid": 0, "9223372036854775807": 0} {
		if got := retryAfterDuration(value, now); got != want {
			t.Fatalf("%s: %s != %s", value, got, want)
		}
	}
}

func TestGlobal429BlocksAPIAndMediaWithoutBurst(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var times []time.Time
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(429)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	srv.Client().Transport = recordingTransport{srv.Client().Transport, &mu, &times}
	c := &Client{HTTP: srv.Client(), Base: srv.URL, Interval: 8 * time.Millisecond, CooldownBase: 100 * time.Millisecond, CooldownMax: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 7)
	request := func(method string) {
		res, e := c.request(ctx, method, srv.URL, nil)
		if e == nil {
			res.Body.Close()
		}
		results <- e
	}
	go request("POST")
	deadline := time.Now().Add(time.Second)
	for c.rateStatus().LimitedUntil == "" {
		if time.Now().After(deadline) {
			t.Fatal("cooldown not entered")
		}
		time.Sleep(time.Millisecond)
	}
	st := c.rateStatus()
	until, _ := time.Parse(time.RFC3339Nano, st.LimitedUntil)
	for _, method := range []string{"GET", "POST", "GET", "POST", "GET", "GET"} {
		go request(method)
	}
	for i := 0; i < 7; i++ {
		if e := <-results; e != nil {
			t.Fatal(e)
		}
	}
	if st.IntervalMS != 16 {
		t.Fatalf("interval not reduced: %+v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(times) != 8 {
		t.Fatalf("calls=%d", len(times))
	}
	for i := 1; i < len(times); i++ {
		if times[i].Before(until) {
			t.Fatal("request bypassed shared cooldown")
		}
		if i > 1 && times[i].Sub(times[i-1]) < 10*time.Millisecond {
			t.Fatal("queued requests burst after cooldown", times)
		}
	}
}

func TestInflight429sCoalesceAndStateSurvivesRestart(t *testing.T) {
	c := &Client{Interval: 3 * time.Second}
	for i := 0; i < 6; i++ {
		if e := c.throttle(""); e != nil {
			t.Fatal(e)
		}
	}
	st := c.rateStatus()
	if st.Level != 1 || st.IntervalMS != 6000 || st.Events != 6 {
		t.Fatalf("inflight responses multiplied backoff: %+v", st)
	}
	restored := &Client{Interval: 3 * time.Second}
	restored.restoreRate(st)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e := restored.pace(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("restored pause ignored: %v", e)
	}
	if restored.rateStatus().LimitedUntil != st.LimitedUntil {
		t.Fatal("cooldown changed on restore")
	}
}

func Test429DoesNotExhaustRetryBudget(t *testing.T) {
	var count atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count.Add(1) <= 6 {
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Object{"download_url": "https://example.invalid/media"})
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, Interval: time.Millisecond, CooldownBase: time.Millisecond, CooldownMax: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, e := c.call(ctx, "mpfs/bulk-download-prepare", nil); e != nil {
		t.Fatal(e)
	}
	if count.Load() != 7 {
		t.Fatal("rate limit consumed retry budget", count.Load())
	}
}

func TestCancelDuring429LeavesFilePending(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, Interval: time.Millisecond, CooldownBase: time.Hour}
	s := testStore(t)
	r, _ := resource(Object{"type": "file", "path": "photounlim:/a.png", "name": "a.png"}, "d")
	s.save(r)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.processBatchStage(ctx, s, []Resource{r}, "originals", func(string) {}) }()
	deadline := time.Now().Add(time.Second)
	for c.rateStatus().Events == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("429 not observed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt cooldown")
	}
	st, _ := s.stats()
	var n int
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors").Scan(&n)
	if st.Failed != 0 || st.Pending != 1 || n != 0 {
		t.Fatalf("429 became file failure: %+v errors=%d", st, n)
	}
}

func TestRepairOld429PreservesRealErrorsAndCopies(t *testing.T) {
	s := testStore(t)
	for _, id := range []string{"limited", "missing", "saved"} {
		r, _ := resource(Object{"type": "file", "path": id, "name": id}, "d")
		s.save(r)
	}
	s.attempt("limited", "original", errors.New("mpfs/bulk-download-prepare: HTTP 429 / response"))
	s.attempt("limited", "thumbnail", errors.New("no thumbnail link"))
	s.failed("limited", errors.New("no thumbnail link"))
	s.attempt("missing", "original", errors.New("media: HTTP 404"))
	s.failed("missing", errors.New("not found"))
	s.done(Result{ID: "saved", Quality: "preview", Bytes: 100})
	s.attempt("saved", "original", errors.New("HTTP 429"))
	if n, e := s.requeueRateLimits(); e != nil || n != 2 {
		t.Fatalf("%d %v", n, e)
	}
	st, _ := s.stats()
	if st.Pending != 1 || st.Failed != 1 || st.Done != 1 || st.Preview != 1 {
		t.Fatalf("%+v", st)
	}
	var n int
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors WHERE error LIKE '%HTTP 429%'").Scan(&n)
	if n != 0 {
		t.Fatal("historical 429 not cleared")
	}
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors WHERE error='no thumbnail link'").Scan(&n)
	if n != 1 {
		t.Fatal("independent errors removed")
	}
}

func TestResumeUsesInventoryWithoutEnumeration(t *testing.T) {
	c := apiFixture(t, func(d Object, url string) Object {
		if str(d["apiMethod"]) != "mpfs/bulk-download-prepare" {
			t.Errorf("resume enumerated API: %s", str(d["apiMethod"]))
		}
		return Object{"download_url": url + "/original"}
	}, map[string][]byte{"/client/disk": []byte(`{"sk":"sk","uid":"1234"}`), "/original": zipBytes("a.png", picture(500, 500))}, nil)
	s := testStore(t)
	r, _ := resource(Object{"type": "file", "path": "photounlim:/a.png", "name": "a.png"}, "d")
	s.save(r)
	s.set("inventory", "incomplete")
	e := runInventory(context.Background(), s, c, 2, "resume", func(string) {})
	if e == nil || !strings.Contains(e.Error(), "полнота") {
		t.Fatalf("inventory was falsely completed: %v", e)
	}
	st, _ := s.stats()
	if st.Total != 1 || st.Done != 1 || st.Original != 1 || st.Inventory != "incomplete" {
		t.Fatalf("%+v", st)
	}
}
