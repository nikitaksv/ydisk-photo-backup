package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Arrival times include variable TLS/network latency. Measure dispatch at the
// transport boundary to test the shared scheduler rather than that latency.
type recordingTransport struct {
	http.RoundTripper
	mu     *sync.Mutex
	starts *[]time.Time
}

func (r recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	*r.starts = append(*r.starts, time.Now())
	r.mu.Unlock()
	return r.RoundTripper.RoundTrip(req)
}

func TestConfiguredRateAndFallbackSurviveRestart(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	if e := c.configureInterval(200 * time.Millisecond); e != nil {
		t.Fatal(e)
	}
	c.throttle("")
	c.mu.Lock()
	c.cooldown = time.Now().Add(-time.Second)
	c.backoffLevel = 0 // Two separate 90-second episodes also trigger fallback.
	c.mu.Unlock()
	c.throttle("")
	st := c.rateStatus()
	if !st.FastFallback || st.ConfiguredIntervalMS != 500 || st.BaseIntervalMS != 500 || st.Fast429Episodes != 2 {
		t.Fatal(st)
	}
	restored := &Client{Interval: 500 * time.Millisecond}
	restored.restoreRate(st)
	if got := restored.rateStatus(); got.ConfiguredIntervalMS != 500 || got.LimitedUntil != st.LimitedUntil || got.IntervalMS != st.IntervalMS || !got.FastFallback {
		t.Fatal(got)
	}
	fast := &Client{Interval: 500 * time.Millisecond}
	fast.restoreRate(RateStatus{ConfiguredIntervalMS: 200, BaseIntervalMS: 200, IntervalMS: 200})
	if fast.Interval != 200*time.Millisecond || fast.baseInterval != 200*time.Millisecond {
		t.Fatal(fast.rateStatus())
	}
}

func TestInflight429IsOneFastEpisode(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.configureInterval(200 * time.Millisecond)
	for i := 0; i < 6; i++ {
		if e := c.throttle(""); e != nil {
			t.Fatal(e)
		}
	}
	s := c.rateStatus()
	if s.Fast429Episodes != 1 || s.FastFallback || s.IntervalMS != 400 || s.ConfiguredIntervalMS != 200 {
		t.Fatal(s)
	}
}

func TestLong429ReturnsBaselineWithoutBypassingPause(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.configureInterval(200 * time.Millisecond)
	c.throttle("180")
	s := c.rateStatus()
	until, _ := time.Parse(time.RFC3339Nano, s.LimitedUntil)
	if !s.FastFallback || s.ConfiguredIntervalMS != 500 || s.IntervalMS != 500 || time.Until(until) < 179*time.Second {
		t.Fatal(s)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e := c.pace(ctx); e == nil {
		t.Fatal("server pause was bypassed")
	}
}

func TestConfigureRatePreservesActiveRestriction(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.configureInterval(200 * time.Millisecond)
	c.throttle("90")
	before := c.rateStatus()
	if e := c.configureInterval(500 * time.Millisecond); e != nil {
		t.Fatal(e)
	}
	after := c.rateStatus()
	if after.LimitedUntil != before.LimitedUntil || after.Level != before.Level || after.Events != before.Events || after.IntervalMS != 500 {
		t.Fatal(before, after)
	}
	if e := c.configureInterval(200 * time.Millisecond); e != nil {
		t.Fatal(e)
	}
	if c.rateStatus().IntervalMS != 500 {
		t.Fatal("configuration reduced active backoff")
	}
	for _, d := range []time.Duration{0, 99 * time.Millisecond, 6 * time.Second} {
		if c.configureInterval(d) == nil {
			t.Fatal("invalid interval accepted", d)
		}
	}
}

func TestTenRPSFallbackToFiveSurvivesRestart(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	if e := c.configureInterval(100*time.Millisecond, 200*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	c.throttle("")
	c.mu.Lock()
	c.cooldown = time.Now().Add(-time.Second)
	c.backoffLevel = 0
	c.mu.Unlock()
	c.throttle("")
	st := c.rateStatus()
	if !st.FastFallback || st.BaseIntervalMS != 200 || st.ConfiguredIntervalMS != 200 || st.FallbackIntervalMS != 200 {
		t.Fatal(st)
	}
	restored := &Client{Interval: 500 * time.Millisecond}
	restored.restoreRate(st)
	// Other in-flight 429 responses must not change the selected target to 500 ms.
	restored.throttle("")
	got := restored.rateStatus()
	if got.BaseIntervalMS != 200 || got.ConfiguredIntervalMS != 200 || got.FallbackIntervalMS != 200 || got.LimitedUntil != st.LimitedUntil {
		t.Fatal(got)
	}
	for i := 0; i < 60; i++ {
		restored.acceptedRequest()
	}
	if restored.rateStatus().IntervalMS != 200 {
		t.Fatal(restored.rateStatus())
	}
}

func TestTenRPSLongPauseReturnsToSelectedInterval(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	if e := c.configureInterval(100 * time.Millisecond); e != nil {
		t.Fatal(e)
	}
	c.throttle("180")
	st := c.rateStatus()
	if !st.FastFallback || st.ConfiguredIntervalMS != 200 || st.FallbackIntervalMS != 200 || st.LimitedUntil == "" {
		t.Fatal(st)
	}
	for _, target := range []time.Duration{0, 100 * time.Millisecond, 6 * time.Second} {
		if c.configureInterval(100*time.Millisecond, target) == nil {
			t.Fatal("invalid fallback accepted", target)
		}
	}
}

func TestTenRPSIsSharedAcrossWorkers(t *testing.T) {
	var mu sync.Mutex
	var starts []time.Time
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Interval: 500 * time.Millisecond}
	c.configureInterval(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, e := c.request(ctx, "GET", srv.URL, nil)
			if e != nil {
				t.Error(e)
				return
			}
			res.Body.Close()
		}()
	}
	wg.Wait()
	if st := c.rateStatus(); st.Requests != 12 || st.SuccessfulRequests != 12 || st.ObservedRPS < 9 || st.ObservedRPS > 12 {
		t.Fatal(st)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(starts); i++ {
		if starts[i].Sub(starts[i-1]) < 90*time.Millisecond {
			t.Fatal("workers exceeded 10 RPS", starts)
		}
	}
}

func TestFastEpisodeWindowExpires(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.configureInterval(200 * time.Millisecond)
	c.throttle("")
	c.mu.Lock()
	c.fast429Since = time.Now().Add(-11 * time.Minute)
	c.cooldown = time.Now().Add(-time.Second)
	c.backoffLevel = 0
	c.mu.Unlock()
	c.throttle("")
	if s := c.rateStatus(); s.Fast429Episodes != 1 || s.FastFallback {
		t.Fatal(s)
	}
}

func TestFiveRPSIsGlobalAndMeasured(t *testing.T) {
	var mu sync.Mutex
	var starts []time.Time
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), Base: srv.URL, Interval: 500 * time.Millisecond}
	c.configureInterval(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, e := c.request(ctx, "GET", srv.URL, nil)
			if e != nil {
				t.Error(e)
				return
			}
			res.Body.Close()
		}()
	}
	wg.Wait()
	st := c.rateStatus()
	if st.Requests != 10 || st.SuccessfulRequests != 10 || st.ObservedRPS < 4 || st.ObservedRPS > 6 {
		t.Fatal(st)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(starts); i++ {
		if starts[i].Sub(starts[i-1]) < 180*time.Millisecond {
			t.Fatal("workers exceeded shared frequency", starts)
		}
	}
}

func TestRedirectsUseTheSharedBudget(t *testing.T) {
	var mu sync.Mutex
	var starts []time.Time
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/media", http.StatusFound)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	srv.Client().Transport = recordingTransport{srv.Client().Transport, &mu, &starts}
	// Redirect scheduling takes longer than the network probe/API budgets.
	c := &Client{HTTP: srv.Client(), Interval: 200 * time.Millisecond, HeaderWait: 60 * time.Millisecond, APITimeout: 100 * time.Millisecond}
	res, e := c.request(context.Background(), "POST", srv.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	res.Body.Close()
	if c.rateStatus().Requests != 2 {
		t.Fatal(c.rateStatus())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(starts) != 2 || starts[1].Sub(starts[0]) < 180*time.Millisecond {
		t.Fatal("redirect bypassed limiter", starts)
	}
}

func TestFastEpisodeGuardSurvivesRestart(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.configureInterval(200 * time.Millisecond)
	c.throttle("")
	restored := &Client{Interval: 500 * time.Millisecond}
	restored.restoreRate(c.rateStatus())
	restored.mu.Lock()
	restored.cooldown = time.Now().Add(-time.Second)
	restored.backoffLevel = 0
	restored.mu.Unlock()
	restored.throttle("")
	if st := restored.rateStatus(); !st.FastFallback || st.Fast429Episodes != 2 || st.ConfiguredIntervalMS != 500 {
		t.Fatal(st)
	}
}
