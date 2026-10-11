package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestMediaHTTPFailureIsProbedOnceThenFallsBack(t *testing.T) {
	for _, status := range []int{422, 500, 502, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var originalCalls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/disk/original" {
					originalCalls.Add(1)
					w.WriteHeader(status)
					return
				}
				w.Write(picture(500, 500))
			}))
			defer server.Close()
			c := &Client{HTTP: server.Client(), Base: server.URL}
			r := Resource{ID: "photo", Name: "photo.png", Candidates: []Candidate{
				{URL: server.URL + "/disk/original", Quality: "original"},
				{URL: server.URL + "/preview/large", Quality: "preview"},
			}}
			start := time.Now()
			got, e := c.downloadOne(context.Background(), testStore(t), r)
			if e != nil || got.Quality != "preview" || originalCalls.Load() != 1 || time.Since(start) > time.Second {
				t.Fatalf("quality=%s calls=%d error=%v elapsed=%s", got.Quality, originalCalls.Load(), e, time.Since(start))
			}
		})
	}
}

func TestRecoveredLegacyBaselineDoesNotRestoreArtificialSlowFloor(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.restoreRate(RateStatus{IntervalMS: 3000, Events: 5})
	if c.Interval != 500*time.Millisecond || c.limitEvents != 5 {
		t.Fatal(c.rateStatus())
	}
	until := time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
	c.restoreRate(RateStatus{LimitedUntil: until, IntervalMS: 30000, Events: 6, Level: 3})
	if c.Interval != 30*time.Second || c.rateStatus().LimitedUntil != until || c.limitEvents != 6 {
		t.Fatal("active restriction was reset", c.rateStatus())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e := c.pace(ctx); e == nil {
		t.Fatal("active restriction was bypassed")
	}
}

func TestFastBaselineIsPersistedAndRestoredAfterRecovery(t *testing.T) {
	c := &Client{Interval: 500 * time.Millisecond}
	c.restoreRate(RateStatus{})
	st := c.rateStatus()
	if st.BaseIntervalMS != 500 {
		t.Fatal(st)
	}
	other := &Client{Interval: 500 * time.Millisecond}
	other.restoreRate(st)
	if other.Interval != c.Interval || other.baseInterval != c.baseInterval {
		t.Fatal(other.rateStatus(), st)
	}
	other.restoreRate(RateStatus{IntervalMS: 1000, BaseIntervalMS: 500, Level: 0})
	if other.Interval != time.Second {
		t.Fatal("unrecovered spacing was discarded", other.rateStatus())
	}
}

func TestNewClientHasShortProbesAndStreamingTimeouts(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test-cookies.json")
	if e := atomicFile(file, func(f *os.File) error {
		_, e := f.Write([]byte(`[{"name":"Session_id","value":"fixture","domain":".yandex.ru"}]`))
		return e
	}); e != nil {
		t.Fatal(e)
	}
	c, e := newClient(file)
	if e != nil {
		t.Fatal(e)
	}
	defer c.HTTP.CloseIdleConnections()
	if c.Interval != 500*time.Millisecond || c.HeaderWait != 8*time.Second || c.FirstByteWait != 8*time.Second || c.HTTP.Timeout != 20*time.Minute {
		t.Fatal("fast probes changed streaming allowance")
	}
}
