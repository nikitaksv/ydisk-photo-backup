package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestZIPDisabledIsNotRateLimitAndQueueSurvives(t *testing.T) {
	s := testStore(t)
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":{"type":"mpfs","code":394,"title":"Archive download is disabled","message":"SECRET"}}`))
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), Base: server.URL, BatchOriginals: true}
	for _, id := range []string{"a", "b"} {
		if e := s.save(Resource{ID: id, Name: id + ".png", Raw: Object{"type": "file", "id": id, "name": id + ".png"}}); e != nil {
			t.Fatal(e)
		}
		if e := s.queueOriginalZIP(id); e != nil {
			t.Fatal(e)
		}
	}
	if e := c.drainOriginalZIP(context.Background(), s, true, false, func(string) {}); !errors.Is(e, errZIPDisabled) {
		t.Fatal(e)
	}
	st, e := s.stats()
	if e != nil || st.Failed != 0 || st.Pending != 2 || c.rateStatus().Events != 0 || c.rateStatus().LimitedUntil != "" || s.zipStatus().Stage != "unavailable" || s.zipStatus().Queued != 2 || !s.zipDisabledUntil().After(time.Now()) {
		t.Fatal(st, e, c.rateStatus(), s.zipStatus())
	}
	// A restarted client observes the persisted feature block, not a quota.
	restarted := &Client{HTTP: server.Client(), Base: server.URL, BatchOriginals: true}
	if e := restarted.drainOriginalZIP(context.Background(), s, true, false, func(string) {}); !errors.Is(e, errZIPDisabled) || calls.Load() != 1 {
		t.Fatal(e, calls.Load())
	}
}

func TestArchiveDisabledClassificationIsMethodSpecific(t *testing.T) {
	for _, test := range []struct {
		body     string
		disabled bool
	}{
		{`{"error":{"type":"mpfs","code":394}}`, true},
		{`{"error":{"type":"mpfs","code":429}}`, false},
		{`{"error":{"code":394}}`, false},
		{`{"code":394}`, false},
		{`<html>394</html>`, false},
	} {
		if got := archiveDownloadDisabled([]byte(test.body)); got != test.disabled {
			t.Fatal(test.body, got)
		}
	}
	if isZIPPrepareURL("https://disk.yandex.ru/media?m=mpfs/bulk-download-prepare") || isZIPPrepareURL("https://disk.yandex.ru/models-v2?m=intapi/photo-init-snapshot") || !isZIPPrepareURL("https://disk.yandex.ru/models-v2?m=mpfs%2Fbulk-download-prepare") {
		t.Fatal("unsafe feature classification")
	}
}
