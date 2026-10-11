package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestWebDAVOriginalUsesOAuthAndVerifiedFullRange(t *testing.T) {
	s := testStore(t)
	data := picture(800, 600)
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/photounlim/Фото #1.png" || r.Header.Get("Authorization") != "OAuth fixture-oauth" || r.Header.Get("Range") != "bytes=0-" {
			t.Error("wrong mobile request", r.URL.Path, r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
		w.WriteHeader(206)
		w.Write(data)
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), Base: server.URL, WebDAVBase: server.URL, WebDAVToken: "fixture-oauth", BatchOriginals: true}
	r := zipTestResource("/photounlim/Фото #1.png", "Фото #1.png", data)
	if e := s.save(r); e != nil {
		t.Fatal(e)
	}
	got, e := c.downloadSelected(context.Background(), s, r, "original")
	if e != nil || calls.Load() != 1 || got.Quality != "original" || got.MD5 != r.MD5 {
		t.Fatal(got, e, calls.Load())
	}
	full, _ := localArchive(s.Root, got.Archive)
	if e := verifyZIP(context.Background(), full, got); e != nil {
		t.Fatal(e)
	}
}

func TestWebDAVDoesNotLeakOAuthToRedirect(t *testing.T) {
	data := picture(700, 500)
	cdn := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("OAuth forwarded to CDN")
		}
		w.Write(data)
	}))
	defer cdn.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/folder/a.png" {
			t.Error("disk namespace leaked into public WebDAV root")
		}
		http.Redirect(w, r, cdn.URL+"/media", 302)
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), WebDAVBase: server.URL, WebDAVToken: "fixture-oauth", BatchOriginals: true}
	s := testStore(t)
	r := zipTestResource("/disk/folder/a.png", "a.png", data)
	if _, e := c.downloadSelected(context.Background(), s, r, "original"); e != nil {
		t.Fatal(e)
	}
}

func TestWebDAVRejectedTokenFallsBackWithoutFailingWebSession(t *testing.T) {
	data := picture(700, 500)
	var native atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/photounlim/a.png" {
			native.Add(1)
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("OAuth sent to web media")
		}
		w.Write(data)
	}))
	defer server.Close()
	cdn := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("OAuth sent to web media")
		}
		w.Write(data)
	}))
	defer cdn.Close()
	c := &Client{HTTP: server.Client(), WebDAVBase: server.URL, WebDAVToken: "fixture-oauth", BatchOriginals: true}
	s := testStore(t)
	r := zipTestResource("/photounlim/a.png", "a.png", data)
	r.Candidates = []Candidate{{URL: cdn.URL + "/disk/media", Quality: "original"}}
	for i := 0; i < 2; i++ {
		if _, e := c.downloadSelected(context.Background(), s, r, "original"); e != nil {
			t.Fatal(e)
		}
	}
	if native.Load() != 1 || !c.webDAVRejected.Load() {
		t.Fatal("invalid OAuth retried for every file", native.Load())
	}
}

func TestWebDAVOnlyAcceptsCompleteRangeAndSafeTokenFile(t *testing.T) {
	ctx := context.WithValue(context.Background(), webDAVDownloadKey{}, true)
	for _, test := range []struct {
		header string
		length int64
		ok     bool
	}{{"bytes 0-9/10", 10, true}, {"bytes 0-9/10", -1, true}, {"bytes 1-9/10", 9, false}, {"bytes 0-4/10", 5, false}, {"bytes 0-9/10", 3, false}, {"bytes 0-9/*", 10, false}} {
		r := &http.Response{Header: http.Header{}, ContentLength: test.length}
		r.Header.Set("Content-Range", test.header)
		if fullWebDAVResponse(ctx, r) != test.ok {
			t.Fatal(test)
		}
		if fullWebDAVResponse(context.Background(), r) {
			t.Fatal("ordinary partial media accepted")
		}
	}
	for _, token := range []string{"OAuth fixture-oauth", "bad\nsecret", "", "https://token.invalid/value"} {
		if _, e := parseOAuthToken([]byte(token)); e == nil {
			t.Fatal("invalid token accepted")
		}
	}
	c := &Client{}
	file := filepath.Join(t.TempDir(), "oauth-token.txt")
	if e := c.loadWebDAVToken(file); e != nil {
		t.Fatal(e)
	}
	os.WriteFile(file, []byte("fixture-oauth\n"), 0600)
	if e := c.loadWebDAVToken(file); e != nil || c.WebDAVToken != "fixture-oauth" {
		t.Fatal(e)
	}
	u, _ := url.Parse("http://webdav.yandex.ru/")
	if c.isWebDAVURL(u) {
		t.Fatal("plaintext auth allowed")
	}
	u, _ = url.Parse("https://webdav.yandex.ru.evil.invalid/")
	if c.isWebDAVURL(u) {
		t.Fatal("OAuth allowed on foreign host")
	}
	os.WriteFile(file, []byte(strings.Repeat("a", 9000)), 0600)
	if e := c.loadWebDAVToken(file); e == nil {
		t.Fatal("oversized credential file accepted")
	}
}
