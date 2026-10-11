package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func multiZIP(entries map[string][]byte) []byte {
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	for name, data := range entries {
		f, _ := z.Create(name)
		f.Write(data)
	}
	z.Close()
	return b.Bytes()
}

func zipTestResource(id, name string, data []byte) Resource {
	h := md5.Sum(data)
	r, _ := resource(Object{"path": id, "name": name, "size": int64(len(data)), "md5": hex.EncodeToString(h[:]), "created": "2021-04-05T12:34:56+03:00"}, "")
	return r
}

func TestZIPBulkImportsOnlyVerifiedEntriesAndKeepsServerBytes(t *testing.T) {
	s := testStore(t)
	data := picture(800, 600)
	resources := []Resource{zipTestResource("photounlim:/a.png", "a.png", data), zipTestResource("photounlim:/b.png", "b.png", data), zipTestResource("photounlim:/missing.png", "missing.png", data)}
	resources[0].Raw["meta"] = Object{"sizes": []any{Object{"name": "ORIGINAL", "url": "https://example.org/disk/a"}}}
	s.saveMany(resources)
	zipData := multiZIP(map[string][]byte{"a.png": data, "nested/b.png": data})
	var calls atomic.Int32
	c := apiFixture(t, func(d Object, base string) Object {
		calls.Add(1)
		if str(d["apiMethod"]) != "mpfs/bulk-download-prepare" || len(arr(obj(d["requestParams"])["items"])) != 3 {
			t.Error("expected one bulk request with all selected photos")
		}
		return Object{"download_url": base + "/zip"}
	}, map[string][]byte{"/zip": zipData}, nil)
	c.ZIPBatchSize = 500
	if e := runZIPPass(context.Background(), s, c, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if calls.Load() != 1 || st.Done != 2 || st.Original != 2 || st.Pending != 1 || st.Failed != 0 || st.ArchiveFiles != 1 || st.ZIPArchives != 1 || st.Archives != 2 {
		t.Fatalf("calls %d stats %+v", calls.Load(), st)
	}
	a, _ := s.result(resources[0].ID)
	b, _ := s.result(resources[1].ID)
	if a.Archive != b.Archive || !a.Bundle || a.Manifest == "" || a.Metadata.Created == "" {
		t.Fatal("shared archive or metadata missing", a)
	}
	full, _ := localArchive(s.Root, a.Archive)
	got, _ := os.ReadFile(full)
	if !bytes.Equal(got, zipData) {
		t.Fatal("server ZIP was repacked")
	}
	for _, r := range []Result{a, b} {
		if e := verifyZIP(context.Background(), full, r); e != nil {
			t.Fatal(e)
		}
	}
	meta, _ := localArchive(s.Root, a.Manifest)
	var manifest zipManifest
	x, _ := os.ReadFile(meta)
	if e := json.Unmarshal(x, &manifest); e != nil || len(manifest.Entries) != 2 || len(manifest.Missing) != 1 {
		t.Fatal("manifest", e)
	}
	z := s.zipStatus()
	if z.Imported != 2 || z.Missing != 1 || z.Archives != 1 {
		t.Fatal(z)
	}
}

func TestZIPBatchPlannerLimitsAndSeparatesDuplicateNames(t *testing.T) {
	var r []Resource
	for i := 0; i < 1001; i++ {
		r = append(r, Resource{ID: strings.Repeat("x", i%4), Name: "duplicate.jpg", Size: 1})
	}
	for i := 0; i < 501; i++ {
		r = append(r, Resource{Name: strings.Repeat("a", i+1) + ".jpg", Size: 1})
	}
	batches := splitZIPBatches(r, 500)
	n := 0
	for _, b := range batches {
		names := map[string]bool{}
		if len(b) > 500 {
			t.Fatal("oversize")
		}
		for _, f := range b {
			if names[f.Name] {
				t.Fatal("duplicate")
			}
			names[f.Name] = true
			n++
		}
	}
	if n != len(r) {
		t.Fatal("lost resources")
	}
	large := splitZIPBatches([]Resource{{Name: "a", Size: 5 << 30}, {Name: "b", Size: 5 << 30}}, 500)
	if len(large) != 2 {
		t.Fatal("size budget ignored")
	}
}

func TestZIPImportRejectsZeroMismatchAndAmbiguousNames(t *testing.T) {
	s := testStore(t)
	data := picture(800, 600)
	r := []Resource{zipTestResource("photounlim:/zero", "zero.png", data), zipTestResource("photounlim:/wrong", "wrong.png", data), zipTestResource("photounlim:/duplicate", "dupe.png", data)}
	s.saveMany(r)
	b := newZIPBatch(r)
	s.rememberZIPBatch(b)
	full, _ := localArchive(s.Root, b.Archive)
	os.MkdirAll(filepath.Dir(full), 0700)
	os.WriteFile(full, multiZIP(map[string][]byte{"zero.png": {}, "wrong.png": picture(20, 20), "a/dupe.png": data, "b/dupe.png": data}), 0600)
	matched, missing, e := importZIPBatch(context.Background(), s, b, r)
	if e != nil || matched != 0 || missing != 3 {
		t.Fatal(matched, missing, e)
	}
	st, _ := s.stats()
	if st.Done != 0 || st.Failed != 0 || st.Pending != 3 {
		t.Fatal("missing ZIP entries changed file status", st)
	}
}

func TestZIPDownloadedArchiveRecoversWithoutNetwork(t *testing.T) {
	s := testStore(t)
	data := picture(800, 600)
	r := zipTestResource("photounlim:/a", "a.png", data)
	s.save(r)
	b := newZIPBatch([]Resource{r})
	s.rememberZIPBatch(b)
	full, _ := localArchive(s.Root, b.Archive)
	os.MkdirAll(filepath.Dir(full), 0700)
	os.WriteFile(full, zipBytes("a.png", data), 0600)
	s.finishZIPState(b.ID, "downloaded", 1, nil)
	c := &Client{HTTP: &http.Client{}, Base: "invalid"}
	if e := runZIPPass(context.Background(), s, c, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Done != 1 || st.Original != 1 {
		t.Fatal(st)
	}
}

func TestZIP429DefersWholeBatchWithoutFileFailures(t *testing.T) {
	s := testStore(t)
	data := picture(800, 600)
	s.save(zipTestResource("photounlim:/a", "a.png", data))
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
		w.Write([]byte(`{"error":{"code":"mpfs"}}`))
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), Base: server.URL, Interval: time.Millisecond}
	e := runZIPPass(context.Background(), s, c, func(string) {})
	if !errors.Is(e, errZIPDeferred) || calls.Load() != 1 {
		t.Fatal(e, calls.Load())
	}
	st, _ := s.stats()
	if st.Failed != 0 || st.Pending != 1 || st.Done != 0 || c.rateStatus().LimitedUntil == "" || s.zipStatus().Stage != "deferred" {
		t.Fatal(st, c.rateStatus(), s.zipStatus())
	}
	var n int
	s.DB.QueryRow("SELECT COUNT(*) FROM version_errors").Scan(&n)
	if n != 0 {
		t.Fatal("rate limit became a file error")
	}
}

func TestSharedZIPVerificationDoesNotReadUnrelatedCorruptMember(t *testing.T) {
	s := testStore(t)
	data := picture(800, 600)
	r := zipTestResource("photounlim:/good", "good.png", data)
	file := filepath.Join(s.Root, "shared.zip")
	os.WriteFile(file, multiZIP(map[string][]byte{"good.png": data, "other.png": data}), 0600)
	zr, _ := zip.OpenReader(file)
	var offset int64
	for _, f := range zr.File {
		if f.Name == "other.png" {
			offset, _ = f.DataOffset()
		}
	}
	zr.Close()
	f, _ := os.OpenFile(file, os.O_RDWR, 0600)
	f.WriteAt([]byte{0xff}, offset)
	f.Close()
	result, _, e := validate(r, writeTestFile(t, data), "original")
	if e != nil {
		t.Fatal(e)
	}
	result.Entry = "good.png"
	result.Bundle = true
	if e := verifyZIP(context.Background(), file, result); e != nil {
		t.Fatal("valid entry was discarded due to unrelated damage", e)
	}
}

func writeTestFile(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "media")
	if e := os.WriteFile(p, data, 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
