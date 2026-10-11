package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, e := openStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.DB.Close() })
	os.MkdirAll(filepath.Join(s.Root, "tmp"), 0700)
	return s
}
func picture(w, h int) []byte {
	var b bytes.Buffer
	png.Encode(&b, image.NewRGBA(image.Rect(0, 0, w, h)))
	return b.Bytes()
}
func zipBytes(name string, data []byte) []byte {
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	w, _ := z.Create(name)
	w.Write(data)
	z.Close()
	return b.Bytes()
}
func apiFixture(t *testing.T, body func(Object, string) Object, media map[string][]byte, status map[string]int) *Client {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models-v2" {
			b, _ := io.ReadAll(r.Body)
			d, e := decodeJSON(b)
			if e != nil {
				t.Error(e)
				w.WriteHeader(400)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(body(d, server.URL))
			return
		}
		if n := status[r.URL.Path]; n != 0 {
			w.WriteHeader(n)
			return
		}
		if b, ok := media[r.URL.Path]; ok {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(b)
			return
		}
		w.WriteHeader(404)
	}))
	t.Cleanup(server.Close)
	return &Client{HTTP: server.Client(), Base: server.URL}
}
func originalAPI(d Object, url string) Object { return Object{"download_url": url + "/original"} }
func TestOriginalVerifiedAndArchived(t *testing.T) {
	data := picture(800, 600)
	h := md5.Sum(data)
	r := Resource{ID: "photounlim:/a.png", Name: "a.png", Size: int64(len(data)), MD5: hex.EncodeToString(h[:])}
	c := apiFixture(t, originalAPI, map[string][]byte{"/original": zipBytes("a.png", data)}, nil)
	s := testStore(t)
	got, e := c.downloadOne(context.Background(), s, r)
	if e != nil {
		t.Fatal(e)
	}
	if got.Quality != "original" || got.Width != 800 {
		t.Fatalf("%+v", got)
	}
	full, _ := localArchive(s.Root, got.Archive)
	if e := verifyZIP(context.Background(), full, got); e != nil {
		t.Fatal(e)
	}
	zr, e := zip.OpenReader(full)
	if e != nil {
		t.Fatal(e)
	}
	defer zr.Close()
	if len(zr.File) != 3 {
		t.Fatal("media + metadata + API metadata expected")
	}
	files, _ := os.ReadDir(filepath.Join(s.Root, "tmp"))
	if len(files) != 0 {
		t.Fatal("temporary media was not cleaned")
	}
}
func TestFallbackSelectsLargestReturnedPreview(t *testing.T) {
	large, small := picture(900, 600), picture(80, 80)
	c := apiFixture(t, originalAPI, map[string][]byte{"/large": large, "/small": small}, map[string]int{"/original": 404})
	s := testStore(t)
	raw := Object{"path": "photounlim:/lost.heic", "name": "lost.heic", "meta": Object{"sizes": []any{Object{"name": "S", "url": c.Base + "/small", "width": 80, "height": 80}, Object{"name": "XXL", "url": c.Base + "/large", "width": 900, "height": 600}}}}
	r, e := resource(raw, "day")
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Candidates) != 2 || r.Candidates[0].URL != c.Base+"/large" {
		t.Fatalf("%+v", r.Candidates)
	}
	got, e := c.downloadOne(context.Background(), s, r)
	if e != nil {
		t.Fatal(e)
	}
	if got.Quality != "preview" || got.Width != 900 || !strings.HasSuffix(got.Entry, "__preview.png") {
		t.Fatalf("%+v", got)
	}
	if got.Warning == "" {
		t.Fatal("original failure must be recorded")
	}
}
func TestThumbnailWhenOriginalAndLargeUnavailable(t *testing.T) {
	c := apiFixture(t, originalAPI, map[string][]byte{"/thumb": picture(100, 100)}, map[string]int{"/original": 404, "/large": 403})
	s := testStore(t)
	r := Resource{ID: "photounlim:/v.mp4", Name: "v.mp4", Candidates: []Candidate{{URL: c.Base + "/large", Quality: "preview"}, {URL: c.Base + "/thumb", Quality: "thumbnail"}}}
	got, e := c.downloadOne(context.Background(), s, r)
	if e != nil {
		t.Fatal(e)
	}
	if got.Quality != "thumbnail" || !strings.HasSuffix(got.Entry, ".png") {
		t.Fatalf("%+v", got)
	}
}
func TestMissingSingleFileDoesNotMarkAnotherDone(t *testing.T) {
	data := picture(400, 400)
	c := apiFixture(t, originalAPI, map[string][]byte{"/original": zipBytes("a.png", data)}, nil)
	s := testStore(t)
	for _, name := range []string{"a.png", "b.png"} {
		r, _ := resource(Object{"path": "photounlim:/" + name, "name": name}, "day")
		if e := s.save(r); e != nil {
			t.Fatal(e)
		}
		got, e := c.downloadOne(context.Background(), s, r)
		if e == nil {
			if e := s.done(got); e != nil {
				t.Fatal(e)
			}
		} else {
			if e := s.failed(r.ID, e); e != nil {
				t.Fatal(e)
			}
		}
	}
	st, e := s.stats()
	if e != nil {
		t.Fatal(e)
	}
	if st.Done != 1 || st.Failed != 1 {
		t.Fatalf("%+v", st)
	}
}
func TestZeroAndWrongHashOriginalFallback(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
		md5  string
		size int64
	}{{"zero", nil, "", 0}, {"hash", picture(500, 400), strings.Repeat("0", 32), 0}, {"size", picture(500, 400), "", 1}} {
		t.Run(test.name, func(t *testing.T) {
			c := apiFixture(t, originalAPI, map[string][]byte{"/original": zipBytes("a.png", test.data), "/thumb": picture(100, 100)}, nil)
			s := testStore(t)
			r := Resource{ID: "photounlim:/a.png", Name: "a.png", MD5: test.md5, Size: test.size, Candidates: []Candidate{{URL: c.Base + "/thumb", Quality: "thumbnail"}}}
			got, e := c.downloadOne(context.Background(), s, r)
			if e != nil {
				t.Fatal(e)
			}
			if got.Quality != "thumbnail" {
				t.Fatalf("%+v", got)
			}
		})
	}
}
func TestHTMLAndBrokenPreviewAreNotSuccessful(t *testing.T) {
	c := apiFixture(t, originalAPI, map[string][]byte{"/bad": []byte("<!doctype html><html>login</html>"), "/broken": []byte("\x89PNG\r\n\x1a\ninvalid")}, map[string]int{"/original": 404})
	s := testStore(t)
	r := Resource{ID: "photounlim:/a.png", Name: "a.png", Candidates: []Candidate{{URL: c.Base + "/bad", Quality: "preview"}, {URL: c.Base + "/broken", Quality: "thumbnail"}}}
	if _, e := c.downloadOne(context.Background(), s, r); e == nil {
		t.Fatal("bad responses marked successful")
	}
}
func TestCollectPaginatedSeparateResourcesAndMissing(t *testing.T) {
	calls := 0
	c := apiFixture(t, func(d Object, url string) Object {
		p := obj(d["requestParams"])
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			return Object{"photoslice_id": "snapshot"}
		case "intapi/photo-get-snapshot":
			calls++
			switch num(p["offset"]) {
			case 0:
				return Object{"items": []any{Object{"id": "c1", "size": 1}}}
			case 1:
				return Object{"items": []any{Object{"id": "c2", "size": 1}}}
			default:
				return Object{"items": []any{}}
			}
		default:
			if _, ok := obj(p["clusters"])["c1"]; ok {
				return Object{"clusters": Object{"fetched": []any{Object{"id": "c1", "items": []any{"opaque-id"}}}}, "resources": Object{"fetched": []any{Object{"id": "opaque-id", "path": "photounlim:/a.png", "name": "a.png", "meta": Object{"preview": url + "/preview"}}}}}
			}
			return Object{"clusters": Object{"fetched": []any{Object{"id": "c2", "items": []any{"photounlim:/lost.jpg"}}}}, "resources": Object{"fetched": []any{}, "missing": []any{"photounlim:/lost.jpg"}}}
		}
	}, nil, nil)
	s := testStore(t)
	if e := c.collect(context.Background(), s, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if calls != 3 || st.Total != 2 || st.Inventory != "complete" {
		t.Fatalf("calls=%d %+v", calls, st)
	}
	r, e := s.load("photounlim:/a.png")
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Candidates) != 1 {
		t.Fatalf("metadata lost: %+v", r)
	}
}
func TestIncompleteClusterCannotProduceCompleteInventory(t *testing.T) {
	c := apiFixture(t, func(d Object, _ string) Object {
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			return Object{"photoslice_id": "s"}
		case "intapi/photo-get-snapshot":
			if num(obj(d["requestParams"])["offset"]) == 0 {
				return Object{"items": []any{Object{"id": "c", "size": 3}}, "total": 1}
			}
			return Object{"items": []any{}}
		default:
			return Object{"clusters": Object{"fetched": []any{Object{"id": "c", "items": []any{}}}}, "resources": Object{"fetched": []any{}, "missing": []any{}}}
		}
	}, nil, nil)
	s := testStore(t)
	if e := c.collect(context.Background(), s, func(string) {}); e == nil {
		t.Fatal("incomplete count accepted")
	}
	if s.get("inventory") != "incomplete" {
		t.Fatal("wrong inventory status")
	}
}
func TestResumeVerifiesAndSkipsGoodArchives(t *testing.T) {
	c := apiFixture(t, originalAPI, map[string][]byte{"/original": zipBytes("a.png", picture(500, 500))}, nil)
	s := testStore(t)
	r, _ := resource(Object{"path": "photounlim:/a.png", "name": "a.png"}, "c")
	s.save(r)
	got, e := c.downloadOne(context.Background(), s, r)
	if e != nil {
		t.Fatal(e)
	}
	s.done(got)
	if e := verifySaved(context.Background(), s, func(string) {}); e != nil {
		t.Fatal(e)
	}
	ids, e := s.ids("status!='done'")
	if e != nil || len(ids) != 0 {
		t.Fatal("verified archive not skipped")
	}
	full, _ := localArchive(s.Root, got.Archive)
	os.WriteFile(full, []byte("broken"), 0600)
	if e := verifySaved(context.Background(), s, func(string) {}); e != nil {
		t.Fatal(e)
	}
	ids, _ = s.ids("status!='done'")
	if len(ids) != 1 {
		t.Fatal("corruption not retried")
	}
	newResult, e := c.downloadOne(context.Background(), s, r)
	if e != nil {
		t.Fatal(e)
	}
	if e := verifyZIP(context.Background(), full, newResult); e != nil {
		t.Fatal(e)
	}
	quarantine, _ := filepath.Glob(full + ".corrupt-*")
	if len(quarantine) != 1 {
		t.Fatal("corrupt copy not preserved")
	}
}

func TestParallelPipelineReportsPartialAndResumes(t *testing.T) {
	var mu sync.Mutex
	prepares := map[string]int{}
	data := picture(500, 500)
	c := apiFixture(t, func(d Object, url string) Object {
		p := obj(d["requestParams"])
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			return Object{"photoslice_id": "s"}
		case "intapi/photo-get-snapshot":
			return Object{"items": []any{Object{"id": "day", "size": 3}}, "total": 1}
		case "intapi/photo-get-clusters-with-resources":
			return Object{"resources": Object{"fetched": []any{Object{"path": "photounlim:/a.png", "name": "a.png"}, Object{"path": "photounlim:/b.png", "name": "b.png", "meta": Object{"thumbnail": url + "/thumb"}}}, "missing": []any{"photounlim:/c.png"}}}
		default:
			id := str(arr(p["items"])[0])
			mu.Lock()
			prepares[id]++
			mu.Unlock()
			if id == "photounlim:/a.png" {
				return Object{"download_url": url + "/original"}
			}
			return Object{}
		}
	}, map[string][]byte{"/client/disk": []byte(`{"sk":"test-sk","uid":"1234"}`), "/original": zipBytes("a.png", data), "/thumb": picture(90, 90)}, nil)
	s := testStore(t)
	if e := runWithClient(context.Background(), s, c, 3, func(string) {}); e == nil {
		t.Fatal("partial pipeline returned success")
	}
	st, e := s.stats()
	if e != nil {
		t.Fatal(e)
	}
	if st.Done != 2 || st.Failed != 1 || st.Original != 1 || st.Thumbnail != 1 || st.Inventory != "complete" {
		t.Fatalf("%+v", st)
	}
	if _, e := os.Stat(filepath.Join(s.Root, "report.csv")); e != nil {
		t.Fatal(e)
	}
	if e := runWithClient(context.Background(), s, c, 3, func(string) {}); e == nil {
		t.Fatal("remaining failure vanished")
	}
	mu.Lock()
	defer mu.Unlock()
	if prepares["photounlim:/a.png"] != 1 || prepares["photounlim:/b.png"] != 2 || prepares["photounlim:/c.png"] != 2 {
		t.Fatalf("resume redownloaded successes: %v", prepares)
	}
}
func TestAPICaptchaIsFatal(t *testing.T) {
	c := apiFixture(t, func(Object, string) Object { return Object{"type": "captcha"} }, nil, nil)
	if _, e := c.call(context.Background(), "test", nil); !errors.Is(e, ErrAuth) {
		t.Fatalf("%v", e)
	}
}

func TestRepeatedSnapshotStillCollectsKnownClusters(t *testing.T) {
	c := apiFixture(t, func(d Object, _ string) Object {
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			return Object{"photoslice_id": "s"}
		case "intapi/photo-get-snapshot":
			return Object{"items": []any{Object{"id": "day", "size": 1}}}
		default:
			return Object{"clusters": Object{"fetched": []any{Object{"id": "day", "items": []any{Object{"path": "photounlim:/a.png", "name": "a.png"}}}}}}
		}
	}, nil, nil)
	s := testStore(t)
	if e := c.collect(context.Background(), s, func(string) {}); e == nil {
		t.Fatal("repeated snapshot accepted as complete")
	}
	st, _ := s.stats()
	if st.Total != 1 || st.Inventory != "incomplete" {
		t.Fatalf("known resources discarded: %+v", st)
	}
}
func TestIdenticalNamesRemainSeparateArchives(t *testing.T) {
	c := apiFixture(t, originalAPI, map[string][]byte{"/original": zipBytes("a.png", picture(500, 500))}, nil)
	s := testStore(t)
	a, e := c.downloadOne(context.Background(), s, Resource{ID: "photounlim:/one/a.png", Name: "a.png"})
	if e != nil {
		t.Fatal(e)
	}
	b, e := c.downloadOne(context.Background(), s, Resource{ID: "photounlim:/two/a.png", Name: "a.png"})
	if e != nil {
		t.Fatal(e)
	}
	if a.Archive == b.Archive {
		t.Fatal("same names collide")
	}
}
func TestCancellationAndSafePaths(t *testing.T) {
	c := apiFixture(t, originalAPI, nil, nil)
	s := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.downloadOne(ctx, s, Resource{ID: "a", Name: "a"}); !errors.Is(e, context.Canceled) {
		t.Fatalf("%v", e)
	}
	if _, e := localArchive(s.Root, "../../escape.zip"); e == nil {
		t.Fatal("path escape")
	}
	if strings.ContainsAny(safeName(`../../evil\a:*.jpg`), `/\:*`) {
		t.Fatal("unsafe name")
	}
}
func TestLockAndCookieDomains(t *testing.T) {
	root := t.TempDir()
	release, e := lockRoot(root)
	if e != nil {
		t.Fatal(e)
	}
	if r, e := lockRoot(root); e == nil {
		r()
		t.Fatal("duplicate process lock")
	}
	release()
	r, e := lockRoot(root)
	if e != nil {
		t.Fatal("lock not released")
	}
	r()
	cookies, e := parseCookies([]byte(`[{"name":"Session_id","value":"test","domain":".yandex.ru","httpOnly":true},{"name":"evil","value":"x","domain":"evilyandex.ru"}]`))
	if e != nil || len(cookies) != 1 || !cookies[0].HTTPOnly {
		t.Fatalf("%+v %v", cookies, e)
	}
}
func TestReportContainsQualityWithoutSignedURL(t *testing.T) {
	s := testStore(t)
	r, _ := resource(Object{"path": "photounlim:/a.jpg", "name": "a.jpg", "preview": "https://example.invalid/?secret=hidden"}, "c")
	s.save(r)
	s.done(Result{ID: r.ID, Quality: "thumbnail", Bytes: 10, Width: 50, Height: 50})
	if e := s.report(); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(s.Root, "report.csv"))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(b, []byte("thumbnail")) || bytes.Contains(b, []byte("secret")) {
		t.Fatal(string(b))
	}
}
