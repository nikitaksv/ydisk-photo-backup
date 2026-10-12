package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"
)

func TestDirectOriginalSkipsZIPPreparationAfterPreview(t *testing.T) {
	original, preview, thumb := picture(900, 600), picture(700, 500), picture(150, 100)
	h := md5.Sum(original)
	c := apiFixture(t, func(Object, string) Object { t.Error("unexpected ZIP preparation"); return nil }, map[string][]byte{"/disk/direct": original, "/preview/large": preview, "/preview/small": thumb}, nil)
	r := Resource{ID: "photounlim:/a.png", Name: "a.png", Size: int64(len(original)), MD5: hex.EncodeToString(h[:]), Candidates: []Candidate{{URL: c.Base + "/disk/direct", Quality: "original"}, {URL: c.Base + "/preview/large", Quality: "preview"}, {URL: c.Base + "/preview/small", Quality: "thumbnail"}}}
	s := testStore(t)
	second := r
	second.ID, second.Name = "photounlim:/b.png", "b.png"
	if e := c.processBatch(context.Background(), s, []Resource{r, second}, func(string) {}); e != nil {
		t.Fatal(e)
	}
	versions, e := s.versions(r.ID)
	if e != nil || len(versions) != 2 || versions["preview"].ID == "" || versions["original"].MD5 != r.MD5 {
		t.Fatal(versions, e)
	}
}

func TestDownloadQueuePrioritizesReadyOriginalLinks(t *testing.T) {
	s := testStore(t)
	for _, raw := range []Object{
		{"type": "file", "path": "/disk/a", "meta": Object{}},
		{"type": "file", "path": "/photounlim/z", "meta": Object{"sizes": []any{Object{"name": "ORIGINAL", "url": "https://downloader.invalid/disk/signed"}}}},
	} {
		r, e := resource(raw, "")
		if e != nil {
			t.Fatal(e)
		}
		if e := s.save(r); e != nil {
			t.Fatal(e)
		}
	}
	ids, e := s.downloadQueue("1=1")
	if e != nil || len(ids) != 2 || ids[0].ID != "/photounlim/z" {
		t.Fatal(ids, e)
	}
}

func TestMissingDirectOriginalFallsBackWithoutZIPOrRepeatingURL(t *testing.T) {
	c := apiFixture(t, func(Object, string) Object { t.Error("unexpected ZIP preparation"); return nil }, map[string][]byte{"/preview/large": picture(700, 500)}, map[string]int{"/disk/direct": 404})
	r := Resource{ID: "photounlim:/a.png", Name: "a.png", FailedURLs: map[string]error{}, Candidates: []Candidate{{URL: c.Base + "/disk/direct", Quality: "original"}, {URL: c.Base + "/preview/large", Quality: "preview"}}}
	s := testStore(t)
	got, e := c.downloadOne(context.Background(), s, r)
	if e != nil || got.Quality != "preview" || r.FailedURLs[c.Base+"/disk/direct"] == nil {
		t.Fatal(got, e)
	}
}

func TestDirectOriginalRejectsMismatchedSizeAndPreviewRoute(t *testing.T) {
	c := apiFixture(t, func(Object, string) Object { t.Error("unexpected ZIP preparation"); return nil }, map[string][]byte{"/disk/direct": picture(600, 400)}, nil)
	r := Resource{ID: "photounlim:/a.png", Name: "a.png", Size: 999999, Candidates: []Candidate{{URL: c.Base + "/disk/direct", Quality: "original"}}}
	if _, e := c.downloadSelected(context.Background(), testStore(t), r, "original"); e == nil {
		t.Fatal("mismatched size accepted")
	}
	r.Candidates[0].URL = c.Base + "/preview/big?size=ORIGINAL"
	if len(directOriginalCandidates(r)) != 0 {
		t.Fatal("preview mistaken for direct original")
	}
}
