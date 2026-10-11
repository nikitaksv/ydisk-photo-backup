package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpgradeKeepsAllThreeVersions(t *testing.T) {
	var ready atomic.Bool
	c := apiFixture(t, func(d Object, url string) Object {
		switch str(d["apiMethod"]) {
		case "intapi/photo-init-snapshot":
			return Object{"photoslice_id": "s"}
		case "intapi/photo-get-clusters-with-resources":
			return Object{"resources": Object{"fetched": []any{Object{"path": "photounlim:/a.png", "name": "a.png", "meta": Object{"created": "2024-03-02T14:15:16.123456789+03:00", "sizes": []any{Object{"name": "XXL", "url": url + "/preview", "width": 900, "height": 600}, Object{"name": "S", "url": url + "/thumb", "width": 90, "height": 90}}}}}}}
		default:
			if ready.Load() {
				return Object{"download_url": url + "/original"}
			}
			return Object{"download_url": url + "/missing"}
		}
	}, map[string][]byte{"/client/disk": []byte(`{"sk":"sk","uid":"1234"}`), "/original": zipBytes("a.png", picture(1200, 800)), "/preview": picture(900, 600), "/thumb": picture(90, 90)}, nil)
	s := testStore(t)
	r, _ := resource(Object{"path": "photounlim:/a.png", "name": "a.png", "thumbnail": c.Base + "/thumb"}, "day")
	s.save(r)
	s.set("cluster_size:day", "1")
	thumb, e := c.downloadSelected(context.Background(), s, r, "thumbnail")
	if e != nil {
		t.Fatal(e)
	}
	s.done(thumb)
	thumbPath, _ := localArchive(s.Root, thumb.Archive)
	before, _ := os.ReadFile(thumbPath)
	if e := runOptions(context.Background(), s, c, 2, true, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Original != 0 || st.Preview != 1 || st.Thumbnail != 1 || st.OnlyPreview != 1 {
		t.Fatalf("%+v", st)
	}
	ready.Store(true)
	if e := runOptions(context.Background(), s, c, 2, true, func(string) {}); e != nil {
		t.Fatal(e)
	}
	versions, _ := s.versions(r.ID)
	if len(versions) != 3 {
		t.Fatalf("versions=%v", versions)
	}
	after, _ := os.ReadFile(thumbPath)
	if !bytes.Equal(before, after) {
		t.Fatal("existing thumbnail overwritten")
	}
	st, _ = s.stats()
	if st.Total != 1 || st.Done != 1 || st.Archives != 3 || st.NoOriginal != 0 || st.NoPreview != 0 || st.NoThumbnail != 0 {
		t.Fatalf("%+v", st)
	}
	original := versions["original"]
	if original.Metadata.Created != "2024-03-02T14:15:16.123456789+03:00" {
		t.Fatalf("%+v", original.Metadata)
	}
	for _, v := range versions {
		p, _ := localArchive(s.Root, v.Archive)
		if e := verifyZIP(context.Background(), p, v); e != nil {
			t.Fatal(e)
		}
	}
}
func TestBulkDoesNotLoseMissingMember(t *testing.T) {
	var bulk, single atomic.Int32
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	for _, name := range []string{"a.png", "b.png"} {
		w, _ := zw.Create(name)
		w.Write(picture(500, 500))
	}
	zw.Close()
	c := apiFixture(t, func(d Object, url string) Object {
		if len(arr(obj(d["requestParams"])["items"])) > 1 {
			bulk.Add(1)
			return Object{"download_url": url + "/bulk"}
		}
		single.Add(1)
		return Object{"download_url": url + "/missing"}
	}, map[string][]byte{"/bulk": b.Bytes()}, nil)
	s := testStore(t)
	var resources []Resource
	for _, name := range []string{"a.png", "b.png", "c.png"} {
		r, _ := resource(Object{"path": "photounlim:/" + name, "name": name}, "d")
		s.save(r)
		resources = append(resources, r)
	}
	if e := c.processBatch(context.Background(), s, resources, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if bulk.Load() != 1 || single.Load() != 1 || st.Done != 2 || st.Failed != 1 || st.Original != 2 {
		t.Fatalf("bulk=%d single=%d %+v", bulk.Load(), single.Load(), st)
	}
	dirs, _ := os.ReadDir(filepath.Join(s.Root, "tmp"))
	if len(dirs) != 0 {
		t.Fatal("bulk staging was not cleaned")
	}
}
func TestSmallGenericPreviewIsPreservedAsThumbnail(t *testing.T) {
	c := apiFixture(t, func(Object, string) Object { return Object{} }, map[string][]byte{"/small": picture(100, 100)}, nil)
	s := testStore(t)
	r, _ := resource(Object{"path": "a.png", "name": "a.png", "preview": c.Base + "/small"}, "d")
	s.save(r)
	if e := c.processBatch(context.Background(), s, []Resource{r}, func(string) {}); e != nil {
		t.Fatal(e)
	}
	st, _ := s.stats()
	if st.Thumbnail != 1 || st.Preview != 0 || st.Failed != 0 {
		t.Fatalf("%+v", st)
	}
}
func TestBulkDuplicateNamesAreSeparated(t *testing.T) {
	c := apiFixture(t, func(Object, string) Object { t.Fatal("ambiguous filenames must not be batched"); return nil }, nil, nil)
	s := testStore(t)
	resources := []Resource{{ID: "one/a.png", Name: "a.png"}, {ID: "two/a.png", Name: "a.png"}}
	_, cleanup, e := c.prefetch(context.Background(), s, resources)
	cleanup()
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range resources {
		if r.OriginalZIP != "" {
			t.Fatal("duplicate filename was batched")
		}
	}
}
func TestStatisticsAndFiltersAreLogicalFiles(t *testing.T) {
	s := testStore(t)
	for _, id := range []string{"all-three", "preview", "thumb", "failed", "pending"} {
		r, _ := resource(Object{"path": id, "name": id}, "d")
		s.save(r)
	}
	for _, pair := range []struct{ id, q string }{{"all-three", "original"}, {"all-three", "preview"}, {"all-three", "thumbnail"}, {"preview", "preview"}, {"thumb", "thumbnail"}} {
		s.done(Result{ID: pair.id, Quality: pair.q, Bytes: 10})
	}
	s.failed("failed", fmt.Errorf("missing"))
	st, e := s.stats()
	if e != nil {
		t.Fatal(e)
	}
	if st.Total != 5 || st.Done != 3 || st.Archives != 5 || st.Original != 1 || st.OnlyPreview != 1 || st.OnlyThumbnail != 1 || st.NoOriginal != 4 || st.NoPreview != 3 || st.NoThumbnail != 3 || st.Bytes != 50 {
		t.Fatalf("%+v", st)
	}
	for filter, want := range map[string]int{"all": 5, "original-best": 1, "missing-original": 4, "missing-preview": 3, "missing-thumbnail": 3, "no-copy": 2, "failed": 1, "pending": 1, "preview-best": 1, "thumbnail-best": 1} {
		_, n, e := s.list(filter, 0, 50)
		if e != nil || n != want {
			t.Fatalf("%s: %d %v", filter, n, e)
		}
	}
	if _, _, e := s.list("'; DROP TABLE files; --", 0, 50); e == nil {
		t.Fatal("invalid filter accepted")
	}
}
func TestMetadataFallbackDatePrecisionAndUnknownTimezone(t *testing.T) {
	raw := Object{"meta": Object{"exif": Object{"DateTimeOriginal": "2024:07:05 13:14:15", "SubSecTimeOriginal": "123456", "OffsetTimeOriginal": "+03:00"}, "caption": "Family photo", "gps": Object{"lat": 55.1}, "preview": "https://example.invalid/?secret=hidden"}}
	m := metadataFrom(raw, nil)
	if m.Created != "2024-07-05T13:14:15.123456+03:00" || !m.TimezoneKnown {
		t.Fatalf("%+v", m)
	}
	unknown := metadataFrom(Object{"created": "2024-07-05T13:14:15.123456"}, nil)
	if unknown.TimezoneKnown || unknown.Created != "2024-07-05T13:14:15.123456" {
		t.Fatalf("%+v", unknown)
	}
	modified := metadataFrom(Object{"mtime": "2024-07-05T13:14:15Z"}, nil)
	if modified.Created != "" || len(modified.Dates) != 1 {
		t.Fatal("modification date must not be invented as creation date")
	}
	dateOnly := metadataFrom(Object{"created": "2024-07-05"}, nil)
	if dateOnly.Created != "2024-07-05" {
		t.Fatal("date-only evidence must not invent time")
	}
	preferred := metadataFrom(raw, Object{"DateTimeOriginal": "2023:02:04 12:13:14.123+03:00"})
	if preferred.Created != "2023-02-04T12:13:14.123+03:00" {
		t.Fatal("embedded capture time must take precedence", preferred)
	}
	s := testStore(t)
	raw["path"] = "missing.jpg"
	raw["name"] = "missing.jpg"
	r, _ := resource(raw, "d")
	s.save(r)
	s.failed(r.ID, fmt.Errorf("no media"))
	if e := s.report(); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(s.Root, "metadata.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(b, []byte("Family photo")) || !bytes.Contains(b, []byte("123456+03:00")) || bytes.Contains(b, []byte("secret=hidden")) {
		t.Fatal(string(b))
	}
}
func TestReadEXIFIncludesSubsecondAndOffset(t *testing.T) {
	date := []byte("2023:02:04 12:13:14\x00")
	sub := []byte("123456\x00")
	offset := []byte("+03:00\x00")
	data := make([]byte, 68+len(date)+len(sub)+len(offset))
	copy(data, []byte("II"))
	binary.LittleEndian.PutUint16(data[2:], 42)
	binary.LittleEndian.PutUint32(data[4:], 8)
	binary.LittleEndian.PutUint16(data[8:], 1)
	binary.LittleEndian.PutUint16(data[10:], 0x8769)
	binary.LittleEndian.PutUint16(data[12:], 4)
	binary.LittleEndian.PutUint32(data[14:], 1)
	binary.LittleEndian.PutUint32(data[18:], 26)
	binary.LittleEndian.PutUint16(data[26:], 3)
	pos := 68
	for i, item := range []struct {
		tag   uint16
		value []byte
	}{{0x9003, date}, {0x9291, sub}, {0x9011, offset}} {
		p := 28 + i*12
		binary.LittleEndian.PutUint16(data[p:], item.tag)
		binary.LittleEndian.PutUint16(data[p+2:], 2)
		binary.LittleEndian.PutUint32(data[p+4:], uint32(len(item.value)))
		binary.LittleEndian.PutUint32(data[p+8:], uint32(pos))
		copy(data[pos:], item.value)
		pos += len(item.value)
	}
	segment := append([]byte("Exif\x00\x00"), data...)
	jpeg := []byte{0xff, 0xd8, 0xff, 0xe1, 0, 0}
	binary.BigEndian.PutUint16(jpeg[4:], uint16(len(segment)+2))
	jpeg = append(jpeg, segment...)
	jpeg = append(jpeg, 0xff, 0xd9)
	file := filepath.Join(t.TempDir(), "exif.jpg")
	os.WriteFile(file, jpeg, 0600)
	exif, raw := readEXIF(file)
	m := metadataFrom(nil, exif)
	if !bytes.Equal(raw, data) || m.Created != "2023-02-04T12:13:14.123456+03:00" {
		t.Fatalf("%+v", m)
	}
}
func TestMetadataSurvivesMissingAndPartialRefresh(t *testing.T) {
	s := testStore(t)
	r, _ := resource(Object{"path": "photo-id", "name": "family.jpg", "meta": Object{"created": "2024-01-02T03:04:05.123Z", "caption": "Family", "sizes": []any{Object{"url": "https://example.invalid/old"}}}}, "d")
	if e := s.save(r); e != nil {
		t.Fatal(e)
	}
	missing, _ := resource(Object{"path": "photo-id"}, "d")
	if e := s.save(missing); e != nil {
		t.Fatal(e)
	}
	got, e := s.load("photo-id")
	if e != nil || got.Name != "family.jpg" || metadataFrom(got.Raw, nil).Created != "2024-01-02T03:04:05.123Z" {
		t.Fatalf("%+v %v", got, e)
	}
	partial, _ := resource(Object{"path": "photo-id", "meta": Object{"sizes": []any{Object{"url": "https://example.invalid/new"}}}}, "d")
	if e := s.saveMany([]Resource{partial}); e != nil {
		t.Fatal(e)
	}
	got, e = s.load("photo-id")
	if e != nil || str(obj(got.Raw["meta"])["caption"]) != "Family" || len(got.Candidates) != 1 || got.Candidates[0].URL != "https://example.invalid/new" {
		t.Fatalf("%+v %v", got, e)
	}
}
func TestMetadataZIPAndStatFingerprint(t *testing.T) {
	c := apiFixture(t, originalAPI, map[string][]byte{"/original": zipBytes("a.png", picture(500, 500))}, nil)
	s := testStore(t)
	r, _ := resource(Object{"path": "photounlim:/a.png", "name": "a.png", "created": "2024-01-02T03:04:05.123456789Z", "meta": Object{"caption": "Archive caption"}}, "day")
	s.save(r)
	got, e := c.downloadSelected(context.Background(), s, r, "original")
	if e != nil {
		t.Fatal(e)
	}
	s.done(got)
	if got.ArchiveSize == 0 || got.ArchiveMtime == 0 {
		t.Fatal("no archive fingerprint")
	}
	full, _ := localArchive(s.Root, got.Archive)
	zr, e := zip.OpenReader(full)
	if e != nil {
		t.Fatal(e)
	}
	defer zr.Close()
	found := false
	for _, f := range zr.File {
		if f.Name == "metadata/api.json" {
			src, _ := f.Open()
			b, _ := io.ReadAll(src)
			src.Close()
			if !bytes.Contains(b, []byte("Archive caption")) {
				t.Fatal(string(b))
			}
			found = true
		}
	}
	if !found {
		t.Fatal("API metadata not archived")
	}
	if e := verifyVersions(context.Background(), s, false, func(string) {}); e != nil {
		t.Fatal(e)
	}
}
func TestVersionMigrationFromPreviousSchema(t *testing.T) {
	root := t.TempDir()
	s, e := openStore(root)
	if e != nil {
		t.Fatal(e)
	}
	r, _ := resource(Object{"path": "id", "name": "a"}, "d")
	s.save(r)
	b, _ := json.Marshal(Result{ID: "id", Quality: "thumbnail", Bytes: 123})
	s.DB.Exec("UPDATE files SET status='done',result=? WHERE id='id'", string(b))
	s.DB.Close()
	s, e = openStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.DB.Close()
	v, _ := s.versions("id")
	st, _ := s.stats()
	if len(v) != 1 || st.Bytes != 123 || st.Thumbnail != 1 {
		t.Fatalf("%v %+v", v, st)
	}
}
func BenchmarkCollectMetadata(b *testing.B) {
	for _, batched := range []bool{false, true} {
		name := "individual-commits"
		if batched {
			name = "page-transaction"
		}
		b.Run(name, func(b *testing.B) {
			s, e := openStore(b.TempDir())
			if e != nil {
				b.Fatal(e)
			}
			defer s.DB.Close()
			var resources []Resource
			for i := 0; i < 100; i++ {
				r, _ := resource(Object{"path": fmt.Sprint(i), "name": "a.jpg", "created": time.Now().UTC().Format(time.RFC3339Nano)}, "d")
				resources = append(resources, r)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if batched {
					e = s.saveMany(resources)
				} else {
					for _, r := range resources {
						if e = s.save(r); e != nil {
							break
						}
					}
				}
				if e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
