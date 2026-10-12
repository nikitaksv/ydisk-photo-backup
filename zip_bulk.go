package main

import (
	"archive/zip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// The batch scheduler may defer ZIP preparation after a 429, but must keep
// the shared cooldown and leave every file eligible for a later attempt.
type zipDeferKey struct{}

var errZIPDeferred = errors.New("подготовка ZIP отложена из-за временного HTTP 429")

var errZIPDisabled = errors.New("Яндекс отключил скачивание серверных ZIP (MPFS 394); оригиналы можно загружать отдельно")

// A confirmed disabled feature is different from a request quota. Probe it
// again on a later check, without slowing down ordinary media downloads.
func (s *Store) zipDisabledUntil() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s.get("zip_disabled_until"))
	return t
}

type ZIPStatus struct {
	Queued    int     `json:"queued"`
	Stage     string  `json:"stage"`
	BatchSize int     `json:"batch_size"`
	Batch     int     `json:"batch"`
	Batches   int     `json:"batches"`
	Requested int     `json:"requested"`
	Imported  int     `json:"imported"`
	Missing   int     `json:"missing"`
	Archives  int     `json:"archives"`
	Bytes     int64   `json:"bytes"`
	Seconds   float64 `json:"seconds"`
	Error     string  `json:"error,omitempty"`
}

type zipManifestEntry struct {
	Result Result `json:"result"`
	API    any    `json:"api"`
	EXIF   []byte `json:"embedded_exif_tiff,omitempty"`
}

type zipManifest struct {
	Archive   string             `json:"archive"`
	Requested []string           `json:"requested"`
	Entries   []zipManifestEntry `json:"entries"`
	Missing   map[string]string  `json:"missing"`
	Created   string             `json:"created"`
}

type zipBatchRecord struct {
	ID, Archive string
	IDs         []string
}

func (s *Store) zipStatus() ZIPStatus {
	var st ZIPStatus
	json.Unmarshal([]byte(s.get("zip_status")), &st)
	s.DB.QueryRow("SELECT COUNT(*) FROM original_zip_queue q JOIN files f ON f.id=q.file_id WHERE " + confirmedSQL("f") + " AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=q.file_id AND quality='original')").Scan(&st.Queued)
	return st
}

func (s *Store) saveZIPStatus(st ZIPStatus) error {
	b, e := json.Marshal(st)
	if e != nil {
		return e
	}
	if e := s.set("zip_status", string(b)); e != nil {
		return &LocalError{e}
	}
	return nil
}

// Duplicate basenames are separated into different ZIPs rather than guessing
// which renamed archive member belongs to which logical resource.
func splitZIPBatches(resources []Resource, limit int) [][]Resource {
	if limit < 1 || limit > 500 {
		limit = 500
	}
	var batches [][]Resource
	remaining := resources
	for len(remaining) > 0 {
		var batch, next []Resource
		names := map[string]bool{}
		var size int64
		for _, r := range remaining {
			estimate := r.Size
			if estimate <= 0 {
				estimate = 8 << 20
			}
			name := path.Base(strings.ReplaceAll(r.Name, "\\", "/"))
			if len(batch) >= limit || names[name] || len(batch) > 0 && size+estimate > 8<<30 {
				next = append(next, r)
				continue
			}
			batch = append(batch, r)
			names[name] = true
			size += estimate
		}
		batches = append(batches, batch)
		remaining = next
	}
	return batches
}

func newZIPBatch(resources []Resource) zipBatchRecord {
	ids := make([]string, 0, len(resources))
	for _, r := range resources {
		ids = append(ids, r.ID)
	}
	canonical := append([]string(nil), ids...)
	sort.Strings(canonical)
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	key := hex.EncodeToString(h[:])
	return zipBatchRecord{ID: key, Archive: filepath.ToSlash(filepath.Join("zip-batches", key+".zip")), IDs: ids}
}

func (s *Store) rememberZIPBatch(b zipBatchRecord) error {
	ids, _ := json.Marshal(b.IDs)
	_, e := s.DB.Exec(`INSERT INTO zip_batches(id,archive,items,state,updated) VALUES(?,?,?,'preparing',?) ON CONFLICT(id) DO UPDATE SET state='preparing',error='',updated=excluded.updated`, b.ID, b.Archive, string(ids), time.Now().UTC().Format(time.RFC3339Nano))
	if e != nil {
		return &LocalError{e}
	}
	return nil
}

func (s *Store) finishZIPState(id, state string, size int64, err error) error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	_, e := s.DB.Exec("UPDATE zip_batches SET state=?,bytes=?,error=?,updated=? WHERE id=?", state, size, msg, time.Now().UTC().Format(time.RFC3339Nano), id)
	if e != nil {
		return &LocalError{e}
	}
	return nil
}

func (s *Store) pendingZIPBatches() ([]zipBatchRecord, error) {
	rows, e := s.DB.Query("SELECT id,archive,items FROM zip_batches WHERE state IN ('preparing','downloaded') ORDER BY updated")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []zipBatchRecord
	for rows.Next() {
		var r zipBatchRecord
		var ids string
		if e := rows.Scan(&r.ID, &r.Archive, &ids); e != nil {
			return nil, e
		}
		if e := json.Unmarshal([]byte(ids), &r.IDs); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) zipQueue() ([]Resource, error) {
	// Plan with small records; only the current 500 resources need their full
	// API metadata and signed candidates in memory.
	rows, e := s.DB.Query(`SELECT id,name,COALESCE(NULLIF(json_extract(raw,'$.size'),0),json_extract(raw,'$.meta.size'),0) FROM files WHERE ` + confirmedSQL("files") + ` AND NOT ` + downloadVideoSQL + ` AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=files.id AND quality='original') ORDER BY EXISTS(SELECT 1 FROM versions WHERE file_id=files.id),id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []Resource
	for rows.Next() {
		var r Resource
		if e := rows.Scan(&r.ID, &r.Name, &r.Size); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (c *Client) fetchZIPBatch(ctx context.Context, s *Store, b zipBatchRecord) (int64, error) {
	dst, e := localArchive(s.Root, b.Archive)
	if e != nil {
		return 0, &LocalError{e}
	}
	if e := os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
		return 0, &LocalError{e}
	}
	// Recover the window between the durable rename and the SQLite checkpoint.
	if st, e := os.Stat(dst); e == nil {
		zr, e := zip.OpenReader(dst)
		if e != nil {
			return 0, &LocalError{errors.New("сохранённый пакет ZIP повреждён; оставлен для проверки")}
		}
		zr.Close()
		return st.Size(), s.finishZIPState(b.ID, "downloaded", st.Size(), nil)
	}
	if s.zipDisabledUntil().After(time.Now()) {
		return 0, errZIPDisabled
	}
	if e := s.rememberZIPBatch(b); e != nil {
		return 0, e
	}
	d, e := c.call(context.WithValue(ctx, zipDeferKey{}, true), "mpfs/bulk-download-prepare", Object{"items": b.IDs})
	if e != nil {
		if errors.Is(e, errZIPDisabled) {
			if se := s.set("zip_disabled_until", time.Now().Add(6*time.Hour).UTC().Format(time.RFC3339Nano)); se != nil {
				return 0, &LocalError{se}
			}
		}
		if se := s.finishZIPState(b.ID, "deferred", 0, e); se != nil {
			return 0, se
		}
		return 0, e
	}
	if e := s.set("zip_disabled_until", ""); e != nil {
		return 0, &LocalError{e}
	}
	uri := first(d, "download_url")
	if uri == "" {
		return 0, errors.New("подготовка пакета ZIP: нет ссылки download_url")
	}
	tmp := dst + ".part"
	defer os.Remove(tmp)
	if e := c.media(ctx, uri, tmp); e != nil {
		return 0, e
	}
	zr, e := zip.OpenReader(tmp)
	if e != nil {
		return 0, errors.New("пакетный ответ Яндекса не является ZIP")
	}
	zr.Close()
	if e := ctx.Err(); e != nil {
		return 0, e
	}
	if e := os.Rename(tmp, dst); e != nil {
		return 0, &LocalError{e}
	}
	st, e := os.Stat(dst)
	if e != nil {
		return 0, &LocalError{e}
	}
	return st.Size(), s.finishZIPState(b.ID, "downloaded", st.Size(), nil)
}

func readZIPOriginal(ctx context.Context, entry *zip.File, r Resource, scratch string) (Result, []byte, error) {
	if entry.UncompressedSize64 == 0 {
		return Result{}, nil, errors.New("нулевой размер оригинала в ZIP")
	}
	if r.Size > 0 && entry.UncompressedSize64 != uint64(r.Size) {
		return Result{}, nil, errors.New("размер оригинала в ZIP не совпадает с метаданными")
	}
	if r.Size <= 0 && entry.UncompressedSize64 > 2<<30 {
		return Result{}, nil, errors.New("слишком большой оригинал без известного размера")
	}
	src, e := entry.Open()
	if e != nil {
		return Result{}, nil, errors.New("ZIP: не удалось открыть запись")
	}
	defer src.Close()
	dst, e := os.OpenFile(scratch, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return Result{}, nil, &LocalError{e}
	}
	sha, md := sha256.New(), md5.New()
	n, ce := copyStream(io.MultiWriter(dst, sha, md), &ctxReader{ctx: ctx, r: src})
	fe := dst.Close()
	if fe != nil {
		return Result{}, nil, &LocalError{fe}
	}
	if ce != nil {
		if ctx.Err() != nil {
			return Result{}, nil, ctx.Err()
		}
		var pe *os.PathError
		if errors.As(ce, &pe) {
			return Result{}, nil, &LocalError{ce}
		}
		return Result{}, nil, errors.New("ZIP: повреждён оригинал / CRC не совпадает")
	}
	result, _, e := validateDigest(r, scratch, "original", Digest{n, hex.EncodeToString(sha.Sum(nil)), hex.EncodeToString(md.Sum(nil))})
	if e != nil {
		return Result{}, nil, e
	}
	exif, raw := readEXIF(scratch)
	result.Metadata = metadataFrom(r.Raw, exif)
	result.Entry = entry.Name
	result.Bundle = true
	return result, raw, nil
}

// Commit all independently verified entries in one durable transaction, after
// both the unchanged server ZIP and its metadata manifest have been flushed.
func (s *Store) commitZIPBatch(b zipBatchRecord, results []Result, missing int) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return &LocalError{e}
	}
	defer tx.Rollback()
	for _, r := range results {
		data, _ := json.Marshal(r)
		if _, e := tx.Exec("INSERT OR IGNORE INTO versions(file_id,quality,result,added,bytes) VALUES(?,?,?,?,?)", r.ID, "original", string(data), time.Now().UTC().Format(time.RFC3339Nano), r.Bytes); e != nil {
			return &LocalError{e}
		}
		if _, e := tx.Exec("DELETE FROM version_errors WHERE file_id=? AND quality='original'", r.ID); e != nil {
			return &LocalError{e}
		}
		var best string
		if e := tx.QueryRow(bestVersion, r.ID).Scan(&best); e != nil {
			return &LocalError{e}
		}
		if _, e := tx.Exec("UPDATE files SET status='done',result=?,error='' WHERE id=?", best, r.ID); e != nil {
			return &LocalError{e}
		}
	}
	if _, e := tx.Exec("UPDATE zip_batches SET state='imported',matched=?,missing=?,error='',updated=? WHERE id=?", len(results), missing, time.Now().UTC().Format(time.RFC3339Nano), b.ID); e != nil {
		return &LocalError{e}
	}
	if e := tx.Commit(); e != nil {
		return &LocalError{e}
	}
	return nil
}

func importZIPBatch(ctx context.Context, s *Store, b zipBatchRecord, resources []Resource) (int, int, error) {
	full, e := localArchive(s.Root, b.Archive)
	if e != nil {
		return 0, 0, &LocalError{e}
	}
	zr, e := zip.OpenReader(full)
	if e != nil {
		return 0, 0, errors.New("пакет ZIP не читается")
	}
	defer zr.Close()
	st, e := os.Stat(full)
	if e != nil {
		return 0, 0, &LocalError{e}
	}
	byName := map[string][]*zip.File{}
	resourceCounts := map[string]int{}
	for _, r := range resources {
		resourceCounts[path.Base(strings.ReplaceAll(r.Name, "\\", "/"))]++
	}
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := path.Base(strings.ReplaceAll(f.Name, "\\", "/"))
		byName[name] = append(byName[name], f)
	}
	dir, e := os.MkdirTemp(filepath.Join(s.Root, "tmp"), "zip-check-")
	if e != nil {
		return 0, 0, &LocalError{e}
	}
	defer os.RemoveAll(dir)
	manifest := zipManifest{Archive: b.Archive, Requested: b.IDs, Missing: map[string]string{}, Created: time.Now().UTC().Format(time.RFC3339Nano)}
	manifestPath := strings.TrimSuffix(b.Archive, ".zip") + ".manifest.json"
	var results []Result
	for _, r := range resources {
		if e := ctx.Err(); e != nil {
			return 0, 0, e
		}
		name := path.Base(strings.ReplaceAll(r.Name, "\\", "/"))
		entries := byName[name]
		if resourceCounts[name] != 1 || len(entries) > 1 {
			manifest.Missing[r.ID] = "неоднозначное имя в ZIP; требуется отдельная загрузка"
			continue
		}
		if len(entries) == 0 {
			manifest.Missing[r.ID] = "файл отсутствует в ZIP; требуется отдельная проверка"
			continue
		}
		result, exif, e := readZIPOriginal(ctx, entries[0], r, filepath.Join(dir, "media.part"))
		if e != nil {
			if fatal(e) {
				return 0, 0, e
			}
			manifest.Missing[r.ID] = e.Error()
			continue
		}
		result.Archive = b.Archive
		result.Manifest = manifestPath
		result.ArchiveSize = st.Size()
		result.ArchiveMtime = st.ModTime().UnixNano()
		results = append(results, result)
		manifest.Entries = append(manifest.Entries, zipManifestEntry{Result: result, API: sanitizedAPI(r.Raw), EXIF: exif})
	}
	meta, e := localArchive(s.Root, manifestPath)
	if e != nil {
		return 0, 0, &LocalError{e}
	}
	if e := atomicFile(meta, func(f *os.File) error { return json.NewEncoder(f).Encode(manifest) }); e != nil {
		return 0, 0, &LocalError{e}
	}
	if e := ctx.Err(); e != nil {
		return 0, 0, e
	}
	if e := s.commitZIPBatch(b, results, len(manifest.Missing)); e != nil {
		return 0, 0, e
	}
	return len(results), len(manifest.Missing), nil
}

func runZIPPass(ctx context.Context, s *Store, c *Client, log func(string)) error {
	limit := c.ZIPBatchSize
	if limit < 1 || limit > 500 {
		limit = 500
	}
	started := time.Now()
	status := ZIPStatus{Stage: "preparing", BatchSize: limit}
	if e := s.saveZIPStatus(status); e != nil {
		return e
	}
	// Fully downloaded ZIPs can be imported offline after an interruption.
	previous, e := s.pendingZIPBatches()
	if e != nil {
		return &LocalError{e}
	}
	for _, b := range previous {
		full, e := localArchive(s.Root, b.Archive)
		if e != nil {
			return &LocalError{e}
		}
		if _, e := os.Stat(full); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return &LocalError{e}
		}
		var resources []Resource
		for _, id := range b.IDs {
			r, e := s.load(id)
			if e != nil {
				return &LocalError{e}
			}
			resources = append(resources, r)
		}
		matched, missing, e := importZIPBatch(ctx, s, b, resources)
		if e != nil {
			return e
		}
		st, e := os.Stat(full)
		if e != nil {
			return &LocalError{e}
		}
		status.Requested += len(b.IDs)
		status.Imported += matched
		status.Missing += missing
		status.Archives++
		status.Bytes += st.Size()
		log(fmt.Sprintf("Восстановлен сохранённый ZIP: оригиналов %d, требуют отдельной проверки %d", matched, missing))
	}
	resources, e := s.zipQueue()
	if e != nil {
		return &LocalError{e}
	}
	batches := splitZIPBatches(resources, limit)
	recovered := status.Archives
	status.Batches = recovered + len(batches)
	log(fmt.Sprintf("ZIP: %d фотографий без оригинала, %d пачек, до %d снимков; архивы скачиваются последовательно.", len(resources), len(batches), limit))
	if len(batches) > 0 && s.zipDisabledUntil().After(time.Now()) {
		status.Stage, status.Error = "unavailable", errZIPDisabled.Error()
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		return errZIPDisabled
	}
	for i, batch := range batches {
		if c.ZIPMaxBatches > 0 && i >= c.ZIPMaxBatches {
			break
		}
		if e := ctx.Err(); e != nil {
			return e
		}
		status.Batch = recovered + i + 1
		status.Requested += len(batch)
		status.Stage = "downloading"
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		log(fmt.Sprintf("ZIP %d/%d: запрашиваю %d фотографий.", i+1, len(batches), len(batch)))
		for j, r := range batch {
			if e := ctx.Err(); e != nil {
				return e
			}
			loaded, e := s.load(r.ID)
			if e != nil {
				return &LocalError{e}
			}
			batch[j] = loaded
		}
		b := newZIPBatch(batch)
		size, e := c.fetchZIPBatch(ctx, s, b)
		if e != nil {
			status.Stage = "deferred"
			if errors.Is(e, errZIPDisabled) {
				status.Stage = "unavailable"
			}
			status.Error = e.Error()
			status.Seconds = time.Since(started).Seconds()
			if se := s.saveZIPStatus(status); se != nil {
				return se
			}
			return e
		}
		status.Stage = "verifying"
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		matched, missing, e := importZIPBatch(ctx, s, b, batch)
		if e != nil {
			return e
		}
		status.Imported += matched
		status.Missing += missing
		status.Archives++
		status.Bytes += size
		status.Seconds = time.Since(started).Seconds()
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		log(fmt.Sprintf("ZIP %d: проверены оригиналы %d/%d, отдельная проверка %d; архив %.1f МБ. Прогресс сохранён.", i+1, matched, len(batch), missing, float64(size)/1e6))
		// Retain only lightweight planning records between batches.
		batches[i] = nil
	}
	status.Stage = "completed"
	status.Seconds = time.Since(started).Seconds()
	return s.saveZIPStatus(status)
}
