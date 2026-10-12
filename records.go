package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// A Photoslice item reference (id/itemId, dimensions, albums) does not prove
// that its file metadata is available. A saved, verified copy does.
const fileMetadataSQL = `(json_extract(raw,'$.type')='file'
 OR COALESCE(NULLIF(json_extract(raw,'$.size'),0),json_extract(raw,'$.meta.size'),0)>0
 OR COALESCE(json_array_length(raw,'$.meta.sizes'),0)>0
 OR COALESCE(json_array_length(raw,'$.sizes'),0)>0
 OR json_type(raw,'$.preview')='text' OR json_type(raw,'$.meta.preview')='text'
 OR json_type(raw,'$.thumbnail')='text' OR json_type(raw,'$.meta.thumbnail')='text')`

const restBase = "https://cloud-api.yandex.net"

func (c *Client) isRESTURL(u *url.URL) bool {
	base := c.RESTBase
	if base == "" {
		base = restBase
	}
	b, e := url.Parse(base)
	return e == nil && u.Scheme == "https" && u.Host == b.Host && u.User == nil && strings.HasPrefix(u.Path, "/v1/disk/resources")
}

func (c *Client) restFileMetadata(ctx context.Context, s *Store, r Resource) error {
	if c.WebDAVToken == "" || !strings.HasPrefix(r.ID, "/disk/") {
		return errors.New("для ссылки нет доступного REST-пути")
	}
	base := c.RESTBase
	if base == "" {
		base = restBase
	}
	u, e := url.Parse(base + "/v1/disk/resources")
	if e != nil || !c.isRESTURL(u) {
		return errors.New("неверный адрес REST API")
	}
	q := u.Query()
	q.Set("path", "disk:"+strings.TrimPrefix(r.ID, "/disk"))
	q.Set("fields", "name,type,size,md5,mime_type,media_type,created,modified,preview,exif")
	q.Set("preview_size", "XXXL")
	q.Set("preview_crop", "false")
	u.RawQuery = q.Encode()
	res, e := c.request(ctx, "GET", u.String(), nil)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return &MediaStatusError{Status: res.StatusCode}
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if e != nil {
		return errors.New("не удалось прочитать метаданные REST API")
	}
	full, e := decodeJSON(b)
	if e != nil || str(full["type"]) != "file" {
		return errors.New("REST API не подтвердил запись как файл")
	}
	merged := Object{}
	for k, v := range r.Raw {
		merged[k] = v
	}
	for k, v := range full {
		merged[k] = v
	}
	merged["path"] = r.ID // Keep Photoslice identity; REST's disk:/ path is an alias.
	if preview := str(full["preview"]); preview != "" {
		meta := Object{}
		for k, v := range obj(merged["meta"]) {
			meta[k] = v
		}
		sizes := append([]any(nil), arr(meta["sizes"])...)
		sizes = append(sizes, Object{"name": "XXXL", "url": preview})
		meta["sizes"] = sizes
		merged["meta"] = meta
	}
	fresh, e := resource(merged, r.Cluster)
	if e != nil {
		return e
	}
	if e = s.save(fresh); e != nil {
		return &LocalError{e}
	}
	return nil
}

func (c *Client) reconcileREST(ctx context.Context, s *Store) (int, error) {
	cursor := s.get("records_rest_cursor")
	where := "NOT " + confirmedSQL("files") + " AND id LIKE '/disk/%'"
	rows, e := s.DB.Query("SELECT id FROM files WHERE "+where+" AND id>? ORDER BY id LIMIT 512", cursor)
	if e != nil {
		return 0, &LocalError{e}
	}
	var selected []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			break
		}
		selected = append(selected, id)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return 0, &LocalError{e}
	}
	if len(selected) == 0 && cursor != "" {
		if e = s.set("records_rest_cursor", ""); e != nil {
			return 0, &LocalError{e}
		}
		return c.reconcileREST(ctx, s)
	}
	checked := 0
	for _, id := range selected {
		if ctx.Err() != nil {
			break
		}
		r, le := s.load(id)
		if le != nil {
			return checked, &LocalError{le}
		}
		e = c.restFileMetadata(ctx, s, r)
		if fatal(e) && ctx.Err() == nil {
			return checked, e
		}
		reason := "confirmed"
		if e != nil {
			reason = "metadata_unavailable"
			var status *MediaStatusError
			if errors.As(e, &status) {
				reason = fmt.Sprintf("http_%d", status.Status)
			}
		}
		if _, le = s.DB.Exec("INSERT INTO record_checks(file_id,result,checked) VALUES(?,?,?) ON CONFLICT(file_id) DO UPDATE SET result=excluded.result,checked=excluded.checked", id, reason, time.Now().UTC().Format(time.RFC3339Nano)); le != nil {
			return checked, &LocalError{le}
		}
		if le = s.set("records_rest_cursor", id); le != nil {
			return checked, &LocalError{le}
		}
		checked++
	}
	return checked, nil
}

func confirmedSQL(alias string) string {
	return "(COALESCE(" + fileMetadataSQL + ",0) OR EXISTS(SELECT 1 FROM versions known_copy WHERE known_copy.file_id=" + alias + ".id))"
}

// This is deliberately outside the download stages. Keep every unresolved
// reference, probe a small rotating group, and promote it only on file metadata.
func (c *Client) reconcileReferences(ctx context.Context, s *Store, force bool, log func(string)) error {
	if !force {
		next, _ := time.Parse(time.RFC3339Nano, s.get("records_check_next"))
		if time.Now().Before(next) {
			return nil
		}
	}
	rows, e := s.DB.Query("SELECT DISTINCT cluster FROM files WHERE NOT " + confirmedSQL("files") + " AND cluster<>'' ORDER BY cluster")
	if e != nil {
		return &LocalError{e}
	}
	var clusters []string
	for rows.Next() {
		var cl string
		if e = rows.Scan(&cl); e != nil {
			break
		}
		clusters = append(clusters, cl)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return &LocalError{e}
	}
	if len(clusters) == 0 {
		return nil
	}
	c.mu.Lock()
	c.downloadStage = "records"
	c.mu.Unlock()
	cursor := s.get("records_check_cursor")
	start := 0
	for i, cl := range clusters {
		if cl > cursor {
			start = i
			break
		}
		start = 0
	}
	checkCtx, cancel := context.WithTimeout(ctx, recordCheckBudget(force))
	defer cancel()
	before, e := s.stats()
	if e != nil {
		return &LocalError{e}
	}
	restChecked := 0
	if c.FreshLinks == nil {
		c.FreshLinks = newLinkRefresher()
	}
	psid, e := c.FreshLinks.snapshotID(checkCtx, c)
	if e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log("Проверка кластеров неподтверждённых записей отложена; доступность не установлена.")
	}
	snapshotOK := e == nil
	checked := 0
	for snapshotOK && checked < len(clusters) && checked < 16 {
		if checkCtx.Err() != nil {
			break
		}
		cl := clusters[(start+checked)%len(clusters)]
		expected := int64(0)
		fmt.Sscan(s.get("cluster_size:"+cl), &expected)
		e = c.collectCluster(checkCtx, s, psid, cl, expected)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fatal(e) && checkCtx.Err() == nil {
			return e
		}
		if e != nil && checkCtx.Err() == nil {
			log("Один кластер не удалось дочитать; продолжаю отдельную проверку остальных записей.")
		}
		if se := s.set("records_check_cursor", cl); se != nil {
			return &LocalError{se}
		}
		checked++
	}
	if c.WebDAVToken != "" && checkCtx.Err() == nil {
		restChecked, e = c.reconcileREST(checkCtx, s)
		if fatal(e) && checkCtx.Err() == nil {
			return e
		}
	}
	if e = s.set("records_check_next", time.Now().Add(6*time.Hour).UTC().Format(time.RFC3339Nano)); e != nil {
		return &LocalError{e}
	}
	after, e := s.stats()
	if e != nil {
		return &LocalError{e}
	}
	log(fmt.Sprintf("Отдельная проверка записей: REST %d, кластеров %d; подтверждено %d; без метаданных осталось %d. Эти записи не занимают очередь скачивания.", restChecked, checked, max(0, before.Unconfirmed-after.Unconfirmed), after.Unconfirmed))
	return nil
}

func recordCheckBudget(force bool) time.Duration {
	if force {
		return 10 * time.Minute
	}
	return 2 * time.Minute
}
