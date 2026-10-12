package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"time"
)

type Store struct {
	DB   *sql.DB
	Root string
}

func openStore(root string) (*Store, error) {
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	db, e := sql.Open("sqlite", filepath.Join(root, "state.db"))
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS files(id TEXT PRIMARY KEY,confirmed INTEGER NOT NULL DEFAULT 0,cluster TEXT NOT NULL,name TEXT NOT NULL,raw TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',result TEXT NOT NULL DEFAULT '{}',error TEXT NOT NULL DEFAULT '',updated TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS files_status ON files(status);
 CREATE TRIGGER IF NOT EXISTS preserve_file_metadata AFTER UPDATE OF raw ON files
 WHEN NEW.raw<>json_patch(OLD.raw,NEW.raw)
 BEGIN UPDATE files SET raw=json_patch(OLD.raw,NEW.raw),name=CASE WHEN json_type(NEW.raw,'$.name') IS NULL THEN OLD.name ELSE NEW.name END WHERE id=NEW.id; END;
 CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS original_zip_queue(file_id TEXT PRIMARY KEY,attempted INTEGER NOT NULL DEFAULT 0);
	CREATE TABLE IF NOT EXISTS zip_batches(id TEXT PRIMARY KEY,archive TEXT NOT NULL,items TEXT NOT NULL,state TEXT NOT NULL,matched INTEGER NOT NULL DEFAULT 0,missing INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',updated TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS versions(file_id TEXT NOT NULL,quality TEXT NOT NULL,result TEXT NOT NULL,added TEXT NOT NULL,PRIMARY KEY(file_id,quality));
 CREATE TABLE IF NOT EXISTS record_checks(file_id TEXT PRIMARY KEY,result TEXT NOT NULL,checked TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS version_errors(file_id TEXT NOT NULL,quality TEXT NOT NULL,error TEXT NOT NULL,checked TEXT NOT NULL,PRIMARY KEY(file_id,quality));
 INSERT OR IGNORE INTO versions(file_id,quality,result,added) SELECT id,json_extract(result,'$.quality'),result,updated FROM files WHERE status='done' AND json_extract(result,'$.quality') IN ('original','preview','thumbnail');`)
	if e != nil {
		db.Close()
		return nil, e
	}
	fileColumns, e := db.Query("PRAGMA table_info(files)")
	if e != nil {
		return nil, e
	}
	hasConfirmed := false
	for fileColumns.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if e = fileColumns.Scan(&cid, &name, &typ, &notnull, &def, &pk); e != nil {
			break
		}
		if name == "confirmed" {
			hasConfirmed = true
		}
	}
	if e == nil {
		e = fileColumns.Err()
	}
	fileColumns.Close()
	if e != nil {
		return nil, e
	}
	if !hasConfirmed {
		tx, te := db.Begin()
		if te != nil {
			return nil, te
		}
		if _, te = tx.Exec("ALTER TABLE files ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0"); te == nil {
			_, te = tx.Exec("UPDATE files SET confirmed=1 WHERE COALESCE(" + fileMetadataSQL + ",0) OR EXISTS(SELECT 1 FROM versions WHERE file_id=files.id)")
		}
		if te != nil {
			tx.Rollback()
			return nil, te
		}
		if te = tx.Commit(); te != nil {
			return nil, te
		}
	}
	if _, e = db.Exec("CREATE INDEX IF NOT EXISTS files_confirmed ON files(confirmed)"); e != nil {
		return nil, e
	}
	rows, e := db.Query("PRAGMA table_info(versions)")
	if e != nil {
		db.Close()
		return nil, e
	}
	hasBytes := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if e := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); e != nil {
			rows.Close()
			db.Close()
			return nil, e
		}
		if name == "bytes" {
			hasBytes = true
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		db.Close()
		return nil, e
	}
	if !hasBytes {
		if _, e := db.Exec("ALTER TABLE versions ADD COLUMN bytes INTEGER NOT NULL DEFAULT 0"); e != nil {
			db.Close()
			return nil, e
		}
	}
	if _, e := db.Exec("UPDATE versions SET bytes=COALESCE(json_extract(result,'$.bytes'),0) WHERE bytes=0"); e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db, root}, nil
}
func (s *Store) set(k, v string) error {
	_, e := s.DB.Exec("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", k, v)
	return e
}
func (s *Store) get(k string) string {
	var v string
	s.DB.QueryRow("SELECT value FROM settings WHERE key=?", k).Scan(&v)
	return v
}

// Promoting an unresolved reference is a metadata event, never a downloaded
// file. Old reference-probe failures must not become confirmed-file failures.
const promotionSQL = "files.confirmed=0 AND excluded.confirmed=1 AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=files.id)"

var upsertFile = `INSERT INTO files(id,cluster,name,raw,updated,confirmed) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET cluster=excluded.cluster,name=excluded.name,status=CASE WHEN ` + promotionSQL + ` THEN 'pending' ELSE files.status END,error=CASE WHEN ` + promotionSQL + ` THEN '' ELSE files.error END,confirmed=MAX(files.confirmed,excluded.confirmed),raw=excluded.raw,updated=excluded.updated`

func fileArgs(r Resource) []any {
	raw := r.Raw
	if len(raw) == 0 {
		raw = Object{"path": r.ID, "name": r.Name}
	}
	b, _ := json.Marshal(raw)
	confirmed := str(raw["type"]) == "file" || r.Size > 0 || len(r.Candidates) > 0
	if !confirmed {
		confirmed = num(raw["size"]) > 0 || num(obj(raw["meta"])["size"]) > 0 || len(previewCandidates(raw)) > 0
	}
	return []any{r.ID, r.Cluster, r.Name, string(b), time.Now().UTC().Format(time.RFC3339Nano), confirmed}
}
func (s *Store) save(r Resource) error { _, e := s.DB.Exec(upsertFile, fileArgs(r)...); return e }
func (s *Store) saveMany(resources []Resource) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	st, e := tx.Prepare(upsertFile)
	if e != nil {
		return e
	}
	defer st.Close()
	for _, r := range resources {
		if _, e := st.Exec(fileArgs(r)...); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) load(id string) (Resource, error) {
	var raw, cl string
	e := s.DB.QueryRow("SELECT raw,cluster FROM files WHERE id=?", id).Scan(&raw, &cl)
	if e != nil {
		return Resource{}, e
	}
	m, e := decodeJSON([]byte(raw))
	if e != nil {
		return Resource{}, e
	}
	r, e := resource(m, cl)
	r.ID = id
	return r, e
}
func (s *Store) ids(where string) ([]string, error) {
	rows, e := s.DB.Query("SELECT id FROM files WHERE " + where + " ORDER BY id")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if e := rows.Scan(&id); e != nil {
			return nil, e
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// The same flag orders the queue and controls the barrier between media phases.
// Metadata can be absent for missing resources, so also inspect the filename.
const downloadVideoSQL = `(lower(COALESCE(NULLIF(json_extract(raw,'$.mimetype'),''),NULLIF(json_extract(raw,'$.mime_type'),''),NULLIF(json_extract(raw,'$.meta.mimetype'),''),json_extract(raw,'$.meta.mime_type'),'')) LIKE 'video/%'
 OR lower(COALESCE(json_extract(raw,'$.media_type'),json_extract(raw,'$.meta.media_type'),''))='video'
 OR lower(name) GLOB '*.mp4' OR lower(name) GLOB '*.mov' OR lower(name) GLOB '*.mkv'
 OR lower(name) GLOB '*.avi' OR lower(name) GLOB '*.webm' OR lower(name) GLOB '*.mts'
 OR lower(name) GLOB '*.m2ts' OR lower(name) GLOB '*.3gp' OR lower(name) GLOB '*.3g2'
 OR lower(name) GLOB '*.m4v' OR lower(name) GLOB '*.mpeg' OR lower(name) GLOB '*.mpg'
 OR lower(name) GLOB '*.wmv' OR lower(name) GLOB '*.flv' OR lower(name) GLOB '*.vob'
 OR lower(name) GLOB '*.ogv' OR lower(name) GLOB '*.ts' OR lower(name) GLOB '*.hevc')`

type downloadItem struct {
	ID    string
	Video bool
}

func (s *Store) downloadQueue(where string) ([]downloadItem, error) {
	rows, e := s.DB.Query(`SELECT id,CASE WHEN ` + downloadVideoSQL + ` THEN 1 ELSE 0 END FROM files WHERE ` + confirmedSQL("files") + ` AND (` + where + `) ORDER BY 2,EXISTS(SELECT 1 FROM versions WHERE file_id=files.id),CASE WHEN EXISTS (
	 SELECT 1 FROM json_each(files.raw,'$.meta.sizes') p
	 WHERE json_extract(p.value,'$.name')='ORIGINAL' AND instr(json_extract(p.value,'$.url'),'/disk/')>0
	) THEN 0 ELSE 1 END,id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var items []downloadItem
	for rows.Next() {
		var item downloadItem
		if e := rows.Scan(&item.ID, &item.Video); e != nil {
			return nil, e
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s *Store) versions(id string) (map[string]Result, error) {
	rows, e := s.DB.Query("SELECT quality,result FROM versions WHERE file_id=?", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]Result{}
	for rows.Next() {
		var q, b string
		if e := rows.Scan(&q, &b); e != nil {
			return nil, e
		}
		var r Result
		if e := json.Unmarshal([]byte(b), &r); e != nil {
			return nil, e
		}
		out[q] = r
	}
	return out, rows.Err()
}

const bestVersion = `SELECT result FROM versions WHERE file_id=? ORDER BY CASE quality WHEN 'original' THEN 0 WHEN 'preview' THEN 1 ELSE 2 END LIMIT 1`

func (s *Store) done(r Result) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	b, _ := json.Marshal(r)
	if _, e := tx.Exec(`INSERT INTO versions(file_id,quality,result,added,bytes) VALUES(?,?,?,?,?) ON CONFLICT(file_id,quality) DO UPDATE SET result=excluded.result,added=excluded.added,bytes=excluded.bytes`, r.ID, r.Quality, string(b), time.Now().UTC().Format(time.RFC3339Nano), r.Bytes); e != nil {
		return e
	}
	if _, e := tx.Exec("DELETE FROM version_errors WHERE file_id=? AND quality=?", r.ID, r.Quality); e != nil {
		return e
	}
	var best string
	if e := tx.QueryRow(bestVersion, r.ID).Scan(&best); e != nil {
		return e
	}
	if _, e := tx.Exec("UPDATE files SET confirmed=1,status='done',result=?,error='' WHERE id=?", best, r.ID); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) attempt(id, q string, err error) error {
	_, e := s.DB.Exec(`INSERT INTO version_errors(file_id,quality,error,checked) VALUES(?,?,?,?) ON CONFLICT(file_id,quality) DO UPDATE SET error=excluded.error,checked=excluded.checked`, id, q, err.Error(), time.Now().UTC().Format(time.RFC3339Nano))
	return e
}
func (s *Store) failed(id string, err error) error {
	_, e := s.DB.Exec(`UPDATE files SET status=CASE WHEN EXISTS(SELECT 1 FROM versions WHERE file_id=files.id) THEN 'done' ELSE 'failed' END,error=? WHERE id=?`, err.Error(), id)
	return e
}

// Repair historical file failures caused by the old rate-limit retry budget.
// Keep independently known preview/thumbnail errors and every saved version.
func (s *Store) requeueRateLimits() (int, error) {
	tx, e := s.DB.Begin()
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	rows, e := tx.Query("SELECT DISTINCT file_id FROM version_errors WHERE error LIKE '%HTTP 429%'")
	if e != nil {
		return 0, e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e := rows.Scan(&id); e != nil {
			rows.Close()
			return 0, e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}
	for _, id := range ids {
		if _, e := tx.Exec("UPDATE files SET status=CASE WHEN EXISTS(SELECT 1 FROM versions WHERE file_id=files.id) THEN 'done' ELSE 'pending' END,error='' WHERE id=?", id); e != nil {
			return 0, e
		}
	}
	if _, e := tx.Exec("DELETE FROM version_errors WHERE error LIKE '%HTTP 429%'"); e != nil {
		return 0, e
	}
	return len(ids), tx.Commit()
}
func (s *Store) reset(id, reason string) error {
	_, e := s.DB.Exec("DELETE FROM versions WHERE file_id=?", id)
	if e != nil {
		return e
	}
	_, e = s.DB.Exec("UPDATE files SET status='pending',result='{}',error=? WHERE id=?", reason, id)
	return e
}
func (s *Store) removeVersion(id, q, reason string) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e := tx.Exec("DELETE FROM versions WHERE file_id=? AND quality=?", id, q); e != nil {
		return e
	}
	var best string
	e = tx.QueryRow(bestVersion, id).Scan(&best)
	if e == sql.ErrNoRows {
		_, e = tx.Exec("UPDATE files SET status='pending',result='{}',error=? WHERE id=?", reason, id)
	} else if e == nil {
		_, e = tx.Exec("UPDATE files SET confirmed=1,status='done',result=?,error=? WHERE id=?", best, reason, id)
	}
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) result(id string) (Result, error) {
	var b string
	e := s.DB.QueryRow(bestVersion, id).Scan(&b)
	var r Result
	if e == nil {
		e = json.Unmarshal([]byte(b), &r)
	}
	return r, e
}

type Stats struct {
	Records         int    `json:"records"`
	Confirmed       int    `json:"confirmed"`
	ConfirmedNoCopy int    `json:"confirmed_no_copy"`
	Unconfirmed     int    `json:"unconfirmed"`
	ArchiveFiles    int    `json:"archive_files"`
	ZIPArchives     int    `json:"zip_archives"`
	Total           int    `json:"total"`
	Done            int    `json:"done"`
	Failed          int    `json:"failed"`
	Pending         int    `json:"pending"`
	Original        int    `json:"original"`
	Preview         int    `json:"preview"`
	Thumbnail       int    `json:"thumbnail"`
	NoOriginal      int    `json:"no_original"`
	NoPreview       int    `json:"no_preview"`
	NoThumbnail     int    `json:"no_thumbnail"`
	OnlyPreview     int    `json:"only_preview"`
	OnlyThumbnail   int    `json:"only_thumbnail"`
	Archives        int    `json:"archives"`
	Bytes           int64  `json:"bytes"`
	Inventory       string `json:"inventory"`
}

const versionFlags = `SELECT file_id,MAX(quality='original') o,MAX(quality='preview') p,MAX(quality='thumbnail') t,COUNT(*) n,SUM(bytes) bytes FROM versions GROUP BY file_id`

func (s *Store) stats() (Stats, error) {
	st := Stats{Inventory: s.get("inventory")}
	e := s.DB.QueryRow(`SELECT COUNT(*),COALESCE(SUM(v.n>0),0),COALESCE(SUM(f.status='failed' AND v.n IS NULL AND `+confirmedSQL("f")+`),0),COALESCE(SUM(f.status='pending' AND v.n IS NULL AND `+confirmedSQL("f")+`),0),COALESCE(SUM(v.o),0),COALESCE(SUM(v.p),0),COALESCE(SUM(v.t),0),COALESCE(SUM(v.o=0 AND v.p=1),0),COALESCE(SUM(v.o=0 AND v.p=0 AND v.t=1),0),COALESCE(SUM(v.n),0),COALESCE(SUM(v.bytes),0),COALESCE(SUM(`+confirmedSQL("f")+`),0) FROM files f LEFT JOIN (`+versionFlags+`) v ON v.file_id=f.id`).Scan(&st.Total, &st.Done, &st.Failed, &st.Pending, &st.Original, &st.Preview, &st.Thumbnail, &st.OnlyPreview, &st.OnlyThumbnail, &st.Archives, &st.Bytes, &st.Confirmed)
	if e != nil {
		return st, e
	}
	st.Records = st.Total // total remains a legacy alias for all catalog records.
	st.Unconfirmed = st.Records - st.Confirmed
	st.ConfirmedNoCopy = st.Confirmed - st.Done
	st.NoOriginal = st.Confirmed - st.Original
	st.NoPreview = st.Confirmed - st.Preview
	st.NoThumbnail = st.Confirmed - st.Thumbnail
	if e == nil {
		e = s.DB.QueryRow("SELECT COUNT(DISTINCT json_extract(result,'$.archive')),COUNT(DISTINCT CASE WHEN json_extract(result,'$.bundle')=1 THEN json_extract(result,'$.archive') END) FROM versions").Scan(&st.ArchiveFiles, &st.ZIPArchives)
	}
	return st, e
}
func (s *Store) checkAccount(uid string) error {
	old := s.get("account")
	if old != "" && old != uid {
		return fmt.Errorf("папка принадлежит другому аккаунту Яндекса")
	}
	return s.set("account", uid)
}
func filterSQL(filter string) (string, error) {
	switch filter {
	case "", "all", "confirmed":
		return confirmedSQL("f"), nil
	case "records-all":
		return "1=1", nil
	case "unconfirmed":
		return "NOT " + confirmedSQL("f"), nil
	case "saved":
		return "EXISTS(SELECT 1 FROM versions WHERE file_id=f.id)", nil
	case "missing-original":
		return confirmedSQL("f") + " AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='original')", nil
	case "missing-preview":
		return confirmedSQL("f") + " AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='preview')", nil
	case "missing-thumbnail":
		return confirmedSQL("f") + " AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='thumbnail')", nil
	case "no-copy":
		return confirmedSQL("f") + " AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id)", nil
	case "failed":
		return confirmedSQL("f") + " AND f.status='failed' AND v.n IS NULL", nil
	case "pending":
		return confirmedSQL("f") + " AND f.status='pending' AND v.n IS NULL", nil
	case "preview-best":
		return "v.o=0 AND v.p=1", nil
	case "original-best":
		return "v.o=1", nil
	case "thumbnail-best":
		return "v.o=0 AND v.p=0 AND v.t=1", nil
	default:
		return "", fmt.Errorf("неизвестный фильтр: %s", filter)
	}
}

type FileRow struct {
	Confirmed   bool   `json:"confirmed"`
	RecordState string `json:"record_state"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Original    bool   `json:"original"`
	Preview     bool   `json:"preview"`
	Thumbnail   bool   `json:"thumbnail"`
	Error       string `json:"error,omitempty"`
	Created     string `json:"created,omitempty"`
}

func (s *Store) list(filter string, offset, limit int) ([]FileRow, int, error) {
	where, e := filterSQL(filter)
	if e != nil {
		return nil, 0, e
	}
	if limit < 1 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	from := ` FROM files f LEFT JOIN (` + versionFlags + `) v ON v.file_id=f.id WHERE ` + where
	var total int
	if e := s.DB.QueryRow("SELECT COUNT(*)" + from).Scan(&total); e != nil {
		return nil, 0, e
	}
	rows, e := s.DB.Query(`SELECT f.id,f.name,COALESCE(v.o,0),COALESCE(v.p,0),COALESCE(v.t,0),f.error,f.raw,f.result,`+confirmedSQL("f")+from+` ORDER BY f.id LIMIT ? OFFSET ?`, limit, offset)
	if e != nil {
		return nil, 0, e
	}
	defer rows.Close()
	out := []FileRow{}
	for rows.Next() {
		var row FileRow
		var raw, best string
		if e := rows.Scan(&row.ID, &row.Name, &row.Original, &row.Preview, &row.Thumbnail, &row.Error, &raw, &best, &row.Confirmed); e != nil {
			return nil, 0, e
		}
		row.RecordState = "unconfirmed"
		if row.Confirmed {
			row.RecordState = "confirmed_no_copy"
			if row.Original || row.Preview || row.Thumbnail {
				row.RecordState = "saved"
			}
		} else {
			row.Error = "Photoslice содержит ссылку, но подробные данные файла не получены; отдельная проверка метаданных."
		}
		m, _ := decodeJSON([]byte(raw))
		var result Result
		json.Unmarshal([]byte(best), &result)
		meta := metadataFrom(m, result.Metadata.EmbeddedEXIF)
		row.Created = meta.Created
		out = append(out, row)
	}
	return out, total, rows.Err()
}
