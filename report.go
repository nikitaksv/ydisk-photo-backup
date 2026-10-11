package main

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
)

func atomicFile(dst string, write func(*os.File) error) error {
	f, e := os.CreateTemp(filepath.Dir(dst), "report-*.part")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	e = write(f)
	se := f.Sync()
	ce := f.Close()
	if e != nil {
		return e
	}
	if se != nil {
		return se
	}
	if ce != nil {
		return ce
	}
	if e := os.Remove(dst); e != nil && !os.IsNotExist(e) {
		return e
	}
	return os.Rename(tmp, dst)
}

const versionsPivot = `SELECT file_id,MAX(CASE WHEN quality='original' THEN result ELSE '{}' END) original,MAX(CASE WHEN quality='preview' THEN result ELSE '{}' END) preview,MAX(CASE WHEN quality='thumbnail' THEN result ELSE '{}' END) thumbnail FROM versions GROUP BY file_id`
const errorsPivot = `SELECT file_id,MAX(CASE WHEN quality='original' THEN error ELSE '' END) original_error,MAX(CASE WHEN quality='preview' THEN error ELSE '' END) preview_error,MAX(CASE WHEN quality='thumbnail' THEN error ELSE '' END) thumbnail_error FROM version_errors GROUP BY file_id`

func (s *Store) report() error {
	st, e := s.stats()
	if e != nil {
		return e
	}
	if e := atomicFile(filepath.Join(s.Root, "summary.json"), func(f *os.File) error { enc := json.NewEncoder(f); enc.SetIndent("", "  "); return enc.Encode(st) }); e != nil {
		return e
	}
	if e := atomicFile(filepath.Join(s.Root, "metadata.jsonl"), func(f *os.File) error {
		rows, e := s.DB.Query("SELECT id,name,raw,result FROM files ORDER BY id")
		if e != nil {
			return e
		}
		defer rows.Close()
		enc := json.NewEncoder(f)
		for rows.Next() {
			var id, name, raw, b string
			if e := rows.Scan(&id, &name, &raw, &b); e != nil {
				return e
			}
			m, _ := decodeJSON([]byte(raw))
			var r Result
			json.Unmarshal([]byte(b), &r)
			meta := metadataFrom(m, r.Metadata.EmbeddedEXIF)
			if e := enc.Encode(Object{"id": id, "name": name, "metadata": meta, "api": sanitizedAPI(m)}); e != nil {
				return e
			}
		}
		return rows.Err()
	}); e != nil {
		return e
	}
	return atomicFile(filepath.Join(s.Root, "report.csv"), func(f *os.File) error {
		f.Write([]byte{0xef, 0xbb, 0xbf})
		w := csv.NewWriter(f)
		w.Write([]string{"id", "name", "status", "best_quality", "original_archive", "preview_archive", "thumbnail_archive", "original_bytes", "preview_bytes", "thumbnail_bytes", "created", "creation_source", "timezone_known", "original_error", "preview_error", "thumbnail_error", "file_error"})
		rows, e := s.DB.Query(`SELECT f.id,f.name,f.status,f.raw,f.result,f.error,COALESCE(v.original,'{}'),COALESCE(v.preview,'{}'),COALESCE(v.thumbnail,'{}'),COALESCE(e.original_error,''),COALESCE(e.preview_error,''),COALESCE(e.thumbnail_error,'') FROM files f LEFT JOIN (` + versionsPivot + `) v ON v.file_id=f.id LEFT JOIN (` + errorsPivot + `) e ON e.file_id=f.id ORDER BY f.id`)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			var id, name, status, raw, b, msg, o, p, t, oe, pe, te string
			if e := rows.Scan(&id, &name, &status, &raw, &b, &msg, &o, &p, &t, &oe, &pe, &te); e != nil {
				return e
			}
			var best, original, preview, thumbnail Result
			json.Unmarshal([]byte(b), &best)
			json.Unmarshal([]byte(o), &original)
			json.Unmarshal([]byte(p), &preview)
			json.Unmarshal([]byte(t), &thumbnail)
			m, _ := decodeJSON([]byte(raw))
			meta := metadataFrom(m, best.Metadata.EmbeddedEXIF)
			w.Write([]string{id, name, status, best.Quality, original.Archive, preview.Archive, thumbnail.Archive, strconv.FormatInt(original.Bytes, 10), strconv.FormatInt(preview.Bytes, 10), strconv.FormatInt(thumbnail.Bytes, 10), meta.Created, meta.CreationSource, strconv.FormatBool(meta.TimezoneKnown), oe, pe, te, msg})
		}
		w.Flush()
		if e := w.Error(); e != nil {
			return e
		}
		return rows.Err()
	})
}
