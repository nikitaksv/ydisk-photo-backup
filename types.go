package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type Object = map[string]any
type Candidate struct {
	URL     string `json:"url"`
	Quality string `json:"quality"`
	Width   int    `json:"width"`
	Height  int    `json:"height"`
	Rank    int    `json:"rank"`
}
type Resource struct {
	OriginalError error            `json:"-"`
	FailedURLs    map[string]error `json:"-"`
	OriginalZIP   string           `json:"-"`
	ID            string           `json:"id"`
	Cluster       string           `json:"cluster"`
	Name          string           `json:"name"`
	Size          int64            `json:"size"`
	MD5           string           `json:"md5,omitempty"`
	MIME          string           `json:"mime,omitempty"`
	Raw           Object           `json:"-"`
	Candidates    []Candidate      `json:"-"`
}
type Result struct {
	Bundle       bool     `json:"bundle,omitempty"`
	Manifest     string   `json:"manifest,omitempty"`
	SourceHash   string   `json:"source_hash,omitempty"`
	Metadata     Metadata `json:"metadata"`
	APIMetadata  any      `json:"-"`
	EXIFBinary   []byte   `json:"-"`
	ArchiveSize  int64    `json:"archive_size,omitempty"`
	ArchiveMtime int64    `json:"archive_mtime,omitempty"`
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Quality      string   `json:"quality"`
	Archive      string   `json:"archive"`
	Entry        string   `json:"entry"`
	Bytes        int64    `json:"bytes"`
	SHA256       string   `json:"sha256"`
	MD5          string   `json:"md5"`
	Width        int      `json:"width,omitempty"`
	Height       int      `json:"height,omitempty"`
	Warning      string   `json:"warning,omitempty"`
}

func obj(v any) Object {
	m, _ := v.(map[string]any)
	if m == nil {
		return Object{}
	}
	return m
}
func arr(v any) []any { a, _ := v.([]any); return a }
func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return string(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return ""
}
func num(v any) int64 { n, _ := strconv.ParseInt(str(v), 10, 64); return n }
func first(m Object, keys ...string) string {
	for _, k := range keys {
		if s := str(m[k]); s != "" {
			return s
		}
	}
	return ""
}
func resource(v any, cluster string) (Resource, error) {
	m := obj(v)
	id := first(m, "path", "id", "resource_id")
	if id == "" {
		id = str(v)
	}
	if id == "" {
		return Resource{}, fmt.Errorf("ресурс без идентификатора")
	}
	name := first(m, "name")
	if name == "" {
		parts := strings.Split(strings.ReplaceAll(id, "\\", "/"), "/")
		name = parts[len(parts)-1]
	}
	size := num(m["size"])
	meta := obj(m["meta"])
	if size == 0 {
		size = num(meta["size"])
	}
	md5 := first(m, "md5")
	if md5 == "" {
		md5 = first(meta, "md5")
	}
	r := Resource{ID: id, Cluster: cluster, Name: name, Size: size, MD5: strings.ToLower(md5), MIME: first(m, "mimetype", "mime_type"), Raw: m}
	if r.MIME == "" {
		r.MIME = first(meta, "mimetype", "mime_type")
	}
	r.Candidates = previewCandidates(m)
	return r, nil
}
