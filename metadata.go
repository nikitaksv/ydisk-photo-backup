package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type DateEvidence struct {
	Source        string `json:"source"`
	Raw           string `json:"raw"`
	ISO           string `json:"iso,omitempty"`
	TimezoneKnown bool   `json:"timezone_known"`
}
type Metadata struct {
	Created        string         `json:"created,omitempty"`
	CreationSource string         `json:"creation_source,omitempty"`
	TimezoneKnown  bool           `json:"timezone_known"`
	Dates          []DateEvidence `json:"dates"`
	EmbeddedEXIF   Object         `json:"embedded_exif,omitempty"`
	Note           string         `json:"note"`
}

func normalizeDate(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if t, e := time.Parse(time.RFC3339Nano, raw); e == nil {
		return t.Format(time.RFC3339Nano), true
	}
	if n, e := strconv.ParseInt(raw, 10, 64); e == nil && len(strings.TrimPrefix(raw, "-")) >= 10 {
		var t time.Time
		switch l := len(strings.TrimPrefix(raw, "-")); {
		case l >= 19:
			t = time.Unix(0, n)
		case l >= 16:
			t = time.UnixMicro(n)
		case l >= 13:
			t = time.UnixMilli(n)
		default:
			t = time.Unix(n, 0)
		}
		return t.UTC().Format(time.RFC3339Nano), true
	}
	for _, layout := range []string{"2006:01:02 15:04:05.999999999-07:00", "2006:01:02 15:04:05-07:00"} {
		if t, e := time.Parse(layout, raw); e == nil {
			return t.Format(time.RFC3339Nano), true
		}
	}
	for _, layout := range []string{"2006:01:02 15:04:05.999999999", "2006:01:02 15:04:05", "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999", "2006-01-02"} {
		if t, e := time.Parse(layout, raw); e == nil {
			if layout == "2006-01-02" {
				return t.Format(layout), false
			}
			return t.Format("2006-01-02T15:04:05.999999999"), false
		}
	}
	return "", false
}
func metadataFrom(raw Object, exif Object) Metadata {
	m := Metadata{Dates: []DateEvidence{}, EmbeddedEXIF: exif, Note: "Исходные значения дат сохранены. Часовой пояс не подставляется, если источник его не указал. Метаданные API сохранены отдельно; встроенные метаданные оригинала остаются внутри оригинального файла."}
	var walk func(any, string)
	walk = func(v any, p string) {
		switch x := v.(type) {
		case map[string]any:
			get := func(names ...string) string {
				for k, v := range x {
					for _, name := range names {
						if strings.EqualFold(k, name) {
							return str(v)
						}
					}
				}
				return ""
			}
			date := get("DateTimeOriginal", "date_time_original", "datetime_original")
			if date != "" {
				combined := date
				if sub := get("SubSecTimeOriginal", "subsec_time_original"); sub != "" && !strings.Contains(date, ".") {
					combined += "." + sub
				}
				_, hasZone := normalizeDate(date)
				if !hasZone {
					combined += get("OffsetTimeOriginal", "offset_time_original")
				}
				if combined != date {
					iso, tz := normalizeDate(combined)
					m.Dates = append(m.Dates, DateEvidence{p + ".DateTimeOriginal_complete", combined, iso, tz})
				}
			}
			for k, v := range x {
				walk(v, p+"."+k)
			}
		case []any:
			for i, v := range x {
				walk(v, fmt.Sprintf("%s[%d]", p, i))
			}
		default:
			key := strings.ToLower(p)
			if !(strings.Contains(key, "created") || strings.Contains(key, "modified") || strings.Contains(key, "datetime") || strings.Contains(key, "date_time") || strings.Contains(key, "photoslice_time") || strings.Contains(key, "photoslicetime") || strings.Contains(key, "photo_time") || strings.HasSuffix(key, ".ctime") || strings.HasSuffix(key, ".mtime") || strings.HasSuffix(key, ".timestamp")) {
				return
			}
			value := str(v)
			if value == "" {
				return
			}
			iso, tz := normalizeDate(value)
			m.Dates = append(m.Dates, DateEvidence{p, value, iso, tz})
		}
	}
	walk(raw, "api")
	walk(exif, "embedded")
	priority := func(d DateEvidence) int {
		s := strings.ToLower(d.Source)
		switch {
		case strings.Contains(s, "embedded.datetimeoriginal"):
			return -2
		case strings.Contains(s, "datetimeoriginal_complete"):
			return -1
		case strings.Contains(s, "datetimeoriginal") || strings.Contains(s, "date_time_original") || strings.Contains(s, "datetime_original"):
			return 1
		case strings.Contains(s, "photoslice_time") || strings.Contains(s, "photoslicetime") || strings.Contains(s, "photo_time"):
			return 2
		case strings.HasSuffix(s, ".created") || strings.HasSuffix(s, ".created_at") || strings.HasSuffix(s, ".ctime"):
			return 3
		case strings.Contains(s, "datetimedigitized") || strings.Contains(s, "date_time_digitized"):
			return 3
		default:
			return 4
		}
	}
	sort.SliceStable(m.Dates, func(i, j int) bool {
		a, b := priority(m.Dates[i]), priority(m.Dates[j])
		if a != b {
			return a < b
		}
		return m.Dates[i].Source < m.Dates[j].Source
	})
	for _, d := range m.Dates {
		if d.ISO != "" && priority(d) < 4 && !strings.Contains(strings.ToLower(d.Source), "modified") {
			m.Created = d.ISO
			m.CreationSource = d.Source
			m.TimezoneKnown = d.TimezoneKnown
			break
		}
	}
	return m
}
func sanitizedAPI(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := Object{}
		for k, v := range x {
			low := strings.ToLower(k)
			if low == "sk" || strings.Contains(low, "cookie") || strings.Contains(low, "token") || low == "connection_id" {
				continue
			}
			m[k] = sanitizedAPI(v)
		}
		return m
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = sanitizedAPI(v)
		}
		return out
	case string:
		if strings.Contains(x, "https://") || strings.HasPrefix(x, "//") {
			return "[URL omitted]"
		}
		return x
	default:
		return v
	}
}
func readEXIF(file string) (Object, []byte) {
	f, e := os.Open(file)
	if e != nil {
		return nil, nil
	}
	defer f.Close()
	peek := make([]byte, 8)
	n, _ := f.Read(peek)
	if n < 2 || !(bytes.HasPrefix(peek[:n], []byte{0xff, 0xd8}) || bytes.HasPrefix(peek[:n], []byte("\x89PNG\r\n\x1a\n"))) {
		return nil, nil
	}
	f.Seek(0, 0)
	b, _ := io.ReadAll(io.LimitReader(f, 2<<20))
	var tiff []byte
	if bytes.HasPrefix(b, []byte{0xff, 0xd8}) {
		for p := 2; p+4 <= len(b); {
			if b[p] != 0xff {
				break
			}
			marker := b[p+1]
			if marker == 0xda || marker == 0xd9 {
				break
			}
			if marker == 0xff {
				p++
				continue
			}
			n := int(binary.BigEndian.Uint16(b[p+2 : p+4]))
			if n < 2 || p+2+n > len(b) {
				break
			}
			segment := b[p+4 : p+2+n]
			if marker == 0xe1 && bytes.HasPrefix(segment, []byte("Exif\x00\x00")) {
				tiff = segment[6:]
				break
			}
			p += 2 + n
		}
	}
	if bytes.HasPrefix(b, []byte("\x89PNG\r\n\x1a\n")) {
		for p := 8; p+12 <= len(b); {
			n := int(binary.BigEndian.Uint32(b[p : p+4]))
			if n < 0 || p+12+n > len(b) {
				break
			}
			if string(b[p+4:p+8]) == "eXIf" {
				tiff = b[p+8 : p+8+n]
				break
			}
			p += 12 + n
		}
	}
	if len(tiff) < 8 {
		return nil, nil
	}
	var order binary.ByteOrder
	if string(tiff[:2]) == "II" {
		order = binary.LittleEndian
	} else if string(tiff[:2]) == "MM" {
		order = binary.BigEndian
	} else {
		return nil, nil
	}
	if order.Uint16(tiff[2:4]) != 42 {
		return nil, nil
	}
	out := Object{}
	var visit func(uint32, int)
	seen := map[uint32]bool{}
	tags := map[uint16]string{0x010f: "Make", 0x0110: "Model", 0x0131: "Software", 0x0132: "DateTime", 0x9003: "DateTimeOriginal", 0x9004: "DateTimeDigitized", 0x9010: "OffsetTime", 0x9011: "OffsetTimeOriginal", 0x9012: "OffsetTimeDigitized", 0x9290: "SubSecTime", 0x9291: "SubSecTimeOriginal", 0x9292: "SubSecTimeDigitized"}
	visit = func(offset uint32, depth int) {
		p := int(offset)
		if depth > 3 || p < 0 || p+2 > len(tiff) || seen[offset] {
			return
		}
		seen[offset] = true
		n := int(order.Uint16(tiff[p : p+2]))
		if n > 1024 {
			return
		}
		for i := 0; i < n; i++ {
			q := p + 2 + i*12
			if q+12 > len(tiff) {
				return
			}
			tag := order.Uint16(tiff[q : q+2])
			typ := order.Uint16(tiff[q+2 : q+4])
			count := int(order.Uint32(tiff[q+4 : q+8]))
			if tag == 0x8769 {
				visit(order.Uint32(tiff[q+8:q+12]), depth+1)
			}
			if name := tags[tag]; name != "" && typ == 2 && count > 0 && count <= 65536 {
				start := q + 8
				if count > 4 {
					start = int(order.Uint32(tiff[q+8 : q+12]))
				}
				if start >= 0 && start+count <= len(tiff) {
					out[name] = strings.TrimRight(string(tiff[start:start+count]), "\x00")
				}
			}
		}
	}
	visit(order.Uint32(tiff[4:8]), 0)
	for _, pair := range []struct{ date, sub, offset string }{{"DateTimeOriginal", "SubSecTimeOriginal", "OffsetTimeOriginal"}, {"DateTimeDigitized", "SubSecTimeDigitized", "OffsetTimeDigitized"}, {"DateTime", "SubSecTime", "OffsetTime"}} {
		if date := str(out[pair.date]); date != "" {
			combined := date
			if sub := str(out[pair.sub]); sub != "" {
				combined += "." + sub
			}
			if offset := str(out[pair.offset]); offset != "" {
				combined += offset
			}
			out[pair.date+"Complete"] = combined
		}
	}
	// Prefer the combined exact EXIF values, including subsecond and offset.
	for _, key := range []string{"DateTimeOriginal", "DateTimeDigitized", "DateTime"} {
		if value := out[key+"Complete"]; value != nil {
			out[key+"Raw"] = out[key]
			out[key] = value
		}
	}
	return out, append([]byte(nil), tiff...)
}
