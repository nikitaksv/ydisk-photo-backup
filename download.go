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
	_ "golang.org/x/image/webp"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var md5RE = regexp.MustCompile(`^[a-fA-F0-9]{32}$`)

type Digest struct {
	Bytes  int64
	SHA256 string
	MD5    string
}

var copyBuffers = sync.Pool{New: func() any { b := make([]byte, 512<<10); return &b }}

func copyStream(dst io.Writer, src io.Reader) (int64, error) {
	b := copyBuffers.Get().(*[]byte)
	defer copyBuffers.Put(b)
	return io.CopyBuffer(dst, src, *b)
}

func digestFile(file string) (int64, string, string, error) {
	f, e := os.Open(file)
	if e != nil {
		return 0, "", "", e
	}
	defer f.Close()
	a, b := sha256.New(), md5.New()
	n, e := io.Copy(io.MultiWriter(a, b), f)
	return n, hex.EncodeToString(a.Sum(nil)), hex.EncodeToString(b.Sum(nil)), e
}
func safeName(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return '_'
		}
		return r
	}, name)
	name = strings.Trim(name, " .")
	if name == "" || name == "." || name == ".." {
		name = "photo"
	}
	r := []rune(name)
	if len(r) > 160 {
		name = string(r[:140]) + filepath.Ext(name)
	}
	return name
}
func fatal(e error) bool {
	var local *LocalError
	return errors.Is(e, ErrAuth) || errors.As(e, &local) || errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded)
}
func (c *Client) original(ctx context.Context, r Resource, dir string) (string, error) {
	return c.originalDigest(ctx, r, dir, nil)
}
func (c *Client) originalDigest(ctx context.Context, r Resource, dir string, digest *Digest) (string, error) {
	if c.WebDAVToken != "" && !c.webDAVRejected.Load() {
		if file, e := c.webDAVOriginal(ctx, r, dir, digest); e == nil {
			return file, nil
		} else if fatal(e) {
			return "", e
		}
	}
	if candidates := directOriginalCandidates(r); len(candidates) > 0 {
		var last error
		for _, candidate := range candidates {
			if c.BatchOriginals && expiredOriginalLink(candidate.URL, time.Now()) {
				last = errors.New("прямая ссылка оригинала устарела; требуется пакетный ZIP")
				if r.FailedURLs != nil {
					r.FailedURLs[candidate.URL] = last
				}
				continue
			}
			if previous := r.FailedURLs[candidate.URL]; previous != nil {
				last = previous
				continue
			}
			target := filepath.Join(dir, "media.part")
			d, e := c.mediaHashed(ctx, candidate.URL, target)
			if e == nil {
				_, _, e = validateDigest(r, target, "original", d)
			}
			if e == nil {
				if digest != nil {
					*digest = d
				}
				return target, nil
			}
			if fatal(e) {
				return "", e
			}
			if r.FailedURLs != nil {
				r.FailedURLs[candidate.URL] = e
			}
			last = e
		}
		return "", last
	}
	if r.OriginalError != nil {
		return "", r.OriginalError
	}
	if c.BatchOriginals && r.OriginalZIP == "" {
		return "", errors.New("нет доступной прямой ссылки оригинала; требуется пакетный ZIP")
	}
	zipPath := r.OriginalZIP
	if zipPath == "" {
		d, e := c.call(ctx, "mpfs/bulk-download-prepare", Object{"items": []string{r.ID}})
		if e != nil {
			return "", e
		}
		uri := first(d, "download_url")
		if uri == "" {
			return "", errors.New("оригинал: нет download_url")
		}
		zipPath = filepath.Join(dir, "original.zip.part")
		if e := c.media(ctx, uri, zipPath); e != nil {
			return "", e
		}
	}
	zr, e := zip.OpenReader(zipPath)
	if e != nil {
		return "", errors.New("оригинал: ответ не является ZIP")
	}
	defer zr.Close()
	var entry *zip.File
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := path.Base(strings.ReplaceAll(f.Name, "\\", "/"))
		if name == r.Name || name == path.Base(strings.ReplaceAll(r.ID, "\\", "/")) {
			if entry != nil {
				return "", errors.New("ZIP содержит неоднозначные имена")
			}
			entry = f
		}
	}
	if entry == nil {
		return "", errors.New("запрошенный файл отсутствует в ZIP")
	}
	if entry.UncompressedSize64 == 0 {
		return "", errors.New("оригинал в ZIP имеет нулевой размер")
	}
	src, e := entry.Open()
	if e != nil {
		return "", e
	}
	defer src.Close()
	target := filepath.Join(dir, "media.part")
	dst, e := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return "", &LocalError{e}
	}
	sha, md := sha256.New(), md5.New()
	n, ce := copyStream(io.MultiWriter(dst, sha, md), &ctxReader{ctx: ctx, r: src})
	se := dst.Sync()
	fe := dst.Close()
	if se != nil {
		return "", &LocalError{se}
	}
	if fe != nil {
		return "", &LocalError{fe}
	}
	if ce != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		var pe *os.PathError
		if errors.As(ce, &pe) {
			return "", &LocalError{ce}
		}
		return "", errors.New("ZIP повреждён / CRC не совпадает")
	}
	if digest != nil {
		*digest = Digest{n, hex.EncodeToString(sha.Sum(nil)), hex.EncodeToString(md.Sum(nil))}
	}
	return target, nil
}

func directOriginalCandidates(r Resource) []Candidate {
	var out []Candidate
	for _, candidate := range r.Candidates {
		u, e := url.Parse(candidate.URL)
		// ORIGINAL from Photoslice sizes is an actual /disk download. An
		// ORIGINAL-sized /preview image is not proof of the original bytes.
		if candidate.Quality == "original" && e == nil && strings.HasPrefix(u.Path, "/disk/") {
			out = append(out, candidate)
		}
	}
	return out
}
func inspect(file string, preview bool) (string, int, int, error) {
	f, e := os.Open(file)
	if e != nil {
		return "", 0, 0, &LocalError{e}
	}
	defer f.Close()
	header := make([]byte, 512)
	n, _ := f.Read(header)
	if n == 0 {
		return "", 0, 0, errors.New("пустой файл")
	}
	ct := http.DetectContentType(header[:n])
	low := strings.ToLower(strings.TrimSpace(string(header[:n])))
	if strings.Contains(ct, "text/html") || strings.HasPrefix(low, "<!doctype") || strings.HasPrefix(low, "<html") || strings.HasPrefix(low, "{") {
		return "", 0, 0, errors.New("вместо медиа получена ошибка/HTML")
	}
	f.Seek(0, 0)
	cfg, format, e := image.DecodeConfig(f)
	if e != nil {
		if preview {
			return "", 0, 0, errors.New("превью не является поддерживаемым изображением JPEG/PNG/GIF/WebP")
		}
		return "", 0, 0, nil
	}
	if preview {
		if int64(cfg.Width)*int64(cfg.Height) > 100000000 {
			return "", 0, 0, errors.New("превью слишком большое для проверки")
		}
		f.Seek(0, 0)
		if _, _, e := image.Decode(f); e != nil {
			return "", 0, 0, errors.New("изображение повреждено")
		}
	}
	ext := "." + format
	if format == "jpeg" {
		ext = ".jpg"
	}
	return ext, cfg.Width, cfg.Height, nil
}
func validate(r Resource, file, quality string) (Result, string, error) {
	return validateDigest(r, file, quality, Digest{})
}
func validateDigest(r Resource, file, quality string, digest Digest) (Result, string, error) {
	n, sha, md := digest.Bytes, digest.SHA256, digest.MD5
	var e error
	if n == 0 {
		n, sha, md, e = digestFile(file)
	}
	if e != nil {
		return Result{}, "", &LocalError{e}
	}
	if n == 0 {
		return Result{}, "", errors.New("нулевой размер")
	}
	if quality == "original" {
		if r.Size > 0 && n != r.Size {
			return Result{}, "", fmt.Errorf("размер оригинала %d вместо %d", n, r.Size)
		}
		if md5RE.MatchString(r.MD5) && !strings.EqualFold(md, r.MD5) {
			return Result{}, "", errors.New("MD5 оригинала не совпадает")
		}
	}
	ext, w, h, e := inspect(file, quality != "original")
	if e != nil {
		return Result{}, "", e
	}
	if quality != "original" && w > 0 && h > 0 && w <= 320 && h <= 320 {
		quality = "thumbnail"
	}
	return Result{ID: r.ID, Name: r.Name, Quality: quality, Bytes: n, SHA256: sha, MD5: md, Width: w, Height: h}, ext, nil
}
func (c *Client) downloadOne(ctx context.Context, s *Store, r Resource) (Result, error) {
	return c.downloadSelected(ctx, s, r, "")
}
func (c *Client) downloadSelected(ctx context.Context, s *Store, r Resource, requested string) (Result, error) {
	if r.FailedURLs == nil {
		r.FailedURLs = make(map[string]error)
	}
	dir, e := os.MkdirTemp(filepath.Join(s.Root, "tmp"), "item-")
	if e != nil {
		return Result{}, &LocalError{e}
	}
	defer os.RemoveAll(dir)
	var result Result
	var file, ext string
	var reasons []string
	e = errors.New("API не вернул ссылок для версии " + requested)
	if requested == "" || requested == "original" {
		var digest Digest
		file, e = c.originalDigest(ctx, r, dir, &digest)
		if e == nil {
			result, ext, e = validateDigest(r, file, "original", digest)
		}
		if e != nil && !fatal(e) && r.OriginalZIP != "" {
			single := r
			single.OriginalZIP = ""
			file, e = c.originalDigest(ctx, single, dir, &digest)
			if e == nil {
				result, ext, e = validateDigest(r, file, "original", digest)
			}
		}
	}
	if e != nil {
		if fatal(e) {
			return Result{}, e
		}
		if requested == "original" {
			return Result{}, e
		}
		if requested == "" || requested == "original" {
			reasons = append(reasons, e.Error())
		}
		candidates := r.Candidates
		var savedPreviewSource string
		if requested == "thumbnail" {
			versions, se := s.versions(r.ID)
			if se != nil {
				return Result{}, &LocalError{se}
			}
			savedPreviewSource = versions["preview"].SourceHash
			// Some APIs return a small image under a generic preview label.
			// Prefer explicit thumbnails, then inspect those fallback links.
			candidates = nil
			for _, q := range []string{"thumbnail", "preview"} {
				for _, candidate := range r.Candidates {
					if candidate.Quality == q {
						candidates = append(candidates, candidate)
					}
				}
			}
		}
		for _, candidate := range candidates {
			if requested != "" && candidate.Quality != requested && !(requested == "thumbnail" && candidate.Quality == "preview") {
				continue
			}
			urlHash := sha256.Sum256([]byte(candidate.URL))
			if requested == "thumbnail" && candidate.Quality == "preview" && savedPreviewSource != "" && savedPreviewSource == hex.EncodeToString(urlHash[:]) {
				continue
			}
			if e := ctx.Err(); e != nil {
				return Result{}, e
			}
			if previous := r.FailedURLs[candidate.URL]; previous != nil {
				e = previous
				reasons = append(reasons, e.Error())
				continue
			}
			file = filepath.Join(dir, "preview.part")
			var digest Digest
			digest, e = c.mediaHashed(ctx, candidate.URL, file)
			if e != nil && !fatal(e) && r.FailedURLs != nil {
				r.FailedURLs[candidate.URL] = e
			}
			if e == nil {
				result, ext, e = validateDigest(r, file, candidate.Quality, digest)
				result.SourceHash = hex.EncodeToString(urlHash[:])
			}
			if e == nil {
				if requested == "preview" && result.Quality == "thumbnail" {
					e = errors.New("вместо превью сервер вернул миниатюру")
				} else if requested == "thumbnail" && result.Quality != "thumbnail" {
					e = errors.New("ссылка превью не содержит миниатюру")
				} else {
					break
				}
			}
			if fatal(e) {
				return Result{}, e
			}
			reasons = append(reasons, e.Error())
		}
	}
	if e != nil {
		if len(reasons) == 0 {
			reasons = append(reasons, e.Error())
		}
		return Result{}, fmt.Errorf("нет доступной проверенной версии: %s", strings.Join(reasons, "; "))
	}
	name := safeName(r.Name)
	if result.Quality != "original" {
		name = strings.TrimSuffix(name, filepath.Ext(name)) + "__" + result.Quality + ext
	}
	result.Entry = "media/" + name
	result.Warning = strings.Join(reasons, "; ")
	exif, rawEXIF := readEXIF(file)
	result.Metadata = metadataFrom(r.Raw, exif)
	result.APIMetadata = sanitizedAPI(r.Raw)
	result.EXIFBinary = rawEXIF
	idHash := sha256.Sum256([]byte(r.ID))
	key := hex.EncodeToString(idHash[:])
	result.Archive = filepath.ToSlash(filepath.Join("archives", key[:2], key+"-"+result.Quality+"-"+result.SHA256[:16]+".zip"))
	if e := writeArchive(ctx, s.Root, result, file); e != nil {
		return Result{}, &LocalError{e}
	}
	full, _ := localArchive(s.Root, result.Archive)
	if st, e := os.Stat(full); e == nil {
		result.ArchiveSize = st.Size()
		result.ArchiveMtime = st.ModTime().UnixNano()
	}
	return result, nil
}
func writeArchive(ctx context.Context, root string, r Result, file string) error {
	dst, e := localArchive(root, r.Archive)
	if e != nil {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(dst), "archive-*.part")
	if e != nil {
		return e
	}
	temp := f.Name()
	defer os.Remove(temp)
	zw := zip.NewWriter(f)
	hdr := &zip.FileHeader{Name: r.Entry, Method: zip.Store}
	hdr.SetModTime(time.Now())
	if r.Metadata.TimezoneKnown {
		if created, e := time.Parse(time.RFC3339Nano, r.Metadata.Created); e == nil {
			hdr.SetModTime(created)
		}
	}
	entry, e := zw.CreateHeader(hdr)
	if e == nil {
		src, oe := os.Open(file)
		if oe != nil {
			e = oe
		} else {
			h := sha256.New()
			n, ce := copyStream(io.MultiWriter(entry, h), &ctxReader{ctx: ctx, r: src})
			e = ce
			if e == nil && (n != r.Bytes || hex.EncodeToString(h.Sum(nil)) != r.SHA256) {
				e = errors.New("медиа изменилось между загрузкой и архивированием")
			}
			src.Close()
		}
	}
	if e == nil {
		var api io.Writer
		api, e = zw.Create("metadata/api.json")
		if e == nil {
			e = json.NewEncoder(api).Encode(r.APIMetadata)
		}
	}
	if e == nil && len(r.EXIFBinary) > 0 {
		var exif io.Writer
		exif, e = zw.Create("metadata/embedded-exif.tiff")
		if e == nil {
			_, e = exif.Write(r.EXIFBinary)
		}
	}
	if e == nil {
		var meta io.Writer
		meta, e = zw.Create("metadata.json")
		if e == nil {
			e = json.NewEncoder(meta).Encode(r)
		}
	}
	ze := zw.Close()
	se := f.Sync()
	fe := f.Close()
	if e != nil {
		return e
	}
	if ze != nil {
		return ze
	}
	if se != nil {
		return se
	}
	if fe != nil {
		return fe
	}
	if e := verifyZIP(ctx, temp, r); e != nil {
		return e
	}
	if _, e := os.Stat(dst); e == nil {
		if e := verifyZIP(ctx, dst, r); e == nil {
			return nil
		}
		if e := ctx.Err(); e != nil {
			return e
		}
		// Preserve corrupt bytes for inspection while replacing them with the
		// independently verified new archive. Never delete the damaged copy.
		if e := os.Rename(dst, dst+fmt.Sprintf(".corrupt-%d", time.Now().UnixNano())); e != nil {
			return e
		}
	}
	return os.Rename(temp, dst)
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}
func localArchive(root, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", errors.New("недопустимый путь архива")
	}
	full := filepath.Join(root, filepath.FromSlash(rel))
	r, e := filepath.Rel(root, full)
	if e != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", errors.New("путь архива выходит из папки")
	}
	return full, nil
}
func verifyZIP(ctx context.Context, file string, r Result) error {
	zr, e := zip.OpenReader(file)
	if e != nil {
		return e
	}
	defer zr.Close()
	found := false
	for _, f := range zr.File {
		if f.Name != r.Entry {
			// A shared server ZIP has independently verified media entries.
			// Reading all 500 entries again for each result would be quadratic.
			if !r.Bundle && !f.FileInfo().IsDir() {
				src, e := f.Open()
				if e != nil {
					return e
				}
				_, e = copyStream(io.Discard, &ctxReader{ctx: ctx, r: src})
				src.Close()
				if e != nil {
					return e
				}
			}
			continue
		}
		if found {
			return errors.New("дубликат записи в архиве")
		}
		found = true
		src, e := f.Open()
		if e != nil {
			return e
		}
		h := sha256.New()
		n, e := io.Copy(h, &ctxReader{ctx: ctx, r: src})
		src.Close()
		if e != nil {
			return e
		}
		if n != r.Bytes || hex.EncodeToString(h.Sum(nil)) != r.SHA256 {
			return errors.New("размер или SHA-256 архива не совпадает")
		}
	}
	if !found {
		return errors.New("медиа отсутствует в архиве")
	}
	return nil
}
