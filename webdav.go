package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const webDAVBase = "https://webdav.yandex.ru"

type webDAVDownloadKey struct{}

var fullRangeRE = regexp.MustCompile(`^bytes 0-([0-9]+)/([0-9]+)$`)

type MediaStatusError struct{ Status int }

func (e *MediaStatusError) Error() string { return fmt.Sprintf("медиа: HTTP %d", e.Status) }

func parseOAuthToken(b []byte) (string, error) {
	token := strings.TrimSpace(string(b))
	if len(token) < 8 || len(token) > 4096 || strings.ContainsAny(token, " \r\n\t") {
		return "", errors.New("OAuth-файл должен содержать только токен, без заголовка OAuth и пробелов")
	}
	for _, ch := range token {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("_-.", ch)) {
			return "", errors.New("неподдерживаемый формат OAuth-токена")
		}
	}
	return token, nil
}

func (c *Client) loadWebDAVToken(path string) error {
	st, e := os.Stat(path)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return errors.New("не удалось прочитать локальный OAuth-файл")
	}
	if st.Size() > 8192 {
		return errors.New("OAuth-файл слишком большой")
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return errors.New("не удалось прочитать локальный OAuth-файл")
	}
	token, e := parseOAuthToken(b)
	if e != nil {
		return e
	}
	c.WebDAVToken = token
	return nil
}

func (c *Client) isWebDAVURL(u *url.URL) bool {
	base := c.WebDAVBase
	if base == "" {
		base = webDAVBase
	}
	b, e := url.Parse(base)
	return e == nil && u.Scheme == "https" && u.Host == b.Host && u.User == nil
}

func (c *Client) webDAVOriginal(ctx context.Context, r Resource, dir string, digest *Digest) (string, error) {
	base := c.WebDAVBase
	if base == "" {
		base = webDAVBase
	}
	u, e := url.Parse(base)
	if e != nil || !c.isWebDAVURL(u) {
		return "", errors.New("неверный адрес WebDAV")
	}
	p := r.ID
	if strings.HasPrefix(p, "disk:/") {
		p = "/disk/" + strings.TrimPrefix(p, "disk:/")
	}
	if strings.HasPrefix(p, "photounlim:/") {
		p = "/photounlim/" + strings.TrimPrefix(p, "photounlim:/")
	}
	if !strings.HasPrefix(p, "/disk/") && !strings.HasPrefix(p, "/photounlim/") {
		return "", errors.New("ресурс не имеет пути Диска для WebDAV")
	}
	// The public WebDAV root is the user's disk, unlike MPFS's /disk namespace.
	// Photoslice's separate photounlim namespace must remain explicit.
	if strings.HasPrefix(p, "/disk/") {
		p = strings.TrimPrefix(p, "/disk")
	}
	u.Path = strings.TrimRight(u.Path, "/") + p
	target := filepath.Join(dir, "webdav-original.part")
	d, e := c.mediaHashed(context.WithValue(ctx, webDAVDownloadKey{}, true), u.String(), target)
	if e != nil {
		var status *MediaStatusError
		if errors.As(e, &status) && (status.Status == 401 || status.Status == 403) && c.webDAVRejected.CompareAndSwap(false, true) && c.Log != nil {
			c.Log("WebDAV не принял OAuth или права доступа. Мобильный способ приостановлен; доступные веб-ссылки продолжают проверяться.")
		}
		return "", e
	}
	if _, _, e := validateDigest(r, target, "original", d); e != nil {
		return "", e
	}
	if digest != nil {
		*digest = d
	}
	return target, nil
}

func fullWebDAVResponse(ctx context.Context, r *http.Response) bool {
	if enabled, _ := ctx.Value(webDAVDownloadKey{}).(bool); !enabled {
		return false
	}
	m := fullRangeRE.FindStringSubmatch(r.Header.Get("Content-Range"))
	if len(m) != 3 {
		return false
	}
	end, e := strconv.ParseInt(m[1], 10, 64)
	if e != nil {
		return false
	}
	total, e := strconv.ParseInt(m[2], 10, 64)
	return e == nil && total > 0 && end == total-1 && (r.ContentLength < 0 || r.ContentLength == total)
}
