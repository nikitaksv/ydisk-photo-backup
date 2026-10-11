package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const baseURL = "https://disk.yandex.ru"
const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

var ErrAuth = errors.New("сессия Яндекса недействительна или требуется CAPTCHA; войди в браузере и обнови cookies")

type Cookie struct {
	Name       string  `json:"name"`
	Value      string  `json:"value"`
	Domain     string  `json:"domain"`
	Path       string  `json:"path"`
	Secure     bool    `json:"secure"`
	HTTPOnly   bool    `json:"httpOnly"`
	Expiration float64 `json:"expirationDate"`
	HostOnly   bool    `json:"hostOnly"`
}
type Client struct {
	WebDAVToken        string // loaded from a local file; never exposed in status or logs
	WebDAVBase         string
	webDAVRejected     atomic.Bool
	downloadStage      string // guarded by mu; stage changes only after all workers finish
	BatchOriginals     bool
	FreshLinks         *linkRefresher
	zipFallbackMu      sync.Mutex
	zipFallbackStart   int64
	zipFallbackStopped bool
	ZIPBatchSize       int
	ZIPMaxBatches      int
	Batch              int
	HTTP               *http.Client
	Base               string
	mu                 sync.Mutex
	sk, uid            string
	next               time.Time
	Interval           time.Duration
	baseInterval       time.Duration
	configuredInterval time.Duration
	fallbackInterval   time.Duration
	fast429Since       time.Time
	fast429Episodes    int
	fastFallback       bool
	measurementStart   time.Time
	requestsStarted    int64
	requestsSucceeded  int64
	tune               RateTuneStatus
	tuneStarted        time.Time
	tuneRequests       int64
	tuneSuccesses      int64
	mediaBytes         atomic.Int64
	cooldown           time.Time
	backoffLevel       int
	limitEvents        int
	accepted           int
	CooldownBase       time.Duration
	CooldownMax        time.Duration
	Log                func(string)
	Checkpoint         func(RateStatus) error
	checkpointMu       sync.Mutex
	APITimeout         time.Duration
	HeaderWait         time.Duration
	FirstByteWait      time.Duration
	IdleWait           time.Duration
	AssetSink          func([]string)
}

func yandexDomain(s string) bool {
	s = strings.TrimPrefix(strings.ToLower(s), ".")
	return s == "yandex.ru" || strings.HasSuffix(s, ".yandex.ru") || s == "yandex.com" || strings.HasSuffix(s, ".yandex.com")
}
func parseCookies(b []byte) ([]Cookie, error) {
	b = bytes.TrimPrefix(b, []byte{0xef, 0xbb, 0xbf})
	var a []Cookie
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, fmt.Errorf("нужен JSON-массив Cookie-Editor: %w", err)
	}
	var valid []Cookie
	for _, c := range a {
		if c.Domain == "" {
			c.Domain = ".yandex.ru"
		}
		if c.Name != "" && c.Value != "" && yandexDomain(c.Domain) {
			valid = append(valid, c)
		}
	}
	if len(valid) == 0 {
		return nil, errors.New("в файле нет cookies Яндекса")
	}
	return valid, nil
}
func newClient(path string) (*Client, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, fmt.Errorf("cookies: %w", e)
	}
	a, e := parseCookies(b)
	if e != nil {
		return nil, e
	}
	jar, _ := cookiejar.New(nil)
	for _, c := range a {
		domain := strings.TrimPrefix(c.Domain, ".")
		u, _ := url.Parse("https://" + domain)
		p := c.Path
		if p == "" {
			p = "/"
		}
		hc := &http.Cookie{Name: c.Name, Value: c.Value, Path: p, Secure: c.Secure, HttpOnly: c.HTTPOnly}
		if !c.HostOnly {
			hc.Domain = c.Domain
		}
		if c.Expiration > 0 {
			hc.Expires = time.Unix(int64(c.Expiration), 0)
		}
		jar.SetCookies(u, []*http.Cookie{hc})
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 24
	tr.ResponseHeaderTimeout = 8 * time.Second
	tr.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	h := &http.Client{Jar: jar, Transport: tr, Timeout: 20 * time.Minute, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 10 {
			return errors.New("слишком много перенаправлений")
		}
		if req.URL.Scheme != "https" {
			return errors.New("небезопасное перенаправление")
		}
		return nil
	}}
	return &Client{HTTP: h, Base: baseURL, Interval: 500 * time.Millisecond, HeaderWait: 8 * time.Second, FirstByteWait: 8 * time.Second, Batch: 16}, nil
}
func waitContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (c *Client) pace(ctx context.Context) error {
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		c.mu.Lock()
		deadline := c.next
		if c.cooldown.After(deadline) {
			deadline = c.cooldown
		}
		delay := time.Until(deadline)
		if delay <= 0 {
			c.next = time.Now().Add(c.Interval)
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		// Recheck after waking: another worker may have extended the cooldown
		// or consumed the next slot. No permits are reserved in advance.
		if e := waitContext(ctx, delay); e != nil {
			return e
		}
	}
}
func (c *Client) request(ctx context.Context, method, uri string, body []byte) (*http.Response, error) {
	for {
		if e := c.pace(ctx); e != nil {
			return nil, e
		}
		c.recordRequest()
		res, e := c.requestOnce(ctx, method, uri, body)
		if e != nil {
			return nil, e
		}
		if res.StatusCode != http.StatusTooManyRequests {
			if res.StatusCode >= 200 && res.StatusCode < 300 {
				if e := c.acceptedRequest(); e != nil {
					res.Body.Close()
					return nil, e
				}
			}
			return res, nil
		}
		// Rate limits are scheduling events. They never exhaust a file's
		// retry budget and never trigger a lower quality fallback.
		var diagnosticBody []byte
		// MPFS uses HTTP 429 / code 394 for "archive download disabled".
		// Inspect only this API method; CDN and other 429 responses still
		// establish the shared cooldown before reading an error body.
		if isZIPPrepareURL(uri) {
			diagnosticBody, _ = io.ReadAll(io.LimitReader(res.Body, 4096))
			if archiveDownloadDisabled(diagnosticBody) {
				res.Body.Close()
				if c.Log != nil {
					c.Log("Серверный ZIP недоступен: MPFS 394 · скачивание архивов отключено Яндексом. Это не ограничение RPS; отдельные загрузки продолжаются.")
				}
				return nil, errZIPDisabled
			}
		}
		retryAfter := res.Header.Get("Retry-After")
		if e := c.throttle(retryAfter); e != nil {
			res.Body.Close()
			return nil, e
		}
		if c.Log != nil {
			if diagnosticBody == nil {
				diagnosticBody, _ = io.ReadAll(io.LimitReader(res.Body, 4096))
			}
			c.Log(rateDiagnostic(uri, retryAfter, diagnosticBody))
		}
		res.Body.Close()
		if deferred, _ := ctx.Value(zipDeferKey{}).(bool); deferred {
			return nil, errZIPDeferred
		}
	}
}
func (c *Client) requestOnce(ctx context.Context, method, uri string, body []byte) (*http.Response, error) {
	requestCtx, cancel := context.WithCancel(ctx)
	req, e := http.NewRequestWithContext(requestCtx, method, uri, bytes.NewReader(body))
	if e != nil {
		cancel()
		return nil, errors.New("неверный адрес запроса")
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Referer", c.Base+"/client/photo")
	req.Header.Set("Accept-Language", "ru,en;q=0.9")
	if c.isWebDAVURL(req.URL) && c.WebDAVToken != "" {
		req.Header.Set("Authorization", "OAuth "+c.WebDAVToken)
		req.Header.Set("Range", "bytes=0-")
	}
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", c.Base)
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
	}
	client := c.HTTP
	if method == "POST" || uri == c.Base+"/client/disk" {
		apiClient := *client
		limit := durationOr(c.APITimeout, 15*time.Second)
		if apiClient.Timeout == 0 || apiClient.Timeout > limit {
			apiClient.Timeout = limit
		}
		client = &apiClient
	}
	// Redirect hops also contact Yandex and must use the shared request budget.
	redirectClient := *client
	remaining := redirectClient.Timeout
	bounded := remaining > 0
	redirectClient.Timeout = 0
	activeSince := time.Now()
	headerTimer := time.AfterFunc(durationOr(c.HeaderWait, 15*time.Second), cancel)
	var totalTimer *time.Timer
	var budgetExpired atomic.Bool
	expireBudget := func() { budgetExpired.Store(true); cancel() }
	if bounded {
		totalTimer = time.AfterFunc(remaining, expireBudget)
	}
	finish := func() {
		headerTimer.Stop()
		if totalTimer != nil {
			totalTimer.Stop()
		}
		cancel()
	}
	previousRedirect := redirectClient.CheckRedirect
	redirectClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// Mobile OAuth belongs only to the WebDAV origin, never to a CDN
		// redirect or a different host supplied by a server response.
		if !c.isWebDAVURL(req.URL) {
			req.Header.Del("Authorization")
		}
		if previousRedirect != nil {
			if e := previousRedirect(req, via); e != nil {
				return e
			}
		} else if len(via) >= 10 {
			return errors.New("слишком много перенаправлений")
		}
		// Scheduling waits do not consume network probe/download timeouts.
		if !headerTimer.Stop() {
			return context.DeadlineExceeded
		}
		if totalTimer != nil && !totalTimer.Stop() {
			return context.DeadlineExceeded
		}
		if bounded {
			remaining -= time.Since(activeSince)
			if remaining <= 0 {
				return context.DeadlineExceeded
			}
		}
		if e := c.pace(req.Context()); e != nil {
			return e
		}
		c.recordRequest()
		activeSince = time.Now()
		headerTimer = time.AfterFunc(durationOr(c.HeaderWait, 15*time.Second), cancel)
		if bounded {
			totalTimer = time.AfterFunc(remaining, expireBudget)
		}
		return nil
	}
	client = &redirectClient
	res, e := client.Do(req)
	headerExpired := !headerTimer.Stop()
	if e != nil {
		totalExpired := totalTimer != nil && !totalTimer.Stop()
		finish()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if headerExpired || totalExpired || timeoutError(e) {
			return nil, &ProbeTimeoutError{Stage: "ответа сервера"}
		}
		return nil, errors.New("сетевая ошибка / таймаут (адрес скрыт)")
	}
	if headerExpired {
		res.Body.Close()
		finish()
		return nil, &ProbeTimeoutError{Stage: "ответа сервера"}
	}
	res.Body = &progressBody{body: res.Body, cancel: finish, budgetExpired: budgetExpired.Load, firstWait: durationOr(c.FirstByteWait, 15*time.Second), idleWait: durationOr(c.IdleWait, 30*time.Second)}
	return res, nil
}

var skRE = regexp.MustCompile(`"sk"\s*:\s*"([^"\r\n]+)"`)
var uidRE = regexp.MustCompile(`"uid"\s*:\s*"?(\d+)"?`)

func (c *Client) auth(ctx context.Context) error {
	res, e := c.request(ctx, "GET", c.Base+"/client/disk", nil)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || strings.Contains(res.Request.URL.Host, "passport") || strings.Contains(res.Request.URL.Path, "captcha") {
		return ErrAuth
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, 32<<20))
	if e != nil {
		return e
	}
	sk := skRE.FindSubmatch(b)
	uid := uidRE.FindSubmatch(b)
	if len(sk) < 2 || len(uid) < 2 {
		return ErrAuth
	}
	if c.AssetSink != nil {
		c.AssetSink(publicScriptURLs(b))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sk = string(sk[1])
	c.uid = string(uid[1])
	return nil
}
func decodeJSON(b []byte) (Object, error) {
	var v Object
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	e := d.Decode(&v)
	return v, e
}
func (c *Client) call(ctx context.Context, method string, params any) (Object, error) {
	var last error
	attempts := 4
	if method == "mpfs/bulk-download-prepare" {
		attempts = 1
	}
	refreshedKey := false
	for i := 0; i < attempts; i++ {
		c.mu.Lock()
		sk, uid := c.sk, c.uid
		c.mu.Unlock()
		body, _ := json.Marshal(Object{"sk": sk, "connection_id": fmt.Sprintf("%s%d", uid, time.Now().UnixNano()), "apiMethod": method, "requestParams": params})
		res, e := c.request(ctx, "POST", c.Base+"/models-v2?m="+url.QueryEscape(method), body)
		if e != nil {
			last = e
		} else {
			b, re := io.ReadAll(io.LimitReader(res.Body, 64<<20))
			res.Body.Close()
			if re != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if timeoutError(re) {
					return nil, &ProbeTimeoutError{Stage: "ответа API"}
				}
				last = errors.New("ответ API оборвался")
			} else {
				d, de := decodeJSON(b)
				errObj := obj(d["error"])
				if first(errObj, "code") == "CKEY" && first(errObj, "ckey") != "" {
					if refreshedKey {
						return nil, ErrAuth
					}
					refreshedKey = true
					c.mu.Lock()
					c.sk = first(errObj, "ckey")
					c.mu.Unlock()
					i--
					continue
				}
				if res.StatusCode == 401 || res.StatusCode == 403 || str(d["type"]) == "captcha" || strings.Contains(res.Request.URL.Path, "captcha") {
					return nil, ErrAuth
				}
				if res.StatusCode == 200 && de == nil {
					if val, ok := d["error"]; ok && val != nil && val != false {
						return nil, fmt.Errorf("%s: ошибка API (code=%s)", method, first(errObj, "code", "name"))
					}
					return d, nil
				}
				last = fmt.Errorf("%s: HTTP %d / неподдерживаемый ответ", method, res.StatusCode)
				if res.StatusCode >= 400 && res.StatusCode < 500 {
					return nil, last
				}
			}
		}
		if fatal(last) || isProbeTimeout(last) {
			return nil, last
		}
		if i < attempts-1 {
			if e := waitContext(ctx, time.Duration(1<<i)*time.Second); e != nil {
				return nil, e
			}
		}
	}
	return nil, last
}
func (c *Client) media(ctx context.Context, uri, dst string) error {
	return c.transfer(ctx, uri, dst, nil)
}
func (c *Client) mediaHashed(ctx context.Context, uri, dst string) (Digest, error) {
	var d Digest
	e := c.transfer(ctx, uri, dst, &d)
	return d, e
}
func (c *Client) transfer(ctx context.Context, uri, dst string, digest *Digest) error {
	uri = strings.ReplaceAll(uri, "&amp;", "&")
	if strings.HasPrefix(uri, "//") {
		uri = "https:" + uri
	}
	u, e := url.Parse(uri)
	if e != nil || u.Host == "" || u.User != nil || u.Scheme != "https" {
		return errors.New("API вернул неподдерживаемый URL")
	}
	var last error
	for i := 0; i < 3; i++ {
		res, e := c.request(ctx, "GET", uri, nil)
		if e != nil {
			last = e
		} else {
			if res.StatusCode == 200 || res.StatusCode == 206 && fullWebDAVResponse(ctx, res) {
				if res.StatusCode == 206 && res.ContentLength < 0 {
					m := fullRangeRE.FindStringSubmatch(res.Header.Get("Content-Range"))
					res.ContentLength, _ = strconv.ParseInt(m[2], 10, 64)
				}
				ct := strings.ToLower(res.Header.Get("Content-Type"))
				if strings.Contains(ct, "text/html") || strings.Contains(ct, "application/json") {
					res.Body.Close()
					return errors.New("вместо медиа получена страница/JSON")
				}
				f, e := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
				if e != nil {
					res.Body.Close()
					return &LocalError{e}
				}
				var target io.Writer = f
				sha, md := sha256.New(), md5.New()
				if digest != nil {
					target = io.MultiWriter(f, sha, md)
				}
				n, ce := copyStream(target, &mediaCounter{Reader: res.Body, total: &c.mediaBytes})
				se := f.Sync()
				fe := f.Close()
				res.Body.Close()
				if ce == nil && se == nil && fe == nil && n > 0 && (res.ContentLength < 0 || res.ContentLength == n) {
					if digest != nil {
						*digest = Digest{n, hex.EncodeToString(sha.Sum(nil)), hex.EncodeToString(md.Sum(nil))}
					}
					return nil
				}
				if se != nil {
					return &LocalError{se}
				}
				if fe != nil {
					return &LocalError{fe}
				}
				if ce != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if timeoutError(ce) {
						return &ProbeTimeoutError{Stage: "поступления данных медиа"}
					}
					var pe *os.PathError
					if errors.As(ce, &pe) {
						return &LocalError{ce}
					}
				}
				last = errors.New("медиа пустое или скачано не полностью")
			} else {
				res.Body.Close()
				last = &MediaStatusError{Status: res.StatusCode}
				// An unsuccessful availability probe must not occupy a worker
				// with immediate retries. Record it and try the next available
				// quality; a later version check can retry transient HTTP errors.
				// HTTP 429 is handled by request's shared pause, before this point.
				return last
			}
		}
		if fatal(last) || isProbeTimeout(last) {
			return last
		}
		if i < 2 {
			if e := waitContext(ctx, time.Duration(1<<i)*time.Second); e != nil {
				return e
			}
		}
	}
	return last
}

type LocalError struct{ error }
