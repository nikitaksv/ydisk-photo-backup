package main

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

var diagnosticCodeRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.:-]{0,79}$`)
var diagnosticMethodRE = regexp.MustCompile(`^(mpfs|intapi)/[a-z][a-z0-9-]{0,79}$`)
var scriptSourceRE = regexp.MustCompile(`(?i)<script\b[^>]*\bsrc\s*=\s*["']([^"']+)["']`)

func isZIPPrepareURL(uri string) bool {
	u, e := url.Parse(uri)
	return e == nil && u.Path == "/models-v2" && u.Query().Get("m") == "mpfs/bulk-download-prepare"
}

func archiveDownloadDisabled(body []byte) bool {
	d, e := decodeJSON(body)
	if e != nil {
		return false
	}
	err := obj(d["error"])
	return str(err["type"]) == "mpfs" && num(err["code"]) == 394
}

// Never log response messages, identifiers, arbitrary headers or signed URLs.
func rateDiagnostic(uri, retryAfter string, body []byte) string {
	operation := "media"
	if u, e := url.Parse(uri); e == nil {
		if u.Path == "/models-v2" {
			operation = "models-v2"
			if method := u.Query().Get("m"); diagnosticMethodRE.MatchString(method) {
				operation = method
			}
		} else if u.Path == "/client/disk" {
			operation = "auth"
		}
	}
	retry := "не указан"
	if delay := retryAfterDuration(retryAfter, time.Now()); delay > 0 {
		retry = fmt.Sprintf("%.0f с", delay.Seconds())
	} else if strings.TrimSpace(retryAfter) == "0" {
		retry = "0 с"
	}
	codes := map[string]bool{}
	if d, e := decodeJSON(body); e == nil {
		for _, item := range []Object{d, obj(d["error"])} {
			for _, key := range []string{"code", "error_code", "type", "name", "error"} {
				if key == "code" {
					if value := str(item[key]); regexp.MustCompile(`^[0-9]{1,8}$`).MatchString(value) {
						codes["code="+value] = true
					}
				}
				if value, ok := item[key].(string); ok && diagnosticCodeRE.MatchString(value) {
					codes[value] = true
				}
			}
		}
	}
	var names []string
	for name := range codes {
		names = append(names, name)
	}
	sort.Strings(names)
	reason := "код причины не указан"
	if len(names) > 0 {
		reason = strings.Join(names, ", ")
	}
	return fmt.Sprintf("HTTP 429 · %s · Retry-After: %s · %s", operation, retry, reason)
}

// The HTML contains session credentials. Keep only public static JS paths.
func publicScriptURLs(html []byte) []string {
	seen := map[string]bool{}
	for _, match := range scriptSourceRE.FindAllSubmatch(html, -1) {
		uri := string(match[1])
		if strings.HasPrefix(uri, "//") {
			uri = "https:" + uri
		}
		u, e := url.Parse(uri)
		if e != nil || u.Scheme != "https" || u.User != nil || u.Hostname() != "yastatic.net" || u.Port() != "" || u.RawQuery != "" || u.Fragment != "" || !strings.HasSuffix(u.Path, ".js") {
			continue
		}
		seen[u.String()] = true
	}
	var result []string
	for uri := range seen {
		result = append(result, uri)
	}
	sort.Strings(result)
	return result
}
