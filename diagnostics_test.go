package main

import (
	"strings"
	"testing"
)

func TestRateDiagnosticsHideAccountData(t *testing.T) {
	got := rateDiagnostic("https://disk.yandex.ru/models-v2?m=mpfs%2Fbulk-download-prepare&token=SECRET", "120", []byte(`{"error":{"code":"TOO_MANY_REQUESTS","message":"SECRET /disk/private.jpg","ckey":"SECRET"},"uid":"SECRET"}`))
	if !strings.Contains(got, "mpfs/bulk-download-prepare") || !strings.Contains(got, "120 с") || !strings.Contains(got, "TOO_MANY_REQUESTS") || strings.Contains(got, "SECRET") || strings.Contains(got, "private") {
		t.Fatal(got)
	}
	got = rateDiagnostic("https://cdn.invalid/path?secret=SECRET", "SECRET", []byte(`<html>SECRET</html>`))
	if strings.Contains(got, "SECRET") || strings.Contains(got, "cdn.invalid") {
		t.Fatal(got)
	}
}

func TestPublicAssetsContainOnlyUnsignedStaticJavaScript(t *testing.T) {
	got := publicScriptURLs([]byte(`<script src="https://yastatic.net/app.js"></script><script src='//yastatic.net/app.js'></script><script src="https://yastatic.net/app.js?token=SECRET"></script><script src="https://evil.invalid/SECRET.js"></script><script src="https://yastatic.net:443/SECRET.js"></script><script src="https://user:SECRET@yastatic.net/app.js"></script>`))
	if len(got) != 1 || got[0] != "https://yastatic.net/app.js" {
		t.Fatal(got)
	}
}
