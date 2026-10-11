package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestZIPPartialPassDefersMissingPhotosToPeriodicChecker(t *testing.T) {
	s := testStore(t)
	s.set("inventory", "incomplete")
	data := picture(800, 600)
	r := []Resource{zipTestResource("photounlim:/a", "a.png", data), zipTestResource("photounlim:/missing", "missing.png", data)}
	s.saveMany(r)
	var calls atomic.Int32
	c := apiFixture(t, func(d Object, base string) Object {
		calls.Add(1)
		if len(arr(obj(d["requestParams"])["items"])) != 2 {
			t.Error("missing photo triggered an individual API request")
		}
		return Object{"download_url": base + "/zip"}
	}, map[string][]byte{"/client/disk": []byte(`{"sk":"test","uid":"123"}`), "/zip": zipBytes("a.png", data)}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e := runInventory(ctx, s, c, 1, "zip", func(string) {}); e != nil {
		t.Fatal("partial ZIP failed entire pass", e)
	}
	st, _ := s.stats()
	if calls.Load() != 1 || st.Done != 1 || st.Pending != 1 || st.Failed != 0 || st.Preview != 0 || st.Thumbnail != 0 || st.Inventory != "incomplete" {
		t.Fatal(st, calls.Load())
	}
}
