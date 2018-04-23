package testutils

import (
	"net/url"
	"testing"
)

func ParseURL(tb testing.TB, addr string) *url.URL {
	tb.Helper()
	u, err := url.Parse(addr)
	if err != nil {
		tb.Fatal(err)
	}
	return u
}
