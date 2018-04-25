package testutils

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
)

// BloomdAddrs returns a list of bloomd addrs for testing
var BloomdAddrs = addrsFromEnv

var envAddr []string
var envAddrOnce sync.Once

func addrsFromEnv() []string {
	envAddrOnce.Do(func() {
		sList := getSchemaList()
		envAddr = make([]string, 0, len(sList))
		for _, schema := range getSchemaList() {
			envAddr = append(envAddr, getBloomdAddr(schema))
		}
	})
	return envAddr
}

var bloomdAddrDefaults = map[string]string{
	"TCP":  "tcp://localhost:8673",
	"UNIX": "unix:///tmp/bloomd.sock",
}

func getBloomdAddr(schema string) string {
	defaultAddr := bloomdAddrDefaults[schema]
	return getEnv(fmt.Sprintf("BLOOMD_%s", schema), defaultAddr)
}

func getSchemaList() []string {
	schemas := getEnv("BLOOMD_SCHEMAS", "TCP,UNIX")
	return strings.Split(schemas, ",")
}

func getEnv(name string, def string) string {
	val := os.Getenv(name)
	if val == "" {
		val = def
	}
	return val
}

// TestForAllAddrs allows to run tests against all bloomd addrs
func TestForAllAddrs(t *testing.T, f func(*url.URL, *testing.T)) {
	t.Helper()
	for _, addr := range BloomdAddrs() {
		url := ParseURL(t, addr)
		t.Run("Test address "+addr, func(t *testing.T) {
			f(url, t)
		})
	}
}

// BenchForAllAddrs allows to run benchmarks against all bloomd addrs
func BenchForAllAddrs(b *testing.B, f func(*url.URL, *testing.B)) {
	b.Helper()
	for _, addr := range BloomdAddrs() {
		url := ParseURL(b, addr)
		b.Run("Test address "+addr, func(b *testing.B) {
			f(url, b)
		})
	}
}
