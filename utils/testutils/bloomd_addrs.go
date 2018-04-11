package testutils

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
)

var BloomdAddrs = addrsFromEnv

func UseAddrsFromEnv() {
	BloomdAddrs = addrsFromEnv
}

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

func TestForAllAddrs(t *testing.T, f func(*url.URL, *testing.T)) {
	for _, addr := range BloomdAddrs() {
		url := ParseURL(t, addr)
		t.Run("Test address "+addr, func(t *testing.T) {
			f(url, t)
		})
	}
}
