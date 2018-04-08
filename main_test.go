package bloomd

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
)

var bloomdAddrs []string

func TestMain(m *testing.M) {
	sList := getSchemaList()
	bloomdAddrs = make([]string, 0, len(sList))
	for _, schema := range getSchemaList() {
		bloomdAddrs = append(bloomdAddrs, getBloomdAddr(schema))

	}
	code := m.Run()
	os.Exit(code)
}

func parseBloomdURL(tb testing.TB, addr string) *url.URL {
	u, err := url.Parse(addr)
	if err != nil {
		tb.Fatal(err)
	}
	return u
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
