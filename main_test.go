package bloomd

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"testing"
)

var bloomdAddr string

func TestMain(m *testing.M) {
	for _, schema := range getSchemaList() {
		bloomdAddr = getBloomdAddr(schema)
		log.Printf("Bloomd addr: %s", bloomdAddr)
		if code := m.Run(); code != 0 {
			os.Exit(code)
		}
	}
	os.Exit(0)
}

func getBloomdURL(tb testing.TB) *url.URL {
	u, err := url.Parse(bloomdAddr)
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
