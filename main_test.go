package bloomd

import (
	"log"
	"net/url"
	"os"
	"testing"
)

var bloomdAddr string

func TestMain(m *testing.M) {
	addrs := []string{
		getBloomdUnixAddr(),
		getBloomdTCPAddr(),
	}
	for _, addr := range addrs {
		log.Printf("Bloomd addr: %s", addr)
		bloomdAddr = addr
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

func getBloomdTCPAddr() string {
	return getEnv("BLOOMD", "tcp://localhost:8673")
}

func getBloomdUnixAddr() string {
	return getEnv("BLOOMD", "unix:///tmp/bloomd.sock")
}

func getEnv(name string, def string) string {
	val := os.Getenv(name)
	if val == "" {
		val = def
	}
	return val
}
