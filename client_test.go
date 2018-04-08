package bloomd

import (
	"net/url"
	"testing"
)

func TestClientConnect(t *testing.T) {
	for _, addr := range bloomdAddrs {
		c := createClientFromString(t, addr)
		defer closeClient(t, c)

		err := c.Ping()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientConnectionError(t *testing.T) {
	t.Run("Tcp addr has no port", func(t *testing.T) {
		_, err := NewFromAddr("tcp://foo")
		if err.Error() != "error: could not create socket (address foo: missing port in address)" {
			t.Error(err)
		}
	})

	t.Run("Scheme is not specified", func(t *testing.T) {
		_, err := NewFromAddr("foo")
		if err.Error() != "error: scheme is not presented in the url" {
			t.Error(err)
		}
	})

	t.Run("Scheme is not supported", func(t *testing.T) {
		_, err := NewFromAddr("http://foo:8000")
		if err.Error() != "error: http scheme is not supported" {
			t.Error(err)
		}
	})
}

func createClientFromString(tb testing.TB, addr string) *Client {
	return createClientFromURL(tb, parseBloomdURL(tb, addr))
}

func createClientFromURL(tb testing.TB, addr *url.URL) *Client {
	c, err := NewFromURL(addr)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

func closeClient(tb testing.TB, c *Client) {
	if err := c.Close(); err != nil {
		tb.Fatal(err)
	}
}
