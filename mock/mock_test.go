package mock

import (
	"net"
	"testing"

	bloomd "github.com/Applifier/go-bloomd"
)

func TestMock(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server := NewMockServer(serverConn)
	go server.Serve()

	client, err := bloomd.NewFromConn(clientConn)
	requireNoError(t, err)

	filter, err := client.CreateFilter("test_filter", 1000, 0.01, true)
	requireNoError(t, err)

	filter = client.GetFilter("test_filter")

	b, err := filter.Set(bloomd.Key("key"))
	requireNoError(t, err)
	if !b {
		t.Fatal("set operation expected to be successful")
	}

	r, err := filter.Check(bloomd.Key("key"))
	requireNoError(t, err)
	if !r {
		t.Fatal("check operation expected to be successful")
	}

	n, err := filter.Check(bloomd.Key("no_exist"))
	requireNoError(t, err)
	if n {
		t.Fatal("check operation expected to be unsuccessful")
	}

	filters := server.Filters()

	f, ok := filters["test_filter"]
	if !ok {
		t.Fatal("filter is expected to be found in internal storage")
	}

	k, ok := f["key"]
	if !ok || !k {
		t.Fatal("key is expected to be found in internal storage")
	}
}

func requireNoError(tb testing.TB, err error) {
	if err != nil {
		tb.Fatal(err)
	}
}
