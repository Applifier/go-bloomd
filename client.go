package bloomd

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap/buffer"

	pool "gopkg.in/fatih/pool.v2"
)

const startMarker = "START"
const endMaker = "END"

// DefaultBufferSize is the default size for the read buffer
var DefaultBufferSize = 4096

var rpool *readerpool
var poolOnce = sync.Once{}

// Client represents a bloomd client
type Client struct {
	conn              net.Conn
	reader            *bufio.Reader
	connErrorReturned bool
}

// NewFromAddr creates a new bloomd client from addr
func NewFromAddr(addr string) (*Client, error) {
	conn, err := createSocket(addr)
	if err != nil {
		return nil, err
	}

	return NewFromConn(conn)
}

// NewFromConn creates a new bloomd client from net.Conn
func NewFromConn(conn net.Conn) (cli *Client, err error) {
	poolOnce.Do(func() {
		rpool = newReaderPool(DefaultBufferSize)
	})

	return &Client{
		conn:   conn,
		reader: rpool.Get(conn),
	}, nil
}

// ListFilters list all filters
func (cli *Client) ListFilters() ([]Filter, error) {
	if err := cli.send([]byte("list")); err != nil {
		return nil, err
	}

	filterNames, err := cli.readList()
	if err != nil {
		return nil, err
	}

	filters := make([]Filter, len(filterNames))
	for i, filter := range filterNames {
		filters[i] = cli.GetFilter(filter)
	}

	return filters, nil
}

// GetFilter returns a previously created filter
func (cli *Client) GetFilter(name string) Filter {
	return Filter{
		Name:   name,
		client: cli,
	}
}

// CreateFilter creates a new filter or returns an existing one
func (cli *Client) CreateFilter(f Filter) (Filter, error) {
	f.client = cli

	if f.Prob > 0 && f.Capacity < 1 {
		return f, Error{
			Message: "Invalid capacity",
		}
	}

	var b buffer.Buffer

	b.Write([]byte("create " + f.Name))
	if f.Capacity > 0 {
		b.Write([]byte(" capacity=" + strconv.Itoa(f.Capacity)))
	}
	if f.Prob > 0 {
		b.Write([]byte(" prob=" + strconv.FormatFloat(f.Prob, 'f', -1, 64)))
	}
	if f.InMemory {
		b.Write([]byte(" in_memory=1"))
	}

	if err := cli.send(b.Bytes()); err != nil {
		return f, err
	}

	resp, err := cli.read()
	if err != nil {
		return f, err
	}

	if resp != "Done" && resp != "Exists" {
		return f, Error{
			Message: "invalid response received from server",
		}
	}

	return f, nil
}

// Close closes underlying connection or return the connection to the Pool if one was used
func (cli *Client) Close() error {
	rpool.Put(cli.reader)

	if cli.connErrorReturned {
		if pc, ok := cli.conn.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
	}

	return cli.conn.Close()
}

// Ping pings the server
func (cli *Client) Ping() error {
	resp, err := cli.sendAndReceive([]byte("ping"))
	// Yeap bloomd has no actual ping command. But this should cause the least amount of side effects
	if resp != "Client Error: Command not supported" {
		return Error{
			Message: "invalid response received",
			Err:     errors.New(resp),
		}
	}
	return err
}

func (cli *Client) send(cmd []byte) error {
	_, err := cli.conn.Write(append(cmd, '\n'))

	if err != nil {
		cli.connErrorReturned = true
		return Error{Err: err, Message: "error while writing to bloomd server", ShouldRetryWithNewClient: true}
	}

	return nil
}

func (cli *Client) read() (string, error) {
	l, err := cli.reader.ReadString('\n')
	if err != nil {
		cli.connErrorReturned = true
		return l, Error{Err: err, Message: "error while reader input from bloomd server", ShouldRetryWithNewClient: true}
	}
	return strings.TrimRight(l, "\r\n"), nil
}

func (cli *Client) sendAndReceive(cmd []byte) (string, error) {
	if err := cli.send(cmd); err != nil {
		return "", err
	}

	return cli.read()
}

func (cli *Client) readList() ([]string, error) {
	start, err := cli.read()
	if err != nil {
		return nil, err
	}

	lines := make([]string, 0, 5)

	if start != startMarker {
		return nil, Error{Message: fmt.Sprintf("expected START, got %s", start)}
	}

	for {
		line, err := cli.read()
		if err != nil {
			return nil, err
		}

		if line == endMaker {
			break
		}

		lines = append(lines, line)
	}

	return lines, nil
}

func createSocket(saddr string) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", saddr)
	if err != nil {
		return nil, Error{Message: "error: could not create socket", Err: err}
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, Error{Message: "error: could not create socket", Err: err}
	}

	return conn, nil
}
