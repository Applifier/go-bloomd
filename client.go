package bloomd

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap/buffer"

	pool "gopkg.in/fatih/pool.v2"
)

// DefaultBufferSize is the default size for the read buffer
var DefaultBufferSize = 4096

const startMarker = "START"
const endMaker = "END"
const cmdDelimeter = '\n'
const itemDelimeter = ' '

// Client represents a bloomd client
type Client struct {
	conn         net.Conn
	resultReader *resultReader
	reader       *bufio.Reader
	writer       *bufio.Writer
	err          error

	clientPool *sync.Pool
}

// NewFromAddr creates a new bloomd client from addr
func NewFromAddr(addr string) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	return NewFromURL(u)
}

// NewFromURL creates a new bloomd client from URL struct
func NewFromURL(u *url.URL) (*Client, error) {
	conn, err := createSocket(u)
	if err != nil {
		return nil, err
	}

	return NewFromConn(conn)
}

func createSocket(u *url.URL) (net.Conn, error) {
	switch u.Scheme {
	case "unix":
		return createUnixSocket(u.Path)
	case "tcp":
		return createTCPSocket(u.Host)
	case "":
		return nil, fmt.Errorf("error: scheme is not presented in the url")
	default:
		return nil, fmt.Errorf("error: %s scheme is not supported", u.Scheme)
	}
}

func newClient() *Client {
	cli := &Client{
		reader: bufio.NewReaderSize(nil, DefaultBufferSize),
		writer: bufio.NewWriterSize(nil, DefaultBufferSize),
	}
	cli.resultReader = &resultReader{
		client: cli,
	}
	return cli
}

// NewFromConn creates a new bloomd client from net.Conn
func NewFromConn(conn net.Conn) (cli *Client, err error) {
	cli = newClient()
	cli.reset(conn)

	return cli, nil
}

// ListFilters list all filters
func (cli *Client) ListFilters() ([]Filter, error) {
	if err := cli.send([]byte("list")); err != nil {
		return nil, err
	}

	filterLines, err := cli.readList()
	if err != nil {
		return nil, err
	}

	filters := make([]Filter, len(filterLines))
	for i, filterLine := range filterLines {
		filterName := strings.Fields(filterLine)[0]
		filters[i] = cli.GetFilter(filterName)
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
func (cli *Client) CreateFilter(name string, capacity int, prob float64, inMemory bool) (Filter, error) {
	f := Filter{
		Name:   name,
		client: cli,
	}

	if prob > 0 && capacity < 1 {
		return f, Error{
			Message: "Invalid capacity",
		}
	}

	var b buffer.Buffer

	b.Write([]byte("create " + f.Name))
	if capacity > 0 {
		b.Write([]byte(" capacity=" + strconv.Itoa(capacity)))
	}
	if prob > 0 {
		b.Write([]byte(" prob=" + strconv.FormatFloat(prob, 'f', -1, 64)))
	}
	if inMemory {
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
			Message: fmt.Sprintf("invalid response received from server: %s", resp),
		}
	}

	return f, nil
}

// Close closes underlying connection or return the connection to the Pool if one was used
func (cli *Client) Close() error {
	if cli.err != nil {
		if pc, ok := cli.conn.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
	}

	err := cli.conn.Close()

	if cli.clientPool != nil {
		cli.clientPool.Put(cli)
	} else {
		// Since we don't need a client object ny more, just handle the reference loop
		cli.resultReader.client = nil
	}

	return err
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

func (cli *Client) reset(conn net.Conn) {
	cli.conn = conn
	cli.reader.Reset(conn)
	cli.writer.Reset(conn)
}

func (cli *Client) send(cmd []byte) error {
	_, err := cli.conn.Write(append(cmd, '\n'))
	return cli.handleWriteError(err)
}

func (cli *Client) handleWriteError(err error) error {
	if err != nil {
		cli.err = err
		return Error{Err: err, Message: "error while writing to bloomd server", ShouldRetryWithNewClient: true}
	}
	return nil
}

func (cli *Client) handleReadError(err error) error {
	if err != nil {
		cli.err = err
		return Error{Err: err, Message: "error while reader input from bloomd server", ShouldRetryWithNewClient: true}
	}
	return nil
}

func (cli *Client) read() (string, error) {
	l, err := cli.reader.ReadString('\n')
	if err != nil {
		return l, cli.handleReadError(err)
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

func createUnixSocket(saddr string) (net.Conn, error) {
	addr, err := net.ResolveUnixAddr("unix", saddr)
	if err != nil {
		return nil, Error{Message: "error: can't resolve unix domain socket address", Err: err}
	}
	conn, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		return nil, Error{Message: "error: could not create socket", Err: err}
	}
	return conn, nil
}

func createTCPSocket(saddr string) (net.Conn, error) {
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
