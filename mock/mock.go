package mock

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
)

// DefaultBufferSize is the default size for the read buffer
var DefaultBufferSize = 4096

// MockServer includes the conn, reader and the mock filters map
type MockServer struct {
	lock    sync.Mutex
	conn    net.Conn
	reader  *bufio.Reader
	filters map[string]map[string]bool
}

// NewMockServer creates and returns a mock server with the supplied connection
func NewMockServer(conn net.Conn) *MockServer {
	return &MockServer{
		lock:    sync.Mutex{},
		conn:    conn,
		reader:  bufio.NewReaderSize(conn, DefaultBufferSize),
		filters: make(map[string]map[string]bool),
	}
}

func (s *MockServer) Filters() map[string]map[string]bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	filters := map[string]map[string]bool{}
	for f, v := range s.filters {
		sub := map[string]bool{}
		for k, t := range v {
			sub[k] = t
		}
		filters[f] = sub
	}
	return filters
}

func (s *MockServer) read() (string, error) {
	l, err := s.reader.ReadString('\n')
	if err != nil {
		return l, err
	}
	return strings.TrimRight(l, "\r\n"), nil
}

func (s *MockServer) send(cmd []byte) error {
	if _, err := s.conn.Write(append(cmd, '\n')); err != nil {
		return err
	}
	return nil
}

// Serve spins up the mock server
func (s *MockServer) Serve() {
	for {
		cmd, err := s.read()
		if err != nil {
			// io.EOF occurs when the client closes the connection
			if err == io.EOF {
				return
			}
			panic(err)
		}
		response := s.handle(cmd)
		if err := s.send([]byte(response)); err != nil {
			panic(err)
		}
	}
}

func (s *MockServer) handle(cmdString string) string {
	tokens := strings.Split(cmdString, " ")
	cmd := tokens[0]
	args := tokens[1:]
	switch cmd {
	case "drop":
		return "Done"
	case "list":
		return "START\nEND"
	case "create":
		return s.createFilter(args[0])
	case "b", "s":
		return s.bulkSet(args[0], args[1:])
	case "m", "c":
		return s.multiCheck(args[0], args[1:])
	default:
		return ""
	}
}

func (s *MockServer) createFilter(name string) string {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, present := s.filters[name]
	if !present {
		s.filters[name] = make(map[string]bool)
	}
	return "Done"
}

func (s *MockServer) bulkSet(filterName string, keys []string) string {
	s.lock.Lock()
	defer s.lock.Unlock()
	var responses []string
	for _, key := range keys {
		s.filters[filterName][key] = true
		// Answer is always "Yes" in this mock for the time being
		responses = append(responses, "Yes")
	}
	return strings.Join(responses, " ")
}

func (s *MockServer) multiCheck(filterName string, keys []string) string {
	s.lock.Lock()
	defer s.lock.Unlock()
	var responses []string
	for _, key := range keys {
		if s.filters[filterName][key] == true {
			responses = append(responses, "Yes")
		} else {
			responses = append(responses, "No")
		}
	}
	return strings.Join(responses, " ")
}
