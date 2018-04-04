package bloomd

import (
	"net"
)

type ResultReader interface {
	Next() (bool, error)
}

type NewClient interface {
	Close() error
	Flush() error
	Check(key string) (bool, error)
	MultiCheck(keys []string) (ResultReader, error)
	Set(key string) (bool, error)
	BulkSet(keys []string) (ResultReader, error)
	GetFilterInfo() (FilterInfo, error)
	ListFilters(prefix string) ([]FilterInfo, error)
	CreateFilter(name string, capacity int, prob float64, inMemory bool) error
	FlushFilter(name string) error
	CloseFilter(name string) error
	ClearFilter(name string) error
	DropFilter(name string) error
}

type FilterInfo struct {
	Name     string
	Capacity int
	Prob     float64
	inMemory bool
}

type client struct {
	pipeFactory func() (pipe, error)
}

type pipe struct {
	conn     *net.Conn
	isBroken bool
}

func (p pipe) write(bytes []byte) error {
	_, err := p.conn.Write()

}
