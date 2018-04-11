package rolling

import (
	"fmt"
	"strconv"
	"sync"
)

type Namer interface {
	NameFor(unit int) string
	ParseUnit(name string) (int, bool)
}

func NewNamer(prefix string, unit string) Namer {
	return &namer{
		cache:  make(map[int]string),
		prefix: prefix + "-" + unit,
		m:      sync.RWMutex{},
	}
}

type namer struct {
	cache  map[int]string
	prefix string
	m      sync.RWMutex
}

func (nr *namer) NameFor(unit int) string {
	name, ok := nr.readCache(unit)
	if !ok {
		name = fmt.Sprintf("%s%d", nr.prefix, unit)
		nr.writeCache(unit, name)
	}
	return name
}

func (nr *namer) ParseUnit(name string) (int, bool) {
	if len(name) > len(nr.prefix) && name[:len(nr.prefix)] == nr.prefix {
		unit, err := strconv.Atoi(name[len(nr.prefix):])
		if err == nil {
			return unit, true
		}
	}
	return 0, false
}

func (nr *namer) readCache(unit int) (string, bool) {
	nr.m.RLock()
	defer nr.m.RUnlock()
	name, ok := nr.cache[unit]
	return name, ok
}

func (nr *namer) writeCache(unit int, name string) {
	nr.m.Lock()
	defer nr.m.Unlock()
	nr.cache[unit] = name
}
