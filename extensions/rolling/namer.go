package rolling

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/Applifier/go-bloomd/utils/clock"
)

// Namer maps filter names to units
type Namer interface {
	NameFor(unit clock.UnitNum) string
	ParseUnit(name string) (clock.UnitNum, bool)
}

// NewNamer creates a new namer with names cache
func NewNamer(prefix string, unit clock.Unit) Namer {
	return &namer{
		cache:  make(map[clock.UnitNum]string),
		prefix: prefix + "-" + string(unit),
		m:      sync.RWMutex{},
	}
}

type namer struct {
	cache  map[clock.UnitNum]string
	prefix string
	m      sync.RWMutex
}

// NameFor resolves name for specified unit
// it checks and updates cache if corresponding name were not found
func (nr *namer) NameFor(unit clock.UnitNum) string {
	name, ok := nr.readCache(unit)
	if !ok {
		name = fmt.Sprintf("%s%d", nr.prefix, unit)
		nr.writeCache(unit, name)
	}
	return name
}

// ParseUnit attemts tp resolve unit from provided filter name
func (nr *namer) ParseUnit(name string) (clock.UnitNum, bool) {
	if len(name) > len(nr.prefix) && name[:len(nr.prefix)] == nr.prefix {
		unit, err := strconv.Atoi(name[len(nr.prefix):])
		if err == nil {
			return clock.UnitNum(unit), true
		}
	}
	return clock.UnitZero, false
}

func (nr *namer) readCache(unit clock.UnitNum) (string, bool) {
	nr.m.RLock()
	defer nr.m.RUnlock()
	name, ok := nr.cache[unit]
	return name, ok
}

func (nr *namer) writeCache(unit clock.UnitNum, name string) {
	nr.m.Lock()
	defer nr.m.Unlock()
	nr.cache[unit] = name
}
