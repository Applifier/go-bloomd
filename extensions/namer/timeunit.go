package namer

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"

	"github.com/Applifier/go-bloomd/utils/clock"
)

// TimeUnitNamer is an namer optimized for lower memory allocations
// it uses rw cache underline
type TimeUnitNamer struct {
	cache  map[clock.UnitNum]string
	prefix string
	m      sync.RWMutex
}

var validPrefix = regexp.MustCompile(`^[a-z_0-9]+$`)

// MustNewTimeUnitNamer creates a new namer with names cache and panics if input parameters are invalid
func MustNewTimeUnitNamer(prefix string, unit clock.Unit) *TimeUnitNamer {
	namer, err := NewTimeUnitNamer(prefix, unit)
	if err != nil {
		panic(err)
	}
	return namer
}

// NewTimeUnitNamer creates a new namer with names cache
// prefix is expected to have only letters in lower case, digits or underscore and shouldn't be empty
// if inputs are invalid error is returned
func NewTimeUnitNamer(prefix string, unit clock.Unit) (*TimeUnitNamer, error) {
	if !validPrefix.MatchString(prefix) {
		return nil, errors.New("TimeUnitNamer.NewTimeUnitNamer: prefix is expected to have only letters in lower case, digits or underscore and shouldn't be empty")
	}
	if err := clock.ValidUnit(unit); err != nil {
		return nil, err
	}
	return &TimeUnitNamer{
		cache:  make(map[clock.UnitNum]string),
		prefix: prefix + "-" + string(unit),
		m:      sync.RWMutex{},
	}, nil
}

// NameFor resolves name for specified unit
// it checks and updates cache if corresponding name were not found
func (nr *TimeUnitNamer) NameFor(unit clock.UnitNum) string {
	name, ok := nr.readCache(unit)
	if !ok {
		name = fmt.Sprintf("%s%d", nr.prefix, unit)
		nr.writeCache(unit, name)
	}
	return name
}

// ParseUnit attempts tp resolve unit from provided filter name
func (nr *TimeUnitNamer) ParseUnit(name string) (clock.UnitNum, error) {
	if len(name) > len(nr.prefix) && name[:len(nr.prefix)] == nr.prefix {
		unit, err := strconv.Atoi(name[len(nr.prefix):])
		if err == nil {
			return clock.UnitNum(unit), nil
		}
		return clock.UnitZero, err
	}
	return clock.UnitZero, fmt.Errorf("name expected to have prefix %s", nr.prefix)
}

func (nr *TimeUnitNamer) readCache(unit clock.UnitNum) (string, bool) {
	nr.m.RLock()
	defer nr.m.RUnlock()
	name, ok := nr.cache[unit]
	return name, ok
}

func (nr *TimeUnitNamer) writeCache(unit clock.UnitNum, name string) {
	nr.m.Lock()
	defer nr.m.Unlock()
	nr.cache[unit] = name
}
