package rolling

import (
	bloomd "github.com/Applifier/go-bloomd"
	"github.com/Applifier/go-bloomd/utils/clock"
)

const (
	// RollDaily roll filter every day
	RollDaily = clock.Unit("d")
	// RollWeekly roll filter every week
	RollWeekly = clock.Unit("w")
	// RollMonthly roll filter every month
	RollMonthly = clock.Unit("m")
)

// Filter provides fuctionality of working with multiple sequential filters through time
type Filter struct {
	namer  Namer
	period clock.UnitNum
	unit   clock.Unit
	client *bloomd.Client
	rs     resultsSet
}

// NewFilter creates a new Filter
// unit - unit of time, e.g. Week, Day or Month. All keys being set during period with a same unit will be stored in a single filter.
// period - period in specified time units to consider in check operations
// namer - provides algorithm to name filters according to specified unit of time
func NewFilter(namer Namer, unit clock.Unit, period clock.UnitNum, client *bloomd.Client) *Filter {
	return &Filter{
		namer:  namer,
		unit:   unit,
		period: period,
		client: client,
		rs:     newResultSet(),
	}
}

// BulkSet sets keys to filter that corresponds to a lates unit
// note that it does not check if filter exists
func (rf *Filter) BulkSet(ks *bloomd.KeySet) (bloomd.ResultReader, error) {
	currUnit := rf.currUnit()
	f := rf.client.GetFilter(rf.nameForUnit(currUnit))
	return f.BulkSet(ks)
}

// MultiCheck sequentially checks filters through period
// note that it does not check if filters exist
func (rf *Filter) MultiCheck(ks *bloomd.KeySet) (bloomd.ResultReader, error) {
	currUnit := rf.currUnit()
	rf.rs.reset(ks.Length())
	for i := clock.UnitZero; i < rf.period; i++ {
		f := rf.client.GetFilter(rf.nameForUnit(currUnit - i))
		reader, err := f.MultiCheck(ks)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			if err = rf.rs.fillFromReader(reader); err != nil {
				return nil, err
			}
		} else {
			if err = rf.rs.mergeFromReader(reader); err != nil {
				return nil, err
			}
		}
	}
	return &rf.rs, nil
}

// Set sets key to filter that corresponds to a lates unit
// note that it does not check if filter exists
func (rf *Filter) Set(k bloomd.Key) (bool, error) {
	currUnit := rf.currUnit()
	f := rf.client.GetFilter(rf.nameForUnit(currUnit))
	return f.Set(k)
}

// Check sequentially checks filters through period
// note that it does not check if filters exist
func (rf *Filter) Check(k bloomd.Key) (bool, error) {
	currUnit := rf.currUnit()
	for i := clock.UnitZero; i < rf.period; i++ {
		f := rf.client.GetFilter(rf.nameForUnit(currUnit - i))
		val, err := f.Check(k)
		if err != nil {
			return false, err
		}
		if val {
			return true, nil
		}
	}
	return false, nil
}

// Drop drops all filters through period
func (rf *Filter) Drop() error {
	return rf.executeForAllFilters(func(f bloomd.Filter) error {
		return f.Drop()
	})
}

// Close closes all filters through period
func (rf *Filter) Close() error {
	return rf.executeForAllFilters(func(f bloomd.Filter) error {
		return f.Close()
	})
}

// Clear clears all filters through period
func (rf *Filter) Clear() error {
	return rf.executeForAllFilters(func(f bloomd.Filter) error {
		return f.Clear()
	})
}

// Flush flushes all filters through period
func (rf *Filter) Flush() error {
	return rf.executeForAllFilters(func(f bloomd.Filter) error {
		return f.Flush()
	})
}

// CreateFilters creates all filters through a specified period
// if advance is greater than 0 that it will preallocate filters
func (rf *Filter) CreateFilters(advance clock.UnitNum, capacity int, prob float64, inMemory bool) error {
	if advance < 0 {
		advance = 0
	}
	currUnit := rf.currUnit()
	from := currUnit - rf.period + 1
	to := currUnit + advance
	for i := from; i <= to; i++ {
		name := rf.nameForUnit(i)
		_, err := rf.client.CreateFilter(name, capacity, prob, inMemory)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropOlderFilters drops all filters that correspond to units older that period
// if tail is greater that 0 that it will preserve some old filters
func (rf *Filter) DropOlderFilters(tail clock.UnitNum) error {
	if tail < 0 {
		tail = 0
	}
	filters, err := rf.findFilters()
	if err != nil {
		return err
	}
	minUnit := rf.currUnit() - rf.period - tail
	for _, f := range filters {
		if err != nil {
			return err
		}
		if f.unit <= minUnit {
			err = f.filter.Drop()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (rf *Filter) executeForAllFilters(filterOp func(f bloomd.Filter) error) error {
	fs, err := rf.findFilters()
	if err != nil {
		return err
	}
	for _, f := range fs {
		if err = filterOp(f.filter); err != nil {
			return err
		}
	}
	return nil
}

func (rf *Filter) findFilters() ([]unitFilter, error) {
	fs, err := rf.client.ListFilters()
	if err != nil {
		return nil, err
	}
	result := make([]unitFilter, 0, len(fs))
	for _, f := range fs {
		unit, ok := rf.namer.ParseUnit(f.Name)
		if ok {
			result = append(result, unitFilter{
				filter: f,
				unit:   unit,
			})
		}
	}
	return result, nil
}

type unitFilter struct {
	filter bloomd.Filter
	unit   clock.UnitNum
}

func (rf *Filter) currUnit() clock.UnitNum {
	switch rf.unit {
	case RollDaily:
		return clock.DayNum()
	case RollWeekly:
		return clock.WeekNum()
	case RollMonthly:
		return clock.MonthNum()
	}
	return 0
}

func (rf *Filter) nameForUnit(unit clock.UnitNum) string {
	return rf.namer.NameFor(unit)
}
