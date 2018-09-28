package conveyor

import (
	"context"
	"fmt"
	"time"

	bloomd "github.com/Applifier/go-bloomd"
	"github.com/Applifier/go-bloomd/extensions/input"
	"github.com/Applifier/go-bloomd/utils/clock"
)

// Namer maps filter names to units
type Namer interface {
	NameFor(unit clock.UnitNum) string
	ParseUnit(name string) (clock.UnitNum, error)
}

const (
	// ShiftDaily roll filter every day
	ShiftDaily = clock.Unit("d")
	// ShiftWeekly roll filter every week
	ShiftWeekly = clock.Unit("w")
	// ShiftMonthly roll filter every month
	ShiftMonthly = clock.Unit("m")
)

// Filter provides fuctionality of working with multiple sequential filters through time
type Filter struct {
	namer    Namer
	period   clock.UnitNum
	unit     clock.Unit
	currUnit func() clock.UnitNum
}

var currUnitMap = map[clock.Unit]func() clock.UnitNum{
	ShiftDaily:   clock.DayNum,
	ShiftWeekly:  clock.WeekNum,
	ShiftMonthly: clock.MonthNum,
}

// NewFilter creates a new Filter
// unit - unit of time, e.g. Week, Day or Month. All keys being set during period with a same unit will be stored in a single filter.
// period - period in specified time units to consider in set operations
// namer - provides algorithm to name filters according to specified unit of time
func NewFilter(namer Namer, unit clock.Unit, period clock.UnitNum) (*Filter, error) {
	currUnitFunc, ok := currUnitMap[unit]
	if !ok {
		return nil, fmt.Errorf("Unit %s does not supported", unit)
	}
	if period >= 100 { // TODO there should be tests to find a good max period
		return nil, fmt.Errorf("Too wide period")
	}
	return &Filter{
		namer:    namer,
		unit:     unit,
		period:   period,
		currUnit: currUnitFunc,
	}, nil
}

// BulkSet sets keys to all filters within the configured period
// it returns result for oldest filter
// note that it does not check if filter exists
func (rf *Filter) BulkSet(ctx context.Context, cli *bloomd.Client, rr input.KeyReaderReseter) (results bloomd.ResultReader, err error) {
	deadline, checkDeadline := ctx.Deadline()
	currUnit := rf.currUnit()
	for i := clock.UnitZero; i < rf.period; i++ {
		if checkDeadline && deadline.Before(time.Now()) {
			return nil, context.DeadlineExceeded
		}
		f := cli.GetFilter(rf.nameForUnit(currUnit - i))
		reader, err := f.BulkSet(rr)
		if err != nil {
			return nil, err
		}
		// return bulk set result for oldest unit
		if i == rf.period-1 {
			results = reader
		} else {
			reader.Close()
			rr.Reset()
		}
	}
	return results, err
}

// MultiCheck checks keys in the oldest filter
// note that it does not check if filters exist
func (rf *Filter) MultiCheck(ctx context.Context, cli *bloomd.Client, reader bloomd.KeyReader) (resultReader bloomd.ResultReader, err error) {
	currUnit := rf.currUnit()
	oldestUnit := currUnit - rf.period + 1
	f := cli.GetFilter(rf.nameForUnit(oldestUnit))
	return f.MultiCheck(reader)
}

// Set sets key to all filters within the configured period
// returns result for set into oldest filter
// note that it does not check if filter exists
func (rf *Filter) Set(ctx context.Context, cli *bloomd.Client, k bloomd.Key) (result bool, err error) {
	deadline, checkDeadline := ctx.Deadline()
	currUnit := rf.currUnit()
	for i := clock.UnitZero; i < rf.period; i++ {
		if checkDeadline && deadline.Before(time.Now()) {
			return false, context.DeadlineExceeded
		}
		fName := rf.nameForUnit(currUnit - i)
		f := cli.GetFilter(fName)
		// result will contain set result for the oldest filter
		result, err = f.Set(k)
		if err != nil {
			return false, err
		}
	}
	return result, err
}

// Check sequentially checks filters through period
// note that it does not check if filters exist
func (rf *Filter) Check(ctx context.Context, cli *bloomd.Client, k bloomd.Key) (bool, error) {
	currUnit := rf.currUnit()
	oldestUnit := currUnit - rf.period + 1
	f := cli.GetFilter(rf.nameForUnit(oldestUnit))
	return f.Check(k)
}

// Drop drops all filters through period
func (rf *Filter) Drop(ctx context.Context, cli *bloomd.Client) error {
	return rf.executeForAllFilters(ctx, cli, func(f bloomd.Filter) error {
		return f.Drop()
	})
}

// Close closes all filters through period
func (rf *Filter) Close(ctx context.Context, cli *bloomd.Client) error {
	return rf.executeForAllFilters(ctx, cli, func(f bloomd.Filter) error {
		return f.Close()
	})
}

// Clear clears all filters through period
func (rf *Filter) Clear(ctx context.Context, cli *bloomd.Client) error {
	return rf.executeForAllFilters(ctx, cli, func(f bloomd.Filter) error {
		return f.Clear()
	})
}

// Flush flushes all filters through period
func (rf *Filter) Flush(ctx context.Context, cli *bloomd.Client) error {
	return rf.executeForAllFilters(ctx, cli, func(f bloomd.Filter) error {
		return f.Flush()
	})
}

// CreateFilters creates all filters through a specified period
// if advance is greater than 0 that it will preallocate filters
func (rf *Filter) CreateFilters(ctx context.Context, cli *bloomd.Client, advance clock.UnitNum, capacity int, prob float64, inMemory bool) error {
	deadline, checkDeadline := ctx.Deadline()
	if advance < 0 {
		advance = 0
	}
	currUnit := rf.currUnit()
	from := currUnit - rf.period + 1
	to := currUnit + advance
	for i := from; i <= to; i++ {
		now := time.Now()
		if checkDeadline && deadline.Before(now) {
			return context.DeadlineExceeded
		}
		name := rf.nameForUnit(i)
		_, err := cli.CreateFilter(name, capacity, prob, inMemory)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropOlderFilters drops all filters that correspond to units older that period
// if tail is greater that 0 that it will preserve some old filters
func (rf *Filter) DropOlderFilters(ctx context.Context, cli *bloomd.Client, tail clock.UnitNum) error {
	deadline, checkDeadline := ctx.Deadline()
	if tail < 0 {
		tail = 0
	}
	filters, err := rf.findFilters(ctx, cli)
	if err != nil {
		return err
	}
	minUnit := rf.currUnit() - rf.period - tail
	for _, f := range filters {
		if checkDeadline && deadline.Before(time.Now()) {
			return context.DeadlineExceeded
		}
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

func (rf *Filter) executeForAllFilters(ctx context.Context, cli *bloomd.Client, filterOp func(f bloomd.Filter) error) error {
	deadline, checkDeadline := ctx.Deadline()
	fs, err := rf.findFilters(ctx, cli)
	if err != nil {
		return err
	}
	for _, f := range fs {
		if checkDeadline && deadline.Before(time.Now()) {
			return context.DeadlineExceeded
		}
		if err = filterOp(f.filter); err != nil {
			return err
		}
	}
	return nil
}

func (rf *Filter) findFilters(ctx context.Context, cli *bloomd.Client) ([]unitFilter, error) {
	fs, err := cli.ListFilters()
	if err != nil {
		return nil, err
	}
	result := make([]unitFilter, 0, len(fs))
	for _, f := range fs {
		unit, err := rf.namer.ParseUnit(f.Name)
		// just skip if filter is not eligible
		if err == nil {
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

func (rf *Filter) nameForUnit(unit clock.UnitNum) string {
	return rf.namer.NameFor(unit)
}
