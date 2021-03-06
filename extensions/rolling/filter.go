package rolling

import (
	"context"
	"fmt"
	"time"

	bloomd "github.com/Applifier/go-bloomd"
	"github.com/Applifier/go-bloomd/extensions/aggregation"
	"github.com/Applifier/go-bloomd/extensions/input"
	"github.com/Applifier/go-bloomd/utils/clock"
)

const (
	// RollDaily roll filter every day
	RollDaily = clock.DayUnit
	// RollWeekly roll filter every week
	RollWeekly = clock.WeekUnit
	// RollMonthly roll filter every month
	RollMonthly = clock.MonthUnit
)

// Namer maps filter names to units
type Namer interface {
	NameFor(unit clock.UnitNum) string
	ParseUnit(name string) (clock.UnitNum, error)
}

// Filter provides functionality of working with multiple sequential filters through time
type Filter struct {
	namer    Namer
	period   clock.UnitNum
	unit     clock.Unit
	currUnit func() clock.UnitNum
}

var currUnitMap = map[clock.Unit]func() clock.UnitNum{
	RollDaily:   clock.DayNum,
	RollMonthly: clock.MonthNum,
	RollWeekly:  clock.WeekNum,
}

// NewFilter creates a new Filter
// unit - unit of time, e.g. Week, Day or Month. All keys being set during period with a same unit will be stored in a single filter.
// period - period in specified time units to consider in check operations
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

// BulkSet sets keys to filter that corresponds to a lates unit
// note that it does not check if filter exists
func (rf *Filter) BulkSet(ctx context.Context, cli *bloomd.Client, reader bloomd.KeyReader) (bloomd.ResultReader, error) {
	currUnit := rf.currUnit()
	f := cli.GetFilter(rf.nameForUnit(currUnit))
	return f.BulkSet(reader)
}

// MultiCheck sequentially checks filters through period
// note that it does not check if filters exist
func (rf *Filter) MultiCheck(ctx context.Context, cli *bloomd.Client, rr input.KeyReaderReseter) (resultReader bloomd.ResultReader, err error) {
	deadline, checkDeadline := ctx.Deadline()
	currUnit := rf.currUnit()
	var rs *aggregation.ResultsSet
	defer func() {
		if err != nil && rs != nil {
			rs.Close()
		}
	}()
	for i := clock.UnitZero; i < rf.period; i++ {
		if checkDeadline && deadline.Before(time.Now()) {
			return nil, context.DeadlineExceeded
		}
		f := cli.GetFilter(rf.nameForUnit(currUnit - i))
		reader, err := f.MultiCheck(rr)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			rs = aggregation.GetResultSet(reader.Length())
			if err = rs.FillFromReader(reader); err != nil {
				return nil, err
			}
		} else {
			if err = rs.MergeFromReader(reader); err != nil {
				return nil, err
			}
		}
		rr.Reset()
	}
	return rs, nil
}

// Set sets key to filter that corresponds to a lates unit
// note that it does not check if filter exists
func (rf *Filter) Set(ctx context.Context, cli *bloomd.Client, k bloomd.Key) (bool, error) {
	currUnit := rf.currUnit()
	f := cli.GetFilter(rf.nameForUnit(currUnit))
	return f.Set(k)
}

// Check sequentially checks filters through period
// note that it does not check if filters exist
func (rf *Filter) Check(ctx context.Context, cli *bloomd.Client, k bloomd.Key) (bool, error) {
	deadline, checkDeadline := ctx.Deadline()
	currUnit := rf.currUnit()
	for i := clock.UnitZero; i < rf.period; i++ {
		if checkDeadline && deadline.Before(time.Now()) {
			return false, context.DeadlineExceeded
		}
		f := cli.GetFilter(rf.nameForUnit(currUnit - i))
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
