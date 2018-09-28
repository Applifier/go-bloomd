package clock

import (
	"fmt"
	"time"

	"github.com/Applifier/go-bloomd/utils/period"
)

var now = time.Now

// Now returns clocks now
func Now() time.Time {
	return now()
}

// Reset resets now function to get real current time
func Reset() {
	now = time.Now
}

// Custom replaces a current time function with custome f for clocks now
func Custom(f func() time.Time) {
	now = f
}

// Static fixes clocks now function to return value t
func Static(t time.Time) {
	now = func() time.Time {
		return t
	}
}

// Unit represents a unit of time, like day, hour, year and etc.
type Unit string

const (
	// DayUnit is a unit code for day
	DayUnit = Unit("d")
	// WeekUnit is a unit code for week
	WeekUnit = Unit("w")
	// MonthUnit is a unit code for month
	MonthUnit = Unit("m")
)

func ValidUnit(unit Unit) error {
	switch unit {
	case DayUnit, WeekUnit, MonthUnit:
		return nil
	default:
		return fmt.Errorf("Unit %s is unknown", unit)
	}
}

// UnitNum is a value of particular Unit from the zero unix time, for example 3d week or 2nd year
type UnitNum uint32

// UnitZero is zero UnitNum
const UnitZero = UnitNum(0)

// WeekNum return current week number
func WeekNum() UnitNum {
	return WeekNumOf(Now())
}

// MonthNum return current month number
func MonthNum() UnitNum {
	return MonthNumOf(Now())
}

// DayNum return current day number
func DayNum() UnitNum {
	return DayNumOf(Now())
}

// WeekNumOf returns week number of specified time t
func WeekNumOf(t time.Time) UnitNum {
	return (DayNumOf(t)-1)/period.DaysInWeek + 1
}

// MonthNumOf returns month number of specified time t
func MonthNumOf(t time.Time) UnitNum {
	return UnitNum(int(t.Month()) + (t.Year()-1970)*12)
}

// DayNumOf returns day number of specified time t
func DayNumOf(t time.Time) UnitNum {
	return UnitNum(t.UnixNano()/int64(period.Day)) + 1
}
