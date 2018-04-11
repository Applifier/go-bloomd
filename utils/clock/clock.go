package clock

import (
	"time"

	"github.com/Applifier/go-bloomd/utils/period"
)

var now = time.Now

func Now() time.Time {
	return now()
}

func Reset() {
	now = time.Now
}

func Custom(f func() time.Time) {
	now = f
}

func Static(t time.Time) {
	now = func() time.Time {
		return t
	}
}

func WeekNum() int {
	return WeekNumOf(Now())
}

func MonthNum() int {
	return MonthNumOf(Now())
}

func DayNum() int {
	return DayNumOf(Now())
}

func WeekNumOf(t time.Time) int {
	return DayNumOf(t) / period.DaysInWeek
}

func MonthNumOf(t time.Time) int {
	return int(t.Month()) + t.Year()*12
}

func DayNumOf(t time.Time) int {
	return int(t.UnixNano() / int64(period.Day))
}
