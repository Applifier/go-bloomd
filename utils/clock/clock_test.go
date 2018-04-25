package clock

import (
	"testing"
	"time"
)

func TestClock(t *testing.T) {
	t.Run("Now should return current time", func(t *testing.T) {
		if time.Now().After(Now()) {
			t.Error("Should be at least current time")
		}
	})

	t.Run("Custom should set time as function result", func(t *testing.T) {
		Custom(func() time.Time {
			return time.Now().Add(-1 * time.Minute)
		})
		if time.Now().Before(Now()) {
			t.Error("Should before after current time")
		}
	})

	t.Run("Static should set time as static value", func(t *testing.T) {
		dt := time.Date(1999, 10, 23, 11, 34, 5, 10, time.UTC)
		Static(dt)
		if Now() != dt {
			t.Error("Should be exactly time")
		}
	})

	t.Run("Reset should reset to default behaviour", func(t *testing.T) {
		Reset()
		if time.Now().After(Now()) {
			t.Error("Should be at least current time")
		}
	})

	t.Run("WeekNumOf should return proper week", func(t *testing.T) {
		dt := time.Date(1970, 1, 8, 0, 0, 0, 0, time.UTC)
		n := WeekNumOf(dt)
		if n != 2 {
			t.Fatal("Should be 2")
		}
	})

	t.Run("DayNumOf should return proper day", func(t *testing.T) {
		dt := time.Date(1970, 1, 9, 0, 0, 0, 0, time.UTC)
		n := DayNumOf(dt)
		if n != 9 {
			t.Fatalf("Should be 9 but was %d", n)
		}
	})

	t.Run("MonthNumOf should return proper month", func(t *testing.T) {
		dt := time.Date(1970, 2, 9, 0, 0, 0, 0, time.UTC)
		n := MonthNumOf(dt)
		if n != 2 {
			t.Fatalf("Should be 2 but was %d", n)
		}
	})

	t.Run("WeekNum should return proper week", func(t *testing.T) {
		dt := time.Date(1970, 1, 8, 0, 0, 0, 0, time.UTC)
		Static(dt)
		n := WeekNum()
		if n != 2 {
			t.Fatal("Should be 2")
		}
	})

	t.Run("DayNum should return proper day", func(t *testing.T) {
		dt := time.Date(1970, 1, 9, 0, 0, 0, 0, time.UTC)
		Static(dt)
		n := DayNum()
		if n != 9 {
			t.Fatalf("Should be 9 but was %d", n)
		}
	})

	t.Run("MonthNum should return proper month", func(t *testing.T) {
		dt := time.Date(1970, 2, 9, 0, 0, 0, 0, time.UTC)
		Static(dt)
		n := MonthNum()
		if n != 2 {
			t.Fatalf("Should be 2 but was %d", n)
		}
	})
}
