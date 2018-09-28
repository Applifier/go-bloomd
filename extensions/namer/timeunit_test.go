package namer

import (
	"testing"

	"github.com/Applifier/go-bloomd/utils/clock"
)

func TestTimeUnitNamer(t *testing.T) {
	t.Run("NameFor", func(t *testing.T) {
		t.Run("should return expected name for provided time unit number", func(t *testing.T) {
			for i := 0; i < 2; i++ { // just to test that cache works well
				prefix := "test"
				tests := []struct {
					unit           clock.Unit
					unitNum        clock.UnitNum
					expectedResult string
				}{
					{
						unit:           clock.WeekUnit,
						unitNum:        10,
						expectedResult: "test-w10",
					},
					{
						unit:           clock.MonthUnit,
						unitNum:        0,
						expectedResult: "test-m0",
					},
					{
						unit:           clock.DayUnit,
						unitNum:        14,
						expectedResult: "test-d14",
					},
				}
				for _, ts := range tests {
					namer, err := NewTimeUnitNamer(prefix, ts.unit)
					if err != nil {
						t.Fatal(err)
					}
					name := namer.NameFor(ts.unitNum)
					if name != ts.expectedResult {
						t.Errorf("Result expected to be %s but was %s", ts.expectedResult, name)
					}
				}
			}
		})
	})

	t.Run("ParseUnit", func(t *testing.T) {
		prefix := "test"
		t.Run("should parse provided name for expected unit", func(t *testing.T) {
			tests := []struct {
				unit            clock.Unit
				inputString     string
				expectedUnitNum clock.UnitNum
			}{
				{
					unit:            clock.WeekUnit,
					inputString:     "test-w10",
					expectedUnitNum: 10,
				},
				{
					unit:            clock.MonthUnit,
					inputString:     "test-m0",
					expectedUnitNum: 0,
				},
				{
					unit:            clock.DayUnit,
					inputString:     "test-d14",
					expectedUnitNum: 14,
				},
			}
			for _, ts := range tests {
				namer, err := NewTimeUnitNamer(prefix, ts.unit)
				if err != nil {
					t.Fatal(err)
				}
				n, err := namer.ParseUnit(ts.inputString)
				if err != nil {
					t.Fatal(err)
				}
				if n != ts.expectedUnitNum {
					t.Errorf("Result expected to be %d but was %d", ts.expectedUnitNum, n)
				}
			}
		})

		t.Run("should return error on unexpected input", func(t *testing.T) {
			unit := clock.WeekUnit
			tests := []struct {
				inputString string
			}{
				{
					inputString: "longstring-w10",
				},
				{
					inputString: "test-m5",
				},
			}
			for _, ts := range tests {
				namer, err := NewTimeUnitNamer(prefix, unit)
				if err != nil {
					t.Fatal(err)
				}
				_, err = namer.ParseUnit(ts.inputString)
				if err == nil {
					t.Fatal("ParseUnit is expected to return error")
				}
			}
		})
	})
}
