package aggregation

import (
	"errors"
	"testing"
)

func TestResultsSet(t *testing.T) {
	rs := GetResultSet(3)
	resultsMock1 := newResultsReaderMock(true, false, false)

	err := rs.FillFromReader(resultsMock1)
	if err != nil {
		t.Fatal(err)
	}

	resultsMock2 := newResultsReaderMock(false, false, true)

	err = rs.MergeFromReader(resultsMock2)
	if err != nil {
		t.Fatal(err)
	}

	var result [3]bool
	_, err = rs.Read(result[:])
	if err != nil {
		t.Fatal(err)
	}

	expected := []bool{true, false, true}
	for i := 0; i < len(expected); i++ {
		if result[i] != expected[i] {
			t.Errorf("expected %d elemnet of array to be %v but was %v", i, expected[i], result[i])
		}
	}
}

func newResultsReaderMock(values ...bool) *resultsReaderMock {
	return &resultsReaderMock{
		arr:   values,
		index: 0,
	}
}

type resultsReaderMock struct {
	index int
	arr   []bool
}

func (rrm *resultsReaderMock) Next() (bool, error) {
	if rrm.index >= len(rrm.arr) {
		return false, errors.New("index out of length")
	}
	result := rrm.arr[rrm.index]
	rrm.index++
	return result, nil
}

func (rrm *resultsReaderMock) Read(p []bool) (n int, err error) {
	return copy(p, rrm.arr), nil
}

func (rrm *resultsReaderMock) Length() int {
	return len(rrm.arr)
}

func (rrm *resultsReaderMock) Close() error {
	return nil
}
