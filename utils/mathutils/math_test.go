package mathutils

import "testing"

func TestMinInt(t *testing.T) {
	if MinInt(10, 3) != 3 {
		t.Error("Should be 3")
	}
}
