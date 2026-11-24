package chord

import (
	"math/big"
	"testing"
)

func TestIntervals(t *testing.T) {
	b := big.NewInt
	cases := []struct {
		x, a, b        int64
		open, halfOpen bool
	}{
		{5, 1, 10, true, true},
		{10, 1, 10, false, true},
		{1, 1, 10, false, false},
		{12, 10, 3, true, true},
		{2, 10, 3, true, true},
		{3, 10, 3, false, true},
		{5, 10, 3, false, false},
		{7, 4, 4, true, true},
		{4, 4, 4, false, true},
	}
	for _, c := range cases {
		if got := inOpen(b(c.x), b(c.a), b(c.b)); got != c.open {
			t.Errorf("inOpen(%d,%d,%d)=%v want %v", c.x, c.a, c.b, got, c.open)
		}
		if got := inHalfOpen(b(c.x), b(c.a), b(c.b)); got != c.halfOpen {
			t.Errorf("inHalfOpen(%d,%d,%d)=%v want %v", c.x, c.a, c.b, got, c.halfOpen)
		}
	}
}

func TestFingerStartWraps(t *testing.T) {
	maxIndex := new(big.Int).Sub(ringSize, big.NewInt(1))
	if got := fingerStart(maxIndex, 0); got.Sign() != 0 {
		t.Errorf("expected wrap to 0, got %v", got)
	}
}
