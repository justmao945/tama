package bit

import "testing"

func TestPowerOfTwo(t *testing.T) {
	if !PowerOfTwo(0) {
		t.Error(0)
	}
	if !PowerOfTwo(1) {
		t.Error(1)
	}
	for i := 1; i < 64; i++ {
		v := uint64(1 << uint(i))
		if !PowerOfTwo(v) {
			t.Error(v)
		}
		v++
		if PowerOfTwo(v) {
			t.Error(v)
		}
	}
}

func TestCountBitSet(t *testing.T) {
	type VN struct {
		v uint64
		n int
	}
	data := []VN{
		VN{0, 0},
		VN{0x1, 1},
		VN{0x11, 2},
		VN{0x111, 3},
		VN{0x1111, 4},
		VN{0x11111, 5},
		VN{0x1111111111111111, 16},
	}
	for _, x := range data {
		n := CountBitSet(x.v)
		if n != x.n {
			t.Error(n, x)
		}
	}
}

func TestRoundUpToPowerOfTwo(t *testing.T) {
	type VT struct {
		v uint64
		t uint64
	}

	data := []VT{
		VT{0, 0},
		VT{1, 1},
		VT{2, 2},
		VT{16, 16},
		VT{17, 32},
		VT{31, 32},
		VT{(1 << 28) - 3, 1 << 28},
		VT{(1 << 59) - 3, 1 << 59},
	}

	for _, x := range data {
		t1 := RoundUpToPowerOfTwo(x.v)
		if t1 != x.t {
			t.Error(t1, x)
		}
	}
}
