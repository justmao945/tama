package bit

// PowerOfTwo returns true if v = 2^x or v is 0.
func PowerOfTwo(v uint64) bool {
	return v&(v-1) == 0
}

// CountBitSet returns the number 1 in v.
func CountBitSet(v uint64) (n int) {
	for n = 0; v != 0; n++ {
		v &= v - 1
	}
	return
}

// RoundUpToPowerOfTwo returns the next highest power of 2 or 0 if v is 0.
func RoundUpToPowerOfTwo(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}
