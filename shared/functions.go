package shared

func Pow2RoundUp(x int) int {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

func Log2RoundDown(x int) int {
	var result int
	for x > 1 {
		x >>= 1
		result++
	}
	return result
}
