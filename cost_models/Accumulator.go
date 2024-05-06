package cost_models

type Accumulator interface {
	Accumulate(actualPosition uint64, expectedPosition uint64, logError float64)
	GetStats() float64
	Reset()
}
