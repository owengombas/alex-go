package cost_models

type Accumulator interface {
	Accumulate(actualPosition int, expectedPosition int, logError float64)
	GetStats() float64
	Reset()
}
