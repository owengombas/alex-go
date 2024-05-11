package cost_models

type Accumulator interface {
	Accumulate(actualPosition int, expectedPosition int)
	GetStats() float64
	Reset()
}
