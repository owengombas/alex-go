package cost_models

import "math"

type ExpectedSearchIterationsAccumulator struct {
	cumulativeLogError float64
	count              int
}

func (e *ExpectedSearchIterationsAccumulator) Accumulate(actualPosition int, expectedPosition int) {
	e.cumulativeLogError += math.Log2(math.Abs(float64(expectedPosition-actualPosition)) + 1)
	e.count++
}

func (e *ExpectedSearchIterationsAccumulator) GetStats() float64 {
	if e.count == 0 {
		return 0.0
	}
	return e.cumulativeLogError / float64(e.count)
}

func (e *ExpectedSearchIterationsAccumulator) Reset() {
	e.cumulativeLogError = 0.0
	e.count = 0
}

func NewExpectedSearchIterationsAccumulator() *ExpectedSearchIterationsAccumulator {
	return &ExpectedSearchIterationsAccumulator{
		cumulativeLogError: 0.0,
		count:              0,
	}
}
