package cost_models

import "math"

type ExpectedSearchIterationsAndShiftsAccumulator struct {
	cumulativeLogError    float64
	lastPosition          int
	denseRegionStartIndex int
	numExpectedShifts     int
	count                 int
	dataCapacity          int
}

func (e *ExpectedSearchIterationsAndShiftsAccumulator) Accumulate(actualPosition int, expectedPosition int, logError float64) {
	e.cumulativeLogError += math.Log2(math.Abs(float64(expectedPosition-actualPosition) + 1))
	if actualPosition > e.lastPosition {
		e.numExpectedShifts++
		denseRegionLength := e.lastPosition - e.denseRegionStartIndex + 1
		e.numExpectedShifts += (denseRegionLength * denseRegionLength) / 4
		e.denseRegionStartIndex = actualPosition
	}
	e.lastPosition = actualPosition + 1
	e.count++
}

func (e *ExpectedSearchIterationsAndShiftsAccumulator) GetStats() float64 {
	panic("should not be called")
}

func (e *ExpectedSearchIterationsAndShiftsAccumulator) Reset() {
	e.cumulativeLogError = 0.0
	e.lastPosition = 0
	e.denseRegionStartIndex = 0
	e.numExpectedShifts = 0
	e.count = 0
}

func (e *ExpectedSearchIterationsAndShiftsAccumulator) GetExpectedNumSearchIterations() float64 {
	if e.count == 0 {
		return 0.0
	}
	return e.cumulativeLogError / float64(e.count)
}

func (e *ExpectedSearchIterationsAndShiftsAccumulator) GetExpectedNumShifts() float64 {
	if e.count == 0 {
		return 0.0
	}
	denseRegionLength := e.lastPosition - e.denseRegionStartIndex + 1
	currentNumExpectedShifts := e.numExpectedShifts + (denseRegionLength*denseRegionLength)/4
	return float64(currentNumExpectedShifts) / float64(e.count)
}

func NewExpectedSearchIterationsAndShiftsAccumulator(dataCapacity int) *ExpectedSearchIterationsAndShiftsAccumulator {
	return &ExpectedSearchIterationsAndShiftsAccumulator{
		cumulativeLogError:    0.0,
		lastPosition:          0,
		denseRegionStartIndex: 0,
		numExpectedShifts:     0,
		count:                 0,
		dataCapacity:          dataCapacity,
	}
}
