package cost_models

type ExpectedShiftsAccumulator struct {
	lastPosition          int
	denseRegionStartIndex int
	numExpectedShifts     int
	count                 int
	dataCapacity          int
}

// A dense region of n keys will contribute a total number of expected shifts
// of approximately
// ((n-1)/2)((n-1)/2 + 1) = n^2/4 - 1/4
// This is exact for odd n and off by 0.25 for even n.
// Therefore, we track n^2/4.
func (e *ExpectedShiftsAccumulator) Accumulate(actualPosition int, expectedPosition int, logError float64) {
	if actualPosition > e.lastPosition+1 {
		denseRegionLength := e.lastPosition - e.denseRegionStartIndex + 1
		e.numExpectedShifts += (denseRegionLength * denseRegionLength) / 4
		e.denseRegionStartIndex = actualPosition
	}
	e.lastPosition = actualPosition
	e.count++
}

func (e *ExpectedShiftsAccumulator) GetStats() float64 {
	if e.count == 0 {
		return 0.0
	}
	denseRegionLength := e.lastPosition - e.denseRegionStartIndex + 1
	currentNumExpectedShifts := e.numExpectedShifts + (denseRegionLength*denseRegionLength)/4
	return float64(currentNumExpectedShifts) / float64(e.count)
}

func (e *ExpectedShiftsAccumulator) Reset() {
	e.lastPosition = -1
	e.denseRegionStartIndex = 0
	e.numExpectedShifts = 0
	e.count = 0
}

func NewExpectedShiftsAccumulator(dataCapacity int) *ExpectedShiftsAccumulator {
	return &ExpectedShiftsAccumulator{
		lastPosition:          -1,
		denseRegionStartIndex: 0,
		numExpectedShifts:     0,
		count:                 0,
		dataCapacity:          dataCapacity,
	}
}
