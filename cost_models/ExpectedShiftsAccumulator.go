package cost_models

type ExpectedShiftsAccumulator struct {
	lastPosition          int
	denseRegionStartIndex int
	numExpectedShifts     int
	count                 int
	dataCapacity          int
}

func (e *ExpectedShiftsAccumulator) Accumulate(actualPosition int, expectedPosition int, logError float64) {
	if actualPosition > e.lastPosition {
		e.numExpectedShifts++
		denseRegionLength := e.lastPosition - e.denseRegionStartIndex + 1
		e.numExpectedShifts += (denseRegionLength * denseRegionLength) / 4
		e.denseRegionStartIndex = actualPosition
	}
	e.lastPosition = actualPosition + 1
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
	e.lastPosition = 0
	e.denseRegionStartIndex = 0
	e.numExpectedShifts = 0
	e.count = 0
}

func NewExpectedShiftsAccumulator() *ExpectedShiftsAccumulator {
	return &ExpectedShiftsAccumulator{
		lastPosition:          0,
		denseRegionStartIndex: 0,
		numExpectedShifts:     0,
		count:                 0,
		dataCapacity:          0,
	}
}
