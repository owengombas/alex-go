package node

import (
	"alex_go/cost_models"
	"alex_go/linear_model"
	"alex_go/shared"
	"fmt"
	"unsafe"
)

type DataNode struct {
	// Parameters from the Node interface
	DuplicationFactor int
	Level             int
	LinearModel       linear_model.LinearModel
	Cost              float64

	NextLeaf *DataNode
	PrevLeaf *DataNode

	// Holds the keys
	Keys []shared.KeyType
	// Holds the payloads
	Payloads []shared.PayloadType

	// Size of key/data_slots array
	DataCapacity int
	// Number of filled key/data slots (as opposed to gaps)
	NumKeys int

	// Bitmap: each uint64_t represents 64 positions in reverse order
	// (i.e., each uint64_t is "read" from the right-most bit to the left-most
	// bit)
	Bitmap []bool

	// Expand after m_num_keys is >= this number
	ExpansionThreshold float64
	// Contract after m_num_keys is < this number
	ContractionThreshold float64

	// -- Counters used in Cost models --
	// Does not reset after resizing
	NumShifts int64
	// Does not reset after resizing
	NumExpSearchIterations int64
	// Does not reset after resizing
	NumLookups int
	// Does not reset after resizing
	NumInserts int
	// Technically not required, but nice to have
	NumResizes int

	// -- Variables for determining append-mostly behavior --
	// Max key in node, updates after inserts but not erases
	MaxKey shared.KeyType
	// Min key in node, updates after inserts but not erases
	MinKey shared.KeyType
	// Number of inserts that are larger than the max key
	NumRightOutOfBoundsInserts int
	// Number of inserts that are smaller than the min key
	NumLeftOutOfBoundsInserts int

	// -- Benchmarks --
	ExpectedAvgExpSearchIterations float64
	ExpectedAvgShifts              float64

	CurrentIteratorPosition int

	MaxSlots int
}

func (self *DataNode) GetCost() float64 {
	return self.Cost
}

func (self *DataNode) SetCost(cost float64) {
	self.Cost = cost
}

func (self *DataNode) GetLevel() int {
	return self.Level
}

func (self *DataNode) SetLevel(level int) {
	self.Level = level
}

func (self *DataNode) GetDuplicationFactor() int {
	return self.DuplicationFactor
}

func (self *DataNode) SetDuplicationFactor(duplicationFactor int) {
	self.DuplicationFactor = duplicationFactor
}

func (self *DataNode) GetLinearModel() *linear_model.LinearModel {
	return &self.LinearModel
}

func (self *DataNode) SetLinearModel(linearModel linear_model.LinearModel) {
	self.LinearModel = linearModel
}

func (self *DataNode) GetNodeSize() int64 {
	return int64(unsafe.Sizeof(*self))
}

func (self *DataNode) IsAppendMostlyRight() bool {
	return float64(self.NumRightOutOfBoundsInserts)/float64(self.NumInserts) > shared.KAppendMostlyThreshold
}

func (self *DataNode) IsAppendMostlyLeft() bool {
	return float64(self.NumLeftOutOfBoundsInserts)/float64(self.NumInserts) > shared.KAppendMostlyThreshold
}

// BinarySearchUpperBound Searches for the first position greater than key in range [l, r)
// https://stackoverflow.com/questions/6443569/implementation-of-c-lower-bound
// Returns position in range [l, r]
func (self *DataNode) BinarySearchUpperBound(l int, r int, key shared.KeyType) int {
	for l < r {
		m := l + (r-l)/2
		if self.Keys[m] <= key {
			l = m + 1
		} else {
			r = m
		}
	}
	return l
}

// BinarySearchLowerBound Searches for the first position no less than key in range [l, r)
// https://stackoverflow.com/questions/6443569/implementation-of-c-lower-bound
// Returns position in range [l, r]
func (self *DataNode) BinarySearchLowerBound(l int, r int, key shared.KeyType) int {
	for l < r {
		m := l + (r-l)/2
		if self.Keys[m] >= key {
			r = m
		} else {
			l = m + 1
		}
	}
	return l
}

// Searches for the first position greater than key, starting from position m
// Returns position in range [0, data_capacity]
func (self *DataNode) ExponentialSearchUpperBound(m int, key shared.KeyType) int {
	bound := 1
	var l, r int
	if self.Keys[m] > key {
		size := m
		for bound < size && self.Keys[m-bound] > key {
			bound *= 2
			self.NumExpSearchIterations++
		}
		l = m - min(bound, size)
		r = m - bound/2
	} else {
		size := self.DataCapacity - m
		for bound < size && self.Keys[m+bound] <= key {
			bound *= 2
			self.NumExpSearchIterations++
		}
		l = m + bound/2
		r = m + min(bound, size)
	}
	return self.BinarySearchUpperBound(l, r, key)
}

// Searches for the first position no less than key, starting from position m
// Returns position in range [0, data_capacity]
func (self *DataNode) ExponentialSearchLowerBound(m int, key shared.KeyType) int {
	bound := 1
	var l, r int
	if self.Keys[m] >= key {
		size := m
		for bound < size && self.Keys[m-bound] >= key {
			bound *= 2
			self.NumExpSearchIterations++
		}
		l = m - min(bound, size)
		r = m - bound/2
	} else {
		size := self.DataCapacity - m
		for bound < size && self.Keys[m+bound] < key {
			bound *= 2
			self.NumExpSearchIterations++
		}
		l = m + bound/2
		r = m + min(bound, size)
	}
	return self.BinarySearchLowerBound(l, r, key)
}

// UpperBound Searches for the first position greater than key
// This could be the position for a gap (i.e., its bit in the Bitmap is 0)
// Returns position in range [0, data_capacity]
// Compare with find_upper()
func (self *DataNode) UpperBound(key shared.KeyType) int {
	self.NumLookups++
	position := self.PredictPosition(key)
	return self.ExponentialSearchUpperBound(position, key)
}

// LowerBound Searches for the first position no less than key
// This could be the position for a gap (i.e., its bit in the Bitmap is 0)
// Returns position in range [0, data_capacity]
// Compare with find_lower()
func (self *DataNode) LowerBound(key shared.KeyType) int {
	self.NumLookups++
	position := self.PredictPosition(key)
	return self.ExponentialSearchLowerBound(position, key)
}

// FindUpper Searches for the first non-gap position greater than key
// Returns position in range [0, data_capacity]
// Compare with upper_bound()
func (self *DataNode) FindUpper(key shared.KeyType) int {
	self.NumLookups++
	position := self.PredictPosition(key)
	pos := self.ExponentialSearchUpperBound(position, key)
	return self.GetNextFilledPosition(pos, false)
}

// FindLower Searches for the first non-gap position no less than key
// Returns position in range [0, data_capacity]
// Compare with lower_bound()
func (self *DataNode) FindLower(key shared.KeyType) int {
	self.NumLookups++
	position := self.PredictPosition(key)
	pos := self.ExponentialSearchLowerBound(position, key)
	return self.GetNextFilledPosition(pos, false)
}

// FindKeyPosition Searches for the last non-gap position equal to key
// If no positions equal to key, returns -1
func (self *DataNode) FindKeyPosition(key shared.KeyType) (int, error) {
	self.NumLookups++
	predictedPosition := self.PredictPosition(key)

	position := self.ExponentialSearchUpperBound(predictedPosition, key) - 1
	if position < 0 || self.Keys[position] != key {
		return 0, shared.KeyNotFoundError
	}

	return position, nil
}

func (self *DataNode) InsertElementAt(key shared.KeyType, payload shared.PayloadType, pos int) {
	self.Keys[pos] = key
	self.Payloads[pos] = payload
	self.Bitmap[pos] = true

	// Overwrite preceding gaps until we reach the previous element
	pos--
	for pos >= 0 && !self.Bitmap[pos] {
		self.Keys[pos] = key
		pos--
	}
}

// Returns position of closest gap to pos. Returns pos if pos is a gap
func (self *DataNode) ClosestGap(pos int) (int, error) {
	// A slower version of closest_gap that does not use lzcnt and tzcnt
	// Does not return pos if pos is a gap
	maxLeftOffset := pos
	maxRightOffset := self.DataCapacity - pos - 1
	maxDirectionalOffset := min(maxLeftOffset, maxRightOffset)
	distance := 1
	for distance <= maxDirectionalOffset {
		if !self.Bitmap[pos-distance] {
			return pos - distance, nil
		}
		if !self.Bitmap[pos+distance] {
			return pos + distance, nil
		}
		distance++
	}
	if maxLeftOffset > maxRightOffset {
		for i := pos - distance; i >= 0; i-- {
			if !self.Bitmap[i] {
				return i, nil
			}
		}
	} else {
		for i := pos + distance; i < self.DataCapacity; i++ {
			if !self.Bitmap[i] {
				return i, nil
			}
		}
	}
	return -1, shared.NoGapFoundError
}

// Predicts the position of a key using the model
func (self *DataNode) PredictPosition(key shared.KeyType) int {
	position := self.LinearModel.Predict(float64(key))
	position = max(min(position, self.DataCapacity-1), 0)
	return position
}

// Finds position to insert a key.
// First returned value takes prediction into account.
// Second returned value is first valid position (i.e., upper_bound of key).
// If there are duplicate keys, the insert position will be to the right of
// all existing keys of the same value.
func (self *DataNode) FindInsertPosition(key shared.KeyType) (int, int) {
	predictPosition := self.PredictPosition(key) // first use model to get prediction

	// insert to the right of duplicate keys
	pos := self.ExponentialSearchUpperBound(predictPosition, key)
	if predictPosition <= pos || self.Bitmap[pos] {
		return pos, pos
	} else {
		return min(predictPosition, self.GetNextFilledPosition(pos, true)-1), pos
	}
}

// Insert key into pos, shifting as necessary in the range [left, right)
// Returns the actual position of insertion
func (self *DataNode) InsertUsingShifts(key shared.KeyType, payload shared.PayloadType, pos int) int {
	gapPos, err := self.ClosestGap(pos)
	if err != nil {
		panic(err)
	}
	self.Bitmap[gapPos] = true

	if gapPos >= pos {
		for i := gapPos; i > pos; i-- {
			self.Keys[i] = self.Keys[i-1]
			self.Payloads[i] = self.Payloads[i-1]
		}
		self.InsertElementAt(key, payload, pos)
		self.NumShifts += int64(gapPos - pos)
		return pos
	} else {
		for i := gapPos; i < pos-1; i++ {
			self.Keys[i] = self.Keys[i+1]
			self.Payloads[i] = self.Payloads[i+1]
		}
		self.InsertElementAt(key, payload, pos-1)
		self.NumShifts += int64(pos - gapPos - 1)
		return pos - 1
	}
}

// Starting from a position, return the first position that is not a gap
// If no more filled positions, will return data_capacity
// If exclusive is true, output is at least (pos + 1)
// If exclusive is false, output can be pos itself
func (self *DataNode) GetNextFilledPosition(pos int, exclusive bool) int {
	if exclusive {
		pos++
		if pos == self.DataCapacity {
			return self.DataCapacity
		}
	}

	for pos < self.DataCapacity && !self.Bitmap[pos] {
		pos++
	}

	return pos
}

// ShiftsPerInserts Empirical average number of shifts per insert
func (self *DataNode) ShiftsPerInserts() float64 {
	if self.NumInserts == 0 {
		return 0.0
	}
	return float64(self.NumShifts) / float64(self.NumInserts)
}

// CatastrophicCost Returns true if Cost is catastrophically high and we want to force a split
// The heuristic for this is if the number of shifts per insert (expected or
// empirical) is over 100
func (self *DataNode) CatastrophicCost() bool {
	return self.LinearModel.A != 0.0 && self.ShiftsPerInserts() > 100 || self.ExpectedAvgShifts > 100
}

// ExpSearchIterationsPerOperation Empirical average number of exponential search iterations per operation
// (either lookup or insert)
func (self *DataNode) ExpSearchIterationsPerOperation() float64 {
	numOps := self.NumInserts + self.NumLookups
	if numOps == 0 {
		return 0.0
	}
	return float64(self.NumExpSearchIterations) / float64(numOps)
}

func (self *DataNode) FracInserts() float64 {
	numOps := self.NumInserts + self.NumLookups
	if numOps == 0 {
		return 0.0
	}
	return float64(self.NumInserts) / float64(numOps)
}

func (self *DataNode) EmpiricalCost() float64 {
	numOps := self.NumInserts + self.NumLookups
	if numOps == 0 {
		return 0.0
	}
	fracInserts := float64(self.NumInserts) / float64(numOps)
	return shared.KExpSearchIterationsWeight*self.ExpSearchIterationsPerOperation() +
		shared.KShiftsWeight*self.ShiftsPerInserts()*fracInserts
}

// SignificantCostDeviation Whether empirical Cost deviates significantly from expected Cost
// Also returns false if empirical Cost is sufficiently low and is not worth
// splitting
func (self *DataNode) SignificantCostDeviation() bool {
	empiricalCost := self.EmpiricalCost()
	return self.LinearModel.A != 0.0 && empiricalCost > shared.KNodeLookupsWeight && empiricalCost > 1.5*self.Cost
}

func (self *DataNode) ComputeExpectedCost(fracInserts float64) float64 {
	if self.NumKeys == 0 {
		return 0.0
	}

	searchIterationsAccumaulator := cost_models.NewExpectedSearchIterationsAccumulator()
	shiftsAccumulator := cost_models.NewExpectedShiftsAccumulator(self.DataCapacity)
	self.IterateFilledPositions(func(key shared.KeyType, payload shared.PayloadType, i int, j int) {
		predictedPosition := max(0, min(self.DataCapacity-1, self.LinearModel.Predict(float64(key))))
		searchIterationsAccumaulator.Accumulate(i, predictedPosition, 0.0)
		shiftsAccumulator.Accumulate(i, predictedPosition, 0.0)
	}, 0, self.DataCapacity)

	expectedAvgExpSearchIterations := searchIterationsAccumaulator.GetStats()
	expectedAvgShifts := shiftsAccumulator.GetStats()

	return shared.KExpSearchIterationsWeight*expectedAvgExpSearchIterations + shared.KShiftsWeight*expectedAvgShifts*fracInserts
}

func (self *DataNode) EraseRange(startKey shared.KeyType, endKey shared.KeyType, endKeyInclusive bool) int {
	var pos int
	if endKeyInclusive {
		pos = self.UpperBound(endKey)
	} else {
		pos = self.LowerBound(endKey)
	}

	if pos == 0 {
		return 0
	}

	numErased := 0
	var nextKey shared.KeyType
	if pos == self.DataCapacity {
		nextKey = shared.KEndSentinel
	} else {
		nextKey = self.Keys[pos]
	}
	pos--

	for pos >= 0 && self.Keys[pos] >= startKey {
		self.Keys[pos] = nextKey
		self.Bitmap[pos] = false
		if self.Bitmap[pos] {
			numErased++
		}
		pos--
	}

	self.NumKeys -= numErased

	if float64(self.NumKeys) < self.ContractionThreshold {
		self.Resize(shared.KMinDensity, false, false, false)
		self.NumResizes++
	}

	return numErased
}

func (self *DataNode) Insert(key shared.KeyType, payload shared.PayloadType) (int, error) {
	// Periodically check for catastrophe
	if self.NumInserts%shared.CatastropheCheckFrequency == 0 && self.CatastrophicCost() {
		return 0, shared.CatastrophicCostInsertionError
	}

	if float64(self.NumKeys) >= self.ExpansionThreshold {
		if self.SignificantCostDeviation() {
			return 0, shared.SignificantCostDeviationInsertionError
		}
		if self.CatastrophicCost() {
			return 0, shared.CatastrophicCostInsertionError
		}
		if float64(self.NumKeys) > float64(self.MaxSlots)*shared.KMinDensity {
			return 0, shared.MaxCapacityInsertionError
		}
		keepLeft := self.IsAppendMostlyRight()
		keepRight := self.IsAppendMostlyLeft()
		self.Resize(shared.KMinDensity, false, keepLeft, keepRight)
		self.NumResizes++
	}

	insertionPosition, _ := self.FindInsertPosition(key)

	if insertionPosition < self.DataCapacity && !self.Bitmap[insertionPosition] {
		self.InsertElementAt(key, payload, insertionPosition)
	} else {
		insertionPosition = self.InsertUsingShifts(key, payload, insertionPosition)
	}

	self.NumKeys++
	self.NumInserts++
	if key > self.MaxKey {
		self.MaxKey = key
		self.NumRightOutOfBoundsInserts++
	}
	if key < self.MinKey {
		self.MinKey = key
		self.NumLeftOutOfBoundsInserts++
	}
	return insertionPosition, nil
}

func (self *DataNode) Resize(targetDensity float64, forceRetrain bool, keepLeft bool, keepRight bool) {
	if self.NumKeys == 0 {
		return
	}

	newDataCapacity := max(int(float64(self.NumKeys)/targetDensity), self.NumKeys+1)
	newKeySlots := make([]shared.KeyType, newDataCapacity)
	newPayloadSlots := make([]shared.PayloadType, newDataCapacity)
	newBitmap := make([]bool, newDataCapacity)

	if self.NumKeys < shared.NumKeysDataNodeRetrainThreshold || forceRetrain {
		linearModelBuilder := linear_model.NewLinearModelBuilder(&self.LinearModel)
		self.IterateFilledPositions(func(key shared.KeyType, payload shared.PayloadType, i int, j int) {
			fmt.Println("--->", key, payload, i, j)
			linearModelBuilder.Add(float64(key), float64(j))
		}, 0, self.DataCapacity)
		linearModelBuilder.Build()
		fmt.Println("a: ", self.LinearModel.A, "b: ", self.LinearModel.B)

		if keepLeft {
			self.LinearModel.Expand(float64(self.DataCapacity) / float64(self.NumKeys))
		} else if keepRight {
			self.LinearModel.Expand(float64(self.DataCapacity) / float64(self.NumKeys))
			self.LinearModel.B += float64(newDataCapacity - self.DataCapacity)
		} else {
			self.LinearModel.Expand(float64(newDataCapacity) / float64(self.NumKeys))
		}
	} else {
		if keepRight {
			self.LinearModel.B += float64(newDataCapacity - self.DataCapacity)
		} else if !keepLeft {
			self.LinearModel.Expand(float64(newDataCapacity) / float64(self.DataCapacity))
		}
	}

	lastPosition := -1
	keysRemaining := self.NumKeys
	i := self.GetNextFilledPosition(0, false)
	for i < self.DataCapacity {
		position := self.LinearModel.Predict(float64(self.Keys[i]))
		position = max(position, lastPosition+1)

		positionsRemaining := newDataCapacity - position
		if positionsRemaining < keysRemaining {
			// fill the rest of the store contiguously
			pos := newDataCapacity - keysRemaining

			for j := lastPosition + 1; j < pos; j++ {
				newKeySlots[j] = self.Keys[i]
			}

			for pos < newDataCapacity {
				newKeySlots[pos] = self.Keys[i]
				newPayloadSlots[pos] = self.Payloads[i]
				newBitmap[pos] = true

				i = self.GetNextFilledPosition(i+1, false)
				pos++
			}

			lastPosition = position - 1
			break
		}

		for j := lastPosition + 1; j < position; j++ {
			newKeySlots[j] = self.Keys[i]
		}

		newKeySlots[position] = self.Keys[i]
		newPayloadSlots[position] = self.Payloads[i]
		newBitmap[position] = true

		lastPosition = position
		keysRemaining--

		i = self.GetNextFilledPosition(i+1, false)
	}

	for i = lastPosition + 1; i < newDataCapacity; i++ {
		newKeySlots[i] = shared.KEndSentinel
	}

	self.DataCapacity = newDataCapacity
	self.Keys = newKeySlots
	self.Payloads = newPayloadSlots
	self.Bitmap = newBitmap
	self.ExpansionThreshold = min(max(float64(self.DataCapacity)*shared.KMaxDensity, float64(self.NumKeys+1)), float64(self.DataCapacity))
	self.ContractionThreshold = float64(self.DataCapacity) * shared.KMinDensity
}

func (self *DataNode) IterateFilledPositions(yield func(shared.KeyType, shared.PayloadType, int, int), start int, end int) {
	j := 0
	for i := max(start, 0); i < min(self.DataCapacity, end); i++ {
		if self.Bitmap[i] {
			key := self.Keys[i]
			payload := self.Payloads[i]
			yield(key, payload, i, j)
			j++
		}
	}
}

func (self *DataNode) GetFirstKey() shared.KeyType {
	for i := 0; i < self.DataCapacity; i++ {
		if self.Bitmap[i] {
			return self.Keys[i]
		}
	}
	return shared.MaxKey
}

func (self *DataNode) GetLastKey() shared.KeyType {
	for i := self.DataCapacity - 1; i >= 0; i-- {
		if self.Bitmap[i] {
			return self.Keys[i]
		}
	}
	return shared.MinKey

}

// Number of keys between positions left and right (exclusive) in
// key/data_slots
func (self *DataNode) NumKeysInRange(left int, right int) int {
	numKeys := 0
	for i := left; i < right; i++ {
		if self.Bitmap[i] {
			numKeys++
		}
	}
	return numKeys
}

func (self *DataNode) ResetStats() {
	self.NumShifts = 0
	self.NumExpSearchIterations = 0
	self.NumLookups = 0
	self.NumInserts = 0
	self.NumResizes = 0
}

func (self *DataNode) IsLeaf() bool {
	return true
}

func (self *DataNode) Initialize(numKeys int, density float64) {
	self.NumKeys = numKeys
	self.DataCapacity = int(max(float64(numKeys)/density, float64(numKeys)+1))
	self.Keys = make([]shared.KeyType, self.DataCapacity)
	self.Payloads = make([]shared.PayloadType, self.DataCapacity)
	self.Bitmap = make([]bool, self.DataCapacity)
}

func (self *DataNode) BulkLoadFromExisting(
	node *DataNode,
	left int,
	right int,
	keepLeft bool,
	keepRight bool,
	preComputedModel *linear_model.LinearModel,
	preComputedActualKeys int,
) {
	if !(left >= 0 && right <= node.DataCapacity) {
		panic("left and right must be within the range of the node")
	}

	numActualKeys := 0
	if preComputedModel == nil || preComputedActualKeys == -1 {
		linearModelBuilder := linear_model.NewLinearModelBuilder(&node.LinearModel)
		node.IterateFilledPositions(func(key shared.KeyType, payload shared.PayloadType, i int, j int) {
			linearModelBuilder.Add(float64(key), float64(j))
			numActualKeys++
		}, left, right)
		linearModelBuilder.Build()
	} else {
		numActualKeys = preComputedActualKeys
		self.LinearModel.A = preComputedModel.A
		self.LinearModel.B = preComputedModel.B
	}

	self.Initialize(numActualKeys, shared.KMinDensity)
	if numActualKeys == 0 {
		self.ExpansionThreshold = float64(self.DataCapacity)
		self.ContractionThreshold = 0.0
		for i := 0; i < self.DataCapacity; i++ {
			self.Keys[i] = shared.KEndSentinel
		}
		return
	}

	if keepLeft {
		self.LinearModel.Expand(float64(numActualKeys) / shared.KMaxDensity / float64(self.NumKeys))
	} else if keepRight {
		self.LinearModel.Expand(float64(numActualKeys) / shared.KMaxDensity / float64(self.NumKeys))
		self.LinearModel.B += float64(self.DataCapacity) - (float64(numActualKeys) / shared.KMaxDensity)
	} else {
		self.LinearModel.Expand(float64(self.DataCapacity) / float64(self.NumKeys))
	}

	// Model-based inserts
	lastPosition := -1
	keysRemaining := self.NumKeys
	i := node.GetNextFilledPosition(left, false)
	minKey := node.Keys[i]
	for i < right {
		position := self.LinearModel.Predict(float64(node.Keys[i]))
		position = max(position, lastPosition+1)

		positionsRemaining := self.DataCapacity - position
		if positionsRemaining < keysRemaining {
			// fill the rest of the store contiguously
			pos := self.DataCapacity - keysRemaining

			for j := lastPosition + 1; j < pos; j++ {
				self.Keys[j] = minKey
			}

			for ; pos < self.DataCapacity; pos++ {
				self.Keys[pos] = node.Keys[i]
				self.Payloads[pos] = node.Payloads[i]
				self.Bitmap[pos] = true

				i = node.GetNextFilledPosition(i+1, false)
			}

			lastPosition = position - 1
			break
		}

		for j := lastPosition + 1; j < position; j++ {
			self.Keys[j] = minKey
		}

		self.Keys[position] = node.Keys[i]
		self.Payloads[position] = node.Payloads[i]
		self.Bitmap[position] = true

		lastPosition = position
		keysRemaining--
		i = node.GetNextFilledPosition(i+1, false)
	}

	for i = lastPosition + 1; i < self.DataCapacity; i++ {
		self.Keys[i] = shared.KEndSentinel
	}

	self.ExpansionThreshold = min(max(float64(self.DataCapacity)*shared.KMaxDensity, float64(self.NumKeys+1)), float64(self.DataCapacity))
	self.ContractionThreshold = float64(self.DataCapacity) * shared.KMinDensity
}

func NewDataNode(dataCapacity int) *DataNode {
	return &DataNode{
		NextLeaf:                       nil,
		PrevLeaf:                       nil,
		DuplicationFactor:              0,
		Level:                          0,
		LinearModel:                    linear_model.LinearModel{},
		Cost:                           0.0,
		Payloads:                       make([]shared.PayloadType, dataCapacity),
		Keys:                           make([]shared.KeyType, dataCapacity),
		NumKeys:                        0,
		DataCapacity:                   dataCapacity,
		Bitmap:                         make([]bool, dataCapacity),
		ExpansionThreshold:             1.0,
		ContractionThreshold:           0.0,
		NumShifts:                      0,
		NumExpSearchIterations:         0,
		NumLookups:                     0,
		NumInserts:                     0,
		NumResizes:                     0,
		MaxKey:                         shared.MinKey,
		MinKey:                         shared.MaxKey,
		NumRightOutOfBoundsInserts:     0,
		NumLeftOutOfBoundsInserts:      0,
		ExpectedAvgExpSearchIterations: 0.0,
		ExpectedAvgShifts:              0.0,
		CurrentIteratorPosition:        0,
		MaxSlots:                       shared.MaxSlots,
	}
}

func BuildNodeImplicitFromExisting(
	node *DataNode,
	left int,
	right int,
	numActualKeys int,
	dataCapacity int,
	acc cost_models.Accumulator,
	linearModel *linear_model.LinearModel,
) {
	lastPosition := -1
	keysRemaining := numActualKeys
	i := node.GetNextFilledPosition(left, false)
	for i < right {
		predictedPosition := max(min(dataCapacity-1, linearModel.Predict(float64(node.Keys[i])), 0))
		actualPosition := max(predictedPosition, lastPosition+1)
		positionRemaining := dataCapacity - actualPosition
		if positionRemaining < keysRemaining {
			actualPosition = dataCapacity - keysRemaining
			for actualPosition < dataCapacity {
				predictedPosition = max(0, min(dataCapacity-1, linearModel.Predict(float64(node.Keys[i]))))
				acc.Accumulate(actualPosition, predictedPosition, 0.0)
				actualPosition++
				i = node.GetNextFilledPosition(i+1, false)
			}
			break
		}
		acc.Accumulate(actualPosition, predictedPosition, 0.0)
		lastPosition = actualPosition
		keysRemaining--
		i = node.GetNextFilledPosition(i+1, false)
	}
}

func ComputeExpectedCostFromExisting(
	node *DataNode,
	left int,
	right int,
	density float64,
	expectedInsertFrac float64,
	existingModel *linear_model.LinearModel,
) (float64, float64, float64) {
	if !(left >= 0 && right <= node.DataCapacity) {
		panic("Invalid range")
	}

	linearModel := linear_model.NewLinearModel(0, 0)
	numActualKeys := 0
	if existingModel != nil {
		builder := linear_model.NewLinearModelBuilder(linearModel)
		node.IterateFilledPositions(func(key shared.KeyType, payload shared.PayloadType, i int, j int) {
			builder.Add(float64(key), float64(j))
			numActualKeys++
		}, left, right)
		builder.Build()
	} else {
		numActualKeys = node.NumKeysInRange(left, right)
		linearModel.A = existingModel.A
		linearModel.B = existingModel.B
	}

	if numActualKeys == 0 {
		return 0, -1, -1
	}

	dataCapacity := max(int(float64(numActualKeys)/density), numActualKeys+1)
	linearModel.Expand(float64(dataCapacity) / float64(numActualKeys))

	cost := 0.0
	expectedAvgExpSearchIterations := 0.0
	expectedAvgShifts := 0.0
	if expectedInsertFrac == 0 {
		accumulator := cost_models.NewExpectedSearchIterationsAccumulator()
		BuildNodeImplicitFromExisting(node, left, right, numActualKeys, dataCapacity, accumulator, linearModel)
		expectedAvgExpSearchIterations = accumulator.GetStats()
	} else {
		accumulator := cost_models.NewExpectedSearchIterationsAndShiftsAccumulator(dataCapacity)
		BuildNodeImplicitFromExisting(node, left, right, numActualKeys, dataCapacity, accumulator, linearModel)
		expectedAvgExpSearchIterations = accumulator.GetExpectedNumSearchIterations()
		expectedAvgShifts = accumulator.GetExpectedNumShifts()
	}
	cost = shared.KExpSearchIterationsWeight*float64(expectedAvgExpSearchIterations) + shared.KShiftsWeight*float64(expectedAvgShifts)*expectedInsertFrac

	return cost, expectedAvgExpSearchIterations, expectedAvgShifts
}
