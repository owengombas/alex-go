package index

import (
	"alex_go/fanout_tree"
	"alex_go/linear_model"
	"alex_go/node"
	"alex_go/shared"
	"errors"
	"math"
)

type Index struct {
	superRootNode *node.ModelNode
	rootNode      node.Node

	// -- Traversal node --
	// Save the traversal path down the RMI by having a linked list of these structs
	traversalNode         *node.ModelNode
	traversalNodeBucketID int

	// -- User-changeable parameters --
	// When bulk loading, Alex can use provided knowledge of the expected
	// fraction of operations that will be inserts
	// For simplicity, operations are either point lookups ("reads") or inserts
	// ("writes)
	// i.e., 0 means we expect a read-only workload, 1 means write-only
	expectedInsertFrac float64
	// Maximum node size, in bytes. By default, 16MB.
	// Higher values result in better average throughput, but worse tail/max
	// insert latency
	maxNodeSize int
	// Approximate model computation: bulk load faster by using sampling to train models
	approximateModelComputation bool
	// Approximate cost computation: bulk load faster by using sampling to compute cost
	approximateCostComputation bool

	// -- Derived parameters --
	// Setting max node size automatically changes these parameters
	// The defaults here assume the default max node size of 16MB
	// assumes 8-byte pointers
	maxFanout        int
	maxDataNodeSlots int

	// -- Statistics --
	numKeys                       int
	numModelNodes                 int
	numDataNodes                  int
	numExpandAndScales            int
	numExpandAndRetrains          int
	numDownwardSplits             int
	numSidewaysSplits             int
	numModelNodeExpansions        int
	numModelNodeSplits            int
	numResizes                    int
	numDownwardSplitKeys          int64
	numSidewaysSplitKeys          int64
	numModelNodeExpansionPointers int64
	numModelNodeSplitPointers     int64
	numNodeLookups                int64
	numLookups                    int64
	numInserts                    int64
	splittingTime                 float64
	costComputationTime           float64

	// -- Internal parameters --
	keyDomainMin                   shared.KeyType
	keyDomainMax                   shared.KeyType
	numKeysAboveKeyDomain          int
	numKeysBelowKeyDomain          int
	numKeysAtLastRightDomainResize int
	numKeysAtLastLeftDomainResize  int

	// -- Split Decision Costs --
	// Used when finding the best way to propagate up the RMI when splitting upwards.
	// Cost is in terms of additional model size created through splitting upwards, measured in units of pointers.
	// One instance of this struct is created for each node on the traversal path.
	// User should take into account the cost of metadata for new model nodes (base_cost).
	baseCost float64
	// Additional cost due to this node if propagation stops at this node.
	// Equal to 0 if redundant slot exists, otherwise number of new pointers due to node expansion.
	stopCost float64
	// Additional cost due to this node if propagation continues past this node.
	// Equal to number of new pointers due to node splitting, plus size of metadata of new model node.
	splitCost float64
}

func (self *Index) createSuperRoot() {
	if self.rootNode == nil {
		return
	}
	self.superRootNode = node.NewModelNode(1)
	self.superRootNode.NumChildren = 1
	self.superRootNode.Children = make([]node.Node, 1)
	self.updateSuperRootNodePointer()
}

func (self *Index) FirstDataNode() *node.DataNode {
	current := self.rootNode

	// Cast the current node to a ModelNode
	for !current.IsLeaf() {
		currentModelNode, _ := current.(*node.ModelNode)
		current = currentModelNode.Children[0]
	}

	return current.(*node.DataNode)
}

func (self *Index) LastDataNode() *node.DataNode {
	current := self.rootNode

	// Cast the current node to a ModelNode
	for !current.IsLeaf() {
		currentModelNode, _ := current.(*node.ModelNode)
		current = currentModelNode.Children[currentModelNode.NumChildren-1]
	}

	return current.(*node.DataNode)
}

func (self *Index) GetMinKey() shared.KeyType {
	return self.FirstDataNode().GetFirstKey()
}

func (self *Index) GetMaxKey() shared.KeyType {
	return self.LastDataNode().GetLastKey()
}

// Make a correction to the traversal path to instead point to the leaf node
// that is to the left or right of the current leaf node.
func (self *Index) correctTraversalPath(leaf *node.DataNode, traversalPath *[]struct {
	*node.ModelNode
	int
}, left bool) {
	if left {
		repeats := 1 << leaf.GetDuplicationFactor()
		tn := (*traversalPath)[len(*traversalPath)-1]
		startBucketID := tn.int - (tn.int % repeats)
		if startBucketID == 0 {
			for startBucketID == 0 {
				*traversalPath = (*traversalPath)[:len(*traversalPath)-1] // Pop the last element
				repeats = 1 << tn.ModelNode.GetDuplicationFactor()
				tn = (*traversalPath)[len(*traversalPath)-1]
				startBucketID = tn.int - (tn.int % repeats)
			}
			correctBucketID := startBucketID - 1
			tn.int = correctBucketID

			currentNode := &tn.ModelNode.Children[correctBucketID]
			for !(*currentNode).IsLeaf() {
				currentModelNode, _ := (*currentNode).(*node.ModelNode)
				*traversalPath = append(*traversalPath, struct {
					*node.ModelNode
					int
				}{currentModelNode, currentModelNode.NumChildren - 1})
				currentNode = &currentModelNode.Children[currentModelNode.NumChildren-1]
			}

			if (*currentNode).(*node.DataNode) != leaf.PrevLeaf {
				panic("Incorrect traversal path")
			}
		} else {
			tn.int = startBucketID - 1
		}
	} else {
		repeats := 1 << leaf.GetDuplicationFactor()
		tn := (*traversalPath)[len(*traversalPath)-1]
		endBucketID := tn.int - (tn.int % repeats) + repeats
		if endBucketID == tn.ModelNode.NumChildren {
			for endBucketID == tn.ModelNode.NumChildren {
				*traversalPath = (*traversalPath)[:len(*traversalPath)-1] // Pop the last element
				repeats = 1 << tn.ModelNode.GetDuplicationFactor()
				tn = (*traversalPath)[len(*traversalPath)-1]
				endBucketID = tn.int - (tn.int % repeats) + repeats
			}
			correctBucketID := endBucketID
			tn.int = correctBucketID

			currentNode := &tn.ModelNode.Children[correctBucketID]
			for !(*currentNode).IsLeaf() {
				currentModelNode, _ := (*currentNode).(*node.ModelNode)
				*traversalPath = append(*traversalPath, struct {
					*node.ModelNode
					int
				}{currentModelNode, 0})
				currentNode = &currentModelNode.Children[0]
			}

			if (*currentNode).(*node.DataNode) != leaf.NextLeaf {
				panic("Incorrect traversal path")
			}
		} else {
			tn.int = endBucketID
		}

	}
}

func (self *Index) GetLeaf(key shared.KeyType, buildTraversalPath bool) (*node.DataNode, []struct {
	*node.ModelNode
	int
}) {
	traversalPath := make([]struct {
		*node.ModelNode
		int
	}, 0)
	if buildTraversalPath {
		traversalPath = append(traversalPath, struct {
			*node.ModelNode
			int
		}{self.superRootNode, 0})
	}

	currentNode := self.rootNode
	for currentNode.IsLeaf() {
		return self.rootNode.(*node.DataNode), traversalPath
	}

	for {
		currentModelNode := currentNode.(*node.ModelNode)
		bucketIDPrediction := currentNode.GetLinearModel().PredictDouble(float64(key))
		bucketID := min(max(int(bucketIDPrediction), 0), currentModelNode.NumChildren-1)
		if buildTraversalPath {
			traversalPath = append(traversalPath, struct {
				*node.ModelNode
				int
			}{currentModelNode, bucketID})
		}

		currentNode = currentModelNode.Children[bucketID]

		if currentNode.IsLeaf() {
			currentDataNode := currentNode.(*node.DataNode)
			self.numNodeLookups += int64(currentDataNode.GetLevel())
			bucketIDPredictionRounded := float64(int(bucketIDPrediction + 0.5))
			epsilon := math.Nextafter(1.0, 2.0) - 1.0 // https://stackoverflow.com/questions/22185636/easiest-way-to-get-the-machine-epsilon-in-go
			tolerance := 10 * epsilon * bucketIDPrediction
			if math.Abs(bucketIDPrediction-bucketIDPredictionRounded) <= tolerance {
				if bucketIDPredictionRounded <= bucketIDPrediction {
					if prevLeaf := currentDataNode.PrevLeaf; prevLeaf != nil {
						if prevLeaf.GetLastKey() >= key {
							if buildTraversalPath {
								self.correctTraversalPath(currentDataNode, &traversalPath, true)
							}
							return prevLeaf, traversalPath
						}
					}
				} else {
					if nextLeaf := currentDataNode.NextLeaf; nextLeaf != nil {
						if nextLeaf.GetFirstKey() <= key {
							if buildTraversalPath {
								self.correctTraversalPath(currentDataNode, &traversalPath, false)
							}
							return nextLeaf, traversalPath
						}
					}
				}
			}
			return currentDataNode, traversalPath
		}
	}
}

func (self *Index) shouldExpandRight() bool {
	isNotLeaf := !self.rootNode.IsLeaf()
	c1 := self.numKeysAboveKeyDomain >= shared.KMinOutOfDomainKeys
	toleranceFactorCondition := float64(self.numKeys)/float64(self.numKeysAtLastRightDomainResize) - 1
	c2 := float64(self.numKeysAboveKeyDomain) >= float64(shared.KOutOfDomainToleranceFactor)*toleranceFactorCondition
	c3 := self.numKeysAboveKeyDomain >= shared.KMaxOutOfDomainKeys
	c1c2 := c1 && c2
	return isNotLeaf && (c1c2 || c3)
}

func (self *Index) shouldExpandLeft() bool {
	isNotLeaf := !self.rootNode.IsLeaf()
	c1 := self.numKeysBelowKeyDomain >= shared.KMinOutOfDomainKeys
	toleranceFactorCondition := float64(self.numKeys)/float64(self.numKeysAtLastLeftDomainResize) - 1
	c2 := float64(self.numKeysBelowKeyDomain) >= float64(shared.KOutOfDomainToleranceFactor)*toleranceFactorCondition
	c3 := self.numKeysBelowKeyDomain >= shared.KMaxOutOfDomainKeys
	c1c2 := c1 && c2
	return isNotLeaf && (c1c2 || c3)
}

func (self *Index) updateSuperRootNodePointer() {
	self.superRootNode.Children[0] = self.rootNode
	self.superRootNode.SetLevel(self.rootNode.GetLevel() - 1)
}

// Caller needs to set the level, duplication factor, and neighbor pointers of the returned data node
func (self *Index) bulkLoadLeafNodeFromExisting(
	existingNode *node.DataNode,
	left int,
	right int,
	computeCost bool,
	treeNode *fanout_tree.FTNode,
	reuseModel bool,
	keepLeft bool,
	keepRight bool,
) *node.DataNode {
	node := node.NewDataNode(1)
	self.numDataNodes++
	if treeNode != nil {
		// Use the model and num_keys saved in the tree node so we don't have to
		// recompute it
		preComputedModel := linear_model.NewLinearModel(treeNode.A, treeNode.B)
		node.BulkLoadFromExisting(
			existingNode,
			left,
			right,
			keepLeft,
			keepRight,
			preComputedModel,
			treeNode.NumKeys,
		)
	} else if reuseModel {
		// Use the model from the existing node
		// Assumes the model is accurate
		numActualKeys := existingNode.NumKeysInRange(left, right)
		preComputedModel := linear_model.CopyLinearModel(existingNode.GetLinearModel())
		preComputedModel.B -= float64(left)
		preComputedModel.Expand(float64(numActualKeys) / float64(right-left))
		node.BulkLoadFromExisting(
			existingNode,
			left,
			right,
			keepLeft,
			keepRight,
			preComputedModel,
			numActualKeys,
		)
	} else {
		// Train a new model
		node.BulkLoadFromExisting(
			existingNode,
			left,
			right,
			keepLeft,
			keepRight,
			nil,
			-1,
		)
	}
	node.MaxSlots = self.maxDataNodeSlots

	if computeCost {
		node.Cost = node.ComputeExpectedCost(existingNode.FracInserts())
	}

	return node
}

// Expand the key value space that is covered by the index.
// Expands the root node (which is a model node).
// If the root node is at the max node size, then we split the root and create
// a new root node.
func (self *Index) expandRoot(key shared.KeyType, expandLeft bool) {
	root := self.rootNode.(*node.ModelNode)

	// Find the new bounds of the key domain.
	// Need to be careful to avoid overflows in the key type.
	var expansionFactor int
	var outermostNode *node.DataNode
	newDomainMin, newDomainMax := self.keyDomainMin, self.keyDomainMax
	domainSize := newDomainMax - newDomainMin
	if expandLeft {
		keyDifference := self.keyDomainMin - min(key, self.GetMinKey())
		expansionFactor = shared.Pow2RoundUp((keyDifference+domainSize-1)/domainSize + 1)

		halfExpandableDomain := self.keyDomainMax/2 - shared.MinKey/2
		halfExpandableDomainSize := shared.KeyType(expansionFactor) / 2 * domainSize
		if halfExpandableDomainSize < halfExpandableDomain {
			newDomainMin = shared.MinKey
		} else {
			newDomainMin = self.keyDomainMax
			newDomainMin -= 2 * halfExpandableDomainSize
		}
		self.numKeysAtLastLeftDomainResize = self.numKeys
		self.numKeysAboveKeyDomain = 0
		outermostNode = self.FirstDataNode()
	} else {
		keyDifference := max(key, self.GetMaxKey()) - self.keyDomainMax
		expansionFactor = shared.Pow2RoundUp((keyDifference+domainSize-1)/domainSize + 1)

		halfExpandableDomain := shared.MaxKey/2 - self.keyDomainMin/2
		halfExpandableDomainSize := shared.KeyType(expansionFactor) / 2 * domainSize
		if halfExpandableDomainSize < halfExpandableDomain {
			newDomainMax = shared.MaxKey
		} else {
			newDomainMax = self.keyDomainMin
			newDomainMax += 2 * halfExpandableDomainSize
		}
		self.numKeysAtLastRightDomainResize = self.numKeys
		self.numKeysBelowKeyDomain = 0
		outermostNode = self.LastDataNode()
	}

	if expansionFactor <= 1 {
		panic("Expansion factor must be greater than 1")
	}

	// Modify the root node appropriately
	// index of first pointer to a new node, and index of last pointer to a new node (exclusive)
	var newNodesStart, newNodesEnd int
	if root.NumChildren*expansionFactor <= self.maxFanout {
		self.numModelNodeExpansions++
		self.numModelNodeExpansionPointers += int64(root.NumChildren)

		newNumChildren := root.NumChildren * expansionFactor
		newChildren := make([]node.Node, newNumChildren)
		var copyStart int

		if expandLeft {
			copyStart = newNumChildren - root.NumChildren
			newNodesStart = 0
			newNodesEnd = copyStart
			root.GetLinearModel().B += float64(newNumChildren - root.NumChildren)
		} else {
			copyStart = 0
			newNodesStart = root.NumChildren
			newNodesEnd = newNumChildren
		}

		for i := 0; i < root.NumChildren; i++ {
			newChildren[copyStart+i] = root.Children[i]
		}

		root.Children = newChildren
		root.NumChildren = newNumChildren
	} else {
		newRoot := node.NewModelNode(root.GetLevel() - 1)
		newRoot.GetLinearModel().A = root.GetLinearModel().A / float64(root.NumChildren)
		newRoot.GetLinearModel().B = root.GetLinearModel().B / float64(root.NumChildren)

		if expandLeft {
			newRoot.GetLinearModel().B += float64(expansionFactor - 1)
		}

		newRoot.NumChildren = expansionFactor

		newRootChildren := make([]node.Node, expansionFactor)
		if expandLeft {
			newRootChildren[expansionFactor-1] = root
			newNodesStart = 0
		} else {
			newRootChildren[0] = root
			newNodesStart = 1
		}
		newRoot.Children = newRootChildren

		newNodesEnd = newNodesStart + expansionFactor - 1
		self.rootNode = newRoot
		self.updateSuperRootNodePointer()
		root = newRoot
	}

	// Determine if new nodes represent a range outside the key type's domain.
	// This happens when we're preventing overflows.
	inBoundsNewNodesStart, inBoundsNewNodesEnd := newNodesStart, newNodesEnd
	if expandLeft {
		inBoundsNewNodesStart = max(newNodesStart, self.rootNode.GetLinearModel().Predict(float64(newDomainMin)))
	} else {
		inBoundsNewNodesEnd = min(newNodesEnd, self.rootNode.GetLinearModel().Predict(float64(newDomainMax))+1)
	}

	// Fill newly created child pointers of the root node with new data nodes.
	// To minimize empty new data nodes, we create a new data node per n child
	// pointers, where n is the number of pointers to existing nodes.
	// Requires reassigning some keys from the outermost pre-existing data node
	// to the new data nodes.
	n := root.NumChildren - (newNodesEnd - newNodesStart)
	if root.NumChildren%n != 0 {
		panic("Root node's number of children must be a multiple of n")
	}
	newNodeDuplicationFactor := shared.Log2RoundDown(n)

	if expandLeft {
		leftBoundaryValue := self.keyDomainMin
		leftBoundary := outermostNode.LowerBound(leftBoundaryValue)
		next := outermostNode
		for i := newNodesEnd; i > newNodesStart; i -= n {
			rightBoundary := leftBoundary
			if i-n <= inBoundsNewNodesStart {
				leftBoundary = 0
			} else {
				leftBoundaryValue -= domainSize
				leftBoundary = outermostNode.LowerBound(leftBoundaryValue)
			}
			newNode := self.bulkLoadLeafNodeFromExisting(
				outermostNode,
				leftBoundary,
				rightBoundary,
				true,
				nil,
				false,
				false,
				false,
			)
			newNode.Level = root.Level + 1
			newNode.DuplicationFactor = newNodeDuplicationFactor

			if next != nil {
				next.PrevLeaf = newNode
			}

			newNode.NextLeaf = next
			next = newNode

			for j := i - 1; j >= i-n; j-- {
				root.Children[j] = newNode
			}
		}
	} else {
		rightBoundaryValue := self.keyDomainMax
		rightBoundary := outermostNode.LowerBound(rightBoundaryValue)
		var prev *node.DataNode = nil
		for i := newNodesStart; i < newNodesEnd; i += n {
			leftBoundary := rightBoundary
			if i+n >= inBoundsNewNodesEnd {
				rightBoundary = outermostNode.DataCapacity
			} else {
				rightBoundaryValue += domainSize
				rightBoundary = outermostNode.LowerBound(rightBoundaryValue)
			}
			newNode := self.bulkLoadLeafNodeFromExisting(
				outermostNode,
				leftBoundary,
				rightBoundary,
				true,
				nil,
				false,
				false,
				false,
			)
			newNode.Level = root.Level + 1
			newNode.DuplicationFactor = newNodeDuplicationFactor

			if prev != nil {
				prev.NextLeaf = newNode
			}

			newNode.PrevLeaf = prev
			prev = newNode

			for j := i; j < i+n; j++ {
				root.Children[j] = newNode
			}
		}
	}

	// Connect leaf nodes and remove reassigned keys from outermost pre-existing
	// node.
	if expandLeft {
		outermostNode.EraseRange(newDomainMin, self.keyDomainMin, false)
		lastNewLeaf := root.Children[newNodesEnd-1].(*node.DataNode)
		outermostNode.PrevLeaf = lastNewLeaf
		lastNewLeaf.NextLeaf = outermostNode
	} else {
		outermostNode.EraseRange(self.keyDomainMax, newDomainMax, true)
		firstNewLeaf := root.Children[newNodesStart].(*node.DataNode)
		outermostNode.NextLeaf = firstNewLeaf
		firstNewLeaf.PrevLeaf = outermostNode
	}
	self.keyDomainMin = newDomainMin
	self.keyDomainMax = newDomainMax
}

func (self *Index) updateSuperRootKeyDomain() {
	if !(self.numInserts == 0 || self.rootNode.IsLeaf()) {
		panic("Root node must be a leaf node if there are no inserts")
	}

	self.keyDomainMin = self.GetMinKey()
	self.keyDomainMax = self.GetMaxKey()
	self.numKeysAtLastRightDomainResize = self.numKeys
	self.numKeysAtLastLeftDomainResize = self.numKeys
	self.numKeysAboveKeyDomain = 0
	self.numKeysBelowKeyDomain = 0
	self.superRootNode.GetLinearModel().A = 1.0 / float64(self.keyDomainMax-self.keyDomainMin)
	self.superRootNode.GetLinearModel().B = -float64(self.keyDomainMin) * self.superRootNode.GetLinearModel().A
}

func (self *Index) linkDataNodes(
	oldLeaf *node.DataNode,
	leftLeaf *node.DataNode,
	rightLeaf *node.DataNode,
) {
	if oldLeaf.PrevLeaf != nil {
		oldLeaf.PrevLeaf.NextLeaf = leftLeaf
	}

	leftLeaf.PrevLeaf = oldLeaf.PrevLeaf
	leftLeaf.NextLeaf = rightLeaf

	rightLeaf.PrevLeaf = leftLeaf
	rightLeaf.NextLeaf = oldLeaf.NextLeaf

	if oldLeaf.NextLeaf != nil {
		oldLeaf.NextLeaf.PrevLeaf = rightLeaf
	}
}

func (self *Index) createTwoNewDataNodes(
	oldNode *node.DataNode,
	parentNode *node.ModelNode,
	duplicationFactor int,
	reuseModel bool,
	startBucketID int,
) {
	if duplicationFactor < 1 {
		panic("Duplication factor must be at least 1")
	}

	numBuckets := int(1 << duplicationFactor)
	endBucketID := startBucketID + numBuckets
	midBucketID := startBucketID + numBuckets/2

	appendMostlyRight := oldNode.IsAppendMostlyRight()
	appendingRightBucketID := min(max(parentNode.LinearModel.Predict(float64(oldNode.MaxKey)), 0), parentNode.NumChildren-1)

	appendMostlyLeft := oldNode.IsAppendMostlyLeft()
	appendingLeftBucketID := min(max(parentNode.LinearModel.Predict(float64(oldNode.MinKey)), 0), parentNode.NumChildren-1)

	rightBoundary := oldNode.LowerBound(shared.KeyType((float64(midBucketID) - parentNode.LinearModel.B) / parentNode.LinearModel.A))

	for rightBoundary < oldNode.DataCapacity &&
		oldNode.Keys[rightBoundary] != shared.KEndSentinel &&
		parentNode.LinearModel.Predict(float64(oldNode.Keys[rightBoundary])) < midBucketID {
		rightBoundary = min(
			oldNode.GetNextFilledPosition(rightBoundary, false)+1,
			oldNode.DataCapacity,
		)
	}

	leftLeaf := self.bulkLoadLeafNodeFromExisting(
		oldNode,
		0,
		rightBoundary,
		true,
		nil,
		reuseModel,
		appendMostlyRight && startBucketID <= appendingRightBucketID && appendingRightBucketID < midBucketID,
		appendMostlyLeft && startBucketID <= appendingLeftBucketID && appendingLeftBucketID < midBucketID,
	)

	rightLeaf := self.bulkLoadLeafNodeFromExisting(
		oldNode,
		rightBoundary,
		oldNode.DataCapacity,
		true,
		nil,
		reuseModel,
		appendMostlyRight && midBucketID <= appendingRightBucketID && appendingRightBucketID < endBucketID,
		appendMostlyLeft && midBucketID <= appendingLeftBucketID && appendingLeftBucketID < endBucketID,
	)

	leftLeaf.Level = parentNode.Level + 1
	rightLeaf.Level = parentNode.Level + 1

	leftLeaf.DuplicationFactor = duplicationFactor - 1
	rightLeaf.DuplicationFactor = duplicationFactor - 1

	for i := startBucketID; i < midBucketID; i++ {
		parentNode.Children[i] = leftLeaf
	}

	for i := midBucketID; i < endBucketID; i++ {
		parentNode.Children[i] = rightLeaf
	}

	self.linkDataNodes(oldNode, leftLeaf, rightLeaf)
}

func (self *Index) createNewDataNodes(
	oldNode *node.DataNode,
	parentNode *node.ModelNode,
	fanOutTreeDepth int,
	usedFanoutTree *[]*fanout_tree.FTNode,
	startBucketID int,
	extraDuplicationFactor int,
) {
	appendMostlyRight := oldNode.IsAppendMostlyRight()
	appendingRightBucketID := min(max(parentNode.LinearModel.Predict(float64(oldNode.MaxKey)), 0), parentNode.NumChildren-1)

	appendMostlyLeft := oldNode.IsAppendMostlyLeft()
	appendingLeftBucketID := min(max(parentNode.LinearModel.Predict(float64(oldNode.MinKey)), 0), parentNode.NumChildren-1)

	// Create the new data nodes
	currentBucketID := startBucketID // first bucket with same child
	prevLeaf := oldNode.PrevLeaf     // used for linking the new data nodes
	leftBoundary := 0
	rightBoundary := 0

	// Keys may be re-assigned to an adjacent fanout tree node due to off-by-one errors
	numReassignedKeys := 0
	for treeNode := range *usedFanoutTree {
		leftBoundary = rightBoundary
		duplicationFactor := fanOutTreeDepth - (*usedFanoutTree)[treeNode].Level + extraDuplicationFactor
		childNodeRepeats := 1 << duplicationFactor
		keepLeft := appendMostlyRight && currentBucketID <= appendingRightBucketID && appendingRightBucketID < currentBucketID+childNodeRepeats
		keepRight := appendMostlyLeft && currentBucketID <= appendingLeftBucketID && appendingLeftBucketID < currentBucketID+childNodeRepeats
		rightBoundary = (*usedFanoutTree)[treeNode].RightBoundary

		// Account for off-by-one errors due to floating-point precision issues.
		(*usedFanoutTree)[treeNode].NumKeys -= numReassignedKeys
		numReassignedKeys = 0
		for rightBoundary < oldNode.DataCapacity &&
			oldNode.Keys[rightBoundary] != shared.KEndSentinel &&
			parentNode.LinearModel.Predict(float64(oldNode.Keys[rightBoundary])) < currentBucketID+childNodeRepeats {
			numReassignedKeys++
			rightBoundary = min(
				oldNode.GetNextFilledPosition(rightBoundary, false)+1,
				oldNode.DataCapacity,
			)
		}
		(*usedFanoutTree)[treeNode].NumKeys += numReassignedKeys
		childNode := self.bulkLoadLeafNodeFromExisting(
			oldNode,
			leftBoundary,
			rightBoundary,
			false,
			(*usedFanoutTree)[treeNode],
			false,
			keepLeft,
			keepRight,
		)
		childNode.Level = parentNode.Level + 1
		childNode.Cost = (*usedFanoutTree)[treeNode].Cost
		childNode.DuplicationFactor = duplicationFactor
		childNode.ExpectedAvgExpSearchIterations = (*usedFanoutTree)[treeNode].ExpectedAvgSearchIterations
		childNode.ExpectedAvgShifts = (*usedFanoutTree)[treeNode].ExpectedAvgShifts
		childNode.PrevLeaf = prevLeaf

		if prevLeaf != nil {
			prevLeaf.NextLeaf = childNode
		}

		for i := currentBucketID; i < currentBucketID+childNodeRepeats; i++ {
			parentNode.Children[i] = childNode
		}

		currentBucketID += childNodeRepeats
		prevLeaf = childNode
	}
	prevLeaf.NextLeaf = oldNode.NextLeaf
	if oldNode.NextLeaf != nil {
		oldNode.NextLeaf.PrevLeaf = prevLeaf
	}
}

func (self *Index) splitDownwards(
	parentNode struct {
		*node.ModelNode
		int
	},
	bucketID int,
	fanoutTreeDepth int,
	usedFanoutTree *[]*fanout_tree.FTNode,
	reuseModel bool,
) *node.ModelNode {
	leaf := parentNode.Children[bucketID].(*node.DataNode)
	self.numDownwardSplits++
	self.numDownwardSplitKeys += int64(leaf.NumKeys)

	// Create the new model node that will replace the current data node
	fanout := 1 << fanoutTreeDepth
	newNode := node.NewModelNode(leaf.GetLevel())
	newNode.DuplicationFactor = leaf.DuplicationFactor
	newNode.NumChildren = fanout
	newNode.Children = make([]node.Node, fanout)

	repeats := int(1 << leaf.DuplicationFactor)
	startBucketID := bucketID - (bucketID % repeats) // first bucket with same child
	endBucketID := startBucketID + repeats           // first bucket with different child

	if parentNode.ModelNode.LinearModel.A == 0 {
		newNode.LinearModel.A = 0
		newNode.LinearModel.B = -(float64(startBucketID) - parentNode.ModelNode.LinearModel.B) / float64(repeats)
	} else {
		leftBoundaryValue := (float64(startBucketID) - parentNode.ModelNode.LinearModel.B) / parentNode.ModelNode.LinearModel.A
		rightBoundaryValue := (float64(endBucketID) - parentNode.ModelNode.LinearModel.B) / parentNode.ModelNode.LinearModel.A

		newNode.LinearModel.A = 1.0 / float64(rightBoundaryValue-leftBoundaryValue) * float64(fanout)
		newNode.LinearModel.B = -newNode.LinearModel.A * leftBoundaryValue
	}

	// Create new data nodes
	if len(*usedFanoutTree) == 0 {
		if fanoutTreeDepth != 1 {
			panic("Fanout tree depth must be 1")
		}
		self.createTwoNewDataNodes(
			leaf,
			newNode,
			fanoutTreeDepth,
			reuseModel,
			0,
		)
	} else {
		self.createNewDataNodes(
			leaf,
			newNode,
			fanoutTreeDepth,
			usedFanoutTree,
			0,
			0,
		)
	}

	self.numDataNodes--
	self.numModelNodes++
	for i := startBucketID; i < endBucketID; i++ {
		parentNode.Children[i] = newNode
	}
	if parentNode.ModelNode == self.superRootNode {
		self.rootNode = newNode
		self.updateSuperRootNodePointer()
	}
	return newNode
}

// Splits data node sideways in the manner determined by the fanout tree.
// If no fanout tree is provided, then splits sideways in two.
func (self *Index) splitSideways(
	parent struct {
		*node.ModelNode
		int
	},
	bucketID int,
	fanoutTreeDepth int,
	usedFanoutTree *[]*fanout_tree.FTNode,
	reuseModel bool,
) {
	leaf := parent.Children[bucketID].(*node.DataNode)
	self.numSidewaysSplits++
	self.numSidewaysSplitKeys += int64(leaf.NumKeys)

	fanout := 1 << fanoutTreeDepth
	repeats := 1 << leaf.DuplicationFactor

	if fanout > repeats {
		// Expand the pointer array in the parent model node if there are not
		// enough redundant pointers
		self.numModelNodeExpansions++
		self.numModelNodeExpansionPointers += int64(parent.NumChildren)
		expansionFactor := parent.Expand(fanoutTreeDepth - leaf.DuplicationFactor)
		repeats *= expansionFactor
		bucketID *= expansionFactor
	}

	startBucketID := bucketID - (bucketID % repeats) // first bucket with same child

	if len(*usedFanoutTree) == 0 {
		if fanoutTreeDepth != 1 {
			panic("Fanout tree depth must be 1")
		}
		self.createTwoNewDataNodes(
			leaf,
			parent.ModelNode,
			max(fanoutTreeDepth, leaf.DuplicationFactor),
			reuseModel,
			startBucketID,
		)
	} else {
		// Extra duplication factor is required when there are more redundant
		// pointers than necessary
		extraDuplication := max(0, leaf.DuplicationFactor-fanoutTreeDepth)
		self.createTwoNewDataNodes(leaf, parent.ModelNode, fanoutTreeDepth+extraDuplication, reuseModel, startBucketID)
	}
	self.numDataNodes--
}

// Insert will NOT do an update of an existing key.
// To perform an update or read-modify-write, do a lookup and modify the
// payload's value.
// Returns iterator to inserted element, and whether the insert happened or
// not.
// Insert does not happen if duplicates are not allowed and duplicate is
// found.
func (self *Index) Insert(key shared.KeyType, payload shared.PayloadType) error {
	if key > self.keyDomainMax {
		self.numKeysAboveKeyDomain++
		if self.shouldExpandRight() {
			self.expandRoot(key, false)
		}
	} else if key < self.keyDomainMin {
		self.numKeysBelowKeyDomain++
		if self.shouldExpandLeft() {
			self.expandRoot(key, true)
		}
	}

	leaf, _ := self.GetLeaf(key, false)
	_, err := leaf.Insert(key, payload)

	if errors.Is(err, shared.NoInsertionError) {
		return err
	}

	if err != nil {
		_, traversalPath := self.GetLeaf(key, true)
		parent := traversalPath[len(traversalPath)-1]

		for err != nil {
			self.numExpandAndScales += self.numResizes

			if parent.ModelNode == self.superRootNode {
				self.updateSuperRootKeyDomain()
			}

			bucketID := parent.ModelNode.GetLinearModel().Predict(float64(key))
			bucketID = min(max(bucketID, 0), parent.ModelNode.NumChildren-1)

			usedFanoutTree := make([]*fanout_tree.FTNode, 0)
			fanoutTreeDepth := 1
			if shared.SplittingPolicyMethod == 0 || (errors.Is(err, shared.MaxCapacityInsertionError) || errors.Is(err, shared.CatastrophicCostInsertionError)) {
				// always split in 2. No extra work required here
			} else if shared.SplittingPolicyMethod == shared.DecideBetweenNoSplittingOrSplittingInTwo {
				// decide between no split (i.e., expand and retrain) or splitting in 2
				fanoutTreeDepth = fanout_tree.FindBestFanoutExistingNode(parent.ModelNode, bucketID, self.numKeys, &usedFanoutTree, 2)
			} else if shared.SplittingPolicyMethod == shared.UseFullFanoutTree {
				// use full fanout tree to decide fanout
				fanoutTreeDepth = fanout_tree.FindBestFanoutExistingNode(parent.ModelNode, bucketID, self.numKeys, &usedFanoutTree, self.maxFanout)
			}
			bestFanout := 1 << fanoutTreeDepth

			if fanoutTreeDepth == 0 {
				leaf.Resize(
					shared.KMinDensity,
					true,
					leaf.IsAppendMostlyRight(),
					leaf.IsAppendMostlyLeft(),
				)
				treeNode := usedFanoutTree[0]
				leaf.Cost = treeNode.Cost
				leaf.ExpectedAvgExpSearchIterations = treeNode.ExpectedAvgSearchIterations
				leaf.ExpectedAvgShifts = treeNode.ExpectedAvgShifts
				leaf.ResetStats()
				self.numExpandAndRetrains++
			} else {
				// split data node: always try to split sideways/upwards, only split downwards if necessary
				reuseModel := errors.Is(err, shared.MaxCapacityInsertionError)
				if shared.AllowSplittingUpwards {
					if shared.SplittingPolicyMethod != shared.DecideBetweenNoSplittingOrSplittingInTwo {
						panic("Splitting upwards is only allowed when using the DecideBetweenNoSplittingOrSplittingInTwo splitting policy")
					}
					panic("Not implemented")
				} else {
					shouldSplitDownwards := parent.NumChildren*bestFanout/(1<<leaf.GetDuplicationFactor()) > self.maxFanout || parent.GetLevel() == self.superRootNode.GetLevel()

					if shouldSplitDownwards {
						parent.ModelNode = self.splitDownwards(
							parent,
							bucketID,
							fanoutTreeDepth,
							&usedFanoutTree,
							reuseModel,
						)
					} else {
						self.splitSideways(
							parent,
							bucketID,
							fanoutTreeDepth,
							&usedFanoutTree,
							reuseModel,
						)
					}
				}
				leaf = (*parent.ModelNode.GetChildNode(key)).(*node.DataNode)
			}

			// Try again to insert the key
			_, err = leaf.Insert(key, payload)
			if errors.Is(err, shared.NoInsertionError) {
				return err
			}
		}
		return nil
	}

	self.numInserts++
	self.numKeys++
	return nil
}

// Looks for an exact match of the key
func (self *Index) Find(key shared.KeyType) (*shared.PayloadType, error) {
	self.numLookups++
	leaf, _ := self.GetLeaf(key, false)
	idx, err := leaf.FindKeyPosition(key)
	if err != nil {
		return nil, err
	}
	return &leaf.Payloads[idx], nil
}

func NewIndex() *Index {
	index := &Index{
		superRootNode: nil,
		rootNode:      nil,

		traversalNode:         nil,
		traversalNodeBucketID: -1,

		expectedInsertFrac:          1.0,
		maxNodeSize:                 1 << 24,
		approximateModelComputation: true,
		approximateCostComputation:  false,

		maxFanout:        1 << 21,
		maxDataNodeSlots: (1 << 24) / shared.BlockSize,

		numKeys:                       0,
		numModelNodes:                 0,
		numDataNodes:                  0,
		numExpandAndScales:            0,
		numExpandAndRetrains:          0,
		numDownwardSplits:             0,
		numSidewaysSplits:             0,
		numModelNodeExpansions:        0,
		numModelNodeSplits:            0,
		numDownwardSplitKeys:          0,
		numSidewaysSplitKeys:          0,
		numModelNodeExpansionPointers: 0,
		numModelNodeSplitPointers:     0,
		numNodeLookups:                0,
		numLookups:                    0,
		numInserts:                    0,
		numResizes:                    0,
		splittingTime:                 0.0,
		costComputationTime:           0.0,

		keyDomainMax:                   shared.MinKey,
		keyDomainMin:                   shared.MaxKey,
		numKeysAboveKeyDomain:          0,
		numKeysBelowKeyDomain:          0,
		numKeysAtLastLeftDomainResize:  0,
		numKeysAtLastRightDomainResize: 0,

		stopCost:  0,
		splitCost: 0,
	}
	emptyDataNode := node.NewDataNode(1)
	emptyDataNode.BulkLoad(make([]shared.PayloadType, 0), 0, nil, false)

	index.rootNode = emptyDataNode
	index.numDataNodes++
	index.createSuperRoot()

	return index
}
