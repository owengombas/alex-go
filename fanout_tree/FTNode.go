package fanout_tree

import (
	"alex_go/linear_model"
	"alex_go/node"
	"alex_go/shared"
	"math"
	"sort"
	"unsafe"
)

// FTNode a node of the fanout tree
type FTNode struct {
	// level in the fanout tree
	Level int
	// node's position within its level
	NodeID int
	Cost   float64

	// start position in input array that this node represents
	LeftBoundary int
	// end position (exclusive) in input array that this node represents
	RightBoundary int

	Use                         bool
	ExpectedAvgSearchIterations float64
	ExpectedAvgShifts           float64
	NumKeys                     int

	// A linear model slope
	A float64
	// B linear model intercept
	B float64
}

// collectUsedNodes filters and collects nodes from the fanout tree.
func collectUsedNodes(fanoutTree [][]*FTNode, maxLevel int, usedFanoutTreeNodes *[]*FTNode) {
	// Limit maxLevel to the smaller of maxLevel or the number of levels in fanoutTree.
	if maxLevel >= len(fanoutTree) {
		maxLevel = len(fanoutTree) - 1
	}

	// Loop through each level up to maxLevel and collect used nodes.
	for i := 0; i <= maxLevel; i++ {
		level := fanoutTree[i]
		for _, treeNode := range level {
			if treeNode.Use {
				*usedFanoutTreeNodes = append(*usedFanoutTreeNodes, treeNode)
			}
		}
	}

	// Sort the collected nodes based on the comparison criteria.
	sort.Slice(*usedFanoutTreeNodes, func(i, j int) bool {
		left, right := (*usedFanoutTreeNodes)[i], (*usedFanoutTreeNodes)[j]
		return (left.NodeID << (maxLevel - left.Level)) < (right.NodeID << (maxLevel - right.Level))
	})
}

// mergeNodesUpwards attempts to merge nodes upwards in the fanout tree if it reduces the cost.
// It returns the new best cost.
func mergeNodesUpwards(startLevel int, bestCost float64, numKeys int, totalKeys int, fanoutTree [][]*FTNode) float64 {
	typeSize := float64(unsafe.Sizeof(node.NewDataNode(0)))

	for level := startLevel; level >= 1; level-- {
		levelFanout := 1 << level
		atLeastOneMerge := false
		for i := 0; i < levelFanout/2; i++ {
			if fanoutTree[level][2*i].Use && fanoutTree[level][2*i+1].Use {
				numNodeKeys := fanoutTree[level-1][i].NumKeys
				if numNodeKeys == 0 {
					fanoutTree[level][2*i].Use = false
					fanoutTree[level][2*i+1].Use = false
					fanoutTree[level-1][i].Use = true
					atLeastOneMerge = true
					bestCost -= shared.KModelSizeWeight * typeSize * float64(totalKeys) / float64(numKeys)
					continue
				}
				numLeftKeys := fanoutTree[level][2*i].NumKeys
				numRightKeys := fanoutTree[level][2*i+1].NumKeys
				mergingCostSaving := (fanoutTree[level][2*i].Cost * float64(numLeftKeys) / float64(numNodeKeys)) +
					(fanoutTree[level][2*i+1].Cost * float64(numRightKeys) / float64(numNodeKeys)) -
					fanoutTree[level-1][i].Cost +
					(shared.KModelSizeWeight * typeSize * float64(totalKeys) / float64(numNodeKeys))

				if mergingCostSaving >= 0 {
					fanoutTree[level][2*i].Use = false
					fanoutTree[level][2*i+1].Use = false
					fanoutTree[level-1][i].Use = true
					bestCost -= mergingCostSaving * float64(numNodeKeys) / float64(numKeys)
					atLeastOneMerge = true
				}
			}
		}
		if !atLeastOneMerge {
			break
		}
	}

	return bestCost
}

// FindBestFanoutExistingNode determines the optimal fanout for existing nodes.
func FindBestFanoutExistingNode(
	parent *node.ModelNode,
	bucketID int,
	totalKeys int,
	usedFanoutTreeNodes *[]*FTNode,
	maxFanout int,
) int {
	typeSize := float64(unsafe.Sizeof(node.NewDataNode(0)))
	node := parent.Children[bucketID].(*node.DataNode)
	numKeys := node.NumKeys
	bestLevel := 0
	bestCost := math.MaxFloat64
	fanoutCosts := make([]float64, 0)
	fanoutTree := make([][]*FTNode, 0)

	repeats := 1 << node.GetDuplicationFactor()
	startBucketID := bucketID - (bucketID % repeats)
	endBucketID := startBucketID + repeats
	baseModel := linear_model.NewLinearModel(0, 0)
	if parent.GetLinearModel().A == 0 {
		baseModel.A = 0
		baseModel.B = -1.0*float64(startBucketID) - parent.GetLinearModel().B/float64(repeats)
	} else {
		leftBoundaryValue := float64(startBucketID) - parent.GetLinearModel().B/parent.GetLinearModel().A
		rightBoundaryValue := float64(endBucketID) - parent.GetLinearModel().B/parent.GetLinearModel().A
		baseModel.A = 1.0 / (rightBoundaryValue - leftBoundaryValue)
		baseModel.B = -1.0 * baseModel.A * leftBoundaryValue
	}

	fanout, fanoutTreeLevel := 1, 0
	for fanout <= maxFanout {
		newLevel := make([]*FTNode, 0)
		cost := 0.0
		a := baseModel.A * float64(fanout)
		b := baseModel.A * float64(fanout)
		leftBoundary := 0
		rightBoundary := 0
		for i := 0; i < fanout; i++ {
			leftBoundary = rightBoundary
			// Implement lower_bound logic here similar to previous examples
			rightBoundary = node.DataCapacity
			if i == fanout-1 {
				rightBoundary = node.LowerBound(shared.KeyType((float64(i+1) - b) / a))
			}

			if leftBoundary == rightBoundary {
				newLevel = append(newLevel, &FTNode{
					fanoutTreeLevel,
					i,
					0,
					leftBoundary,
					rightBoundary,
					false,
					0,
					0,
					0,
					0,
					0,
				})
				continue
			}

			numActualKeys := 0
			model := linear_model.NewLinearModel(0, 0)
			modelBuilder := linear_model.NewLinearModelBuilder(model)
			node.IterateFilledPositions(func(key shared.KeyType, payload shared.PayloadType, i int) {
				modelBuilder.Add(float64(key), float64(i))
				numActualKeys++
			}, leftBoundary, rightBoundary)

			nodeCost := 0.0 // Placeholder for cost computation

			cost += nodeCost * float64(numActualKeys) / float64(numKeys)

			newLevel = append(newLevel, &FTNode{
				fanoutTreeLevel,
				i,
				nodeCost,
				leftBoundary,
				rightBoundary,
				false,
				0,
				0,
				numActualKeys,
				model.A,
				model.B,
			})
		}
		traversalCost := shared.KNodeLookupsWeight + (shared.KModelSizeWeight * float64(fanout) * (typeSize + float64(unsafe.Sizeof(uintptr(0)))) * float64(totalKeys) / float64(numKeys))
		cost += traversalCost
		fanoutCosts = append(fanoutCosts, cost)

		if len(fanoutCosts) >= 3 && fanoutCosts[len(fanoutCosts)-1] > fanoutCosts[len(fanoutCosts)-2] && fanoutCosts[len(fanoutCosts)-2] > fanoutCosts[len(fanoutCosts)-3] {
			break
		}

		if cost < bestCost {
			bestCost = cost
			bestLevel = fanoutTreeLevel
		}
		fanoutTree = append(fanoutTree, newLevel)

		fanout *= 2
		fanoutTreeLevel++
	}

	for n := range fanoutTree[bestLevel] {
		fanoutTree[bestLevel][n].Use = true
	}

	mergeNodesUpwards(bestLevel, bestCost, numKeys, totalKeys, fanoutTree)
	collectUsedNodes(fanoutTree, bestLevel, usedFanoutTreeNodes)

	return bestLevel
}
