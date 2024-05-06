package node

import "C"
import (
	"alex_go/linear_model"
	"alex_go/shared"
	"unsafe"
)

type ModelNode struct {
	// Parameters from the Node interface
	DuplicationFactor int
	Level             int
	LinearModel       linear_model.LinearModel
	Cost              float64

	// Array of Children
	Children []Node

	// Number of logical Children. Must be a power of 2
	NumChildren int
}

func (self *ModelNode) GetChildNode(key shared.KeyType) *Node {
	bucketId := self.LinearModel.Predict(float64(key))
	bucketId = min(max(bucketId, 0), self.NumChildren-1)
	return &self.Children[bucketId]
}

func (self *ModelNode) Expand(log2ExpansionFactor int) int {
	expansionFactor := 1 << log2ExpansionFactor
	numNewChildren := self.NumChildren * expansionFactor
	newChildren := make([]Node, numNewChildren)
	currentIndex := 0
	for currentIndex < self.NumChildren {
		currentChild := &self.Children[currentIndex]
		currentChildDuplicationFactor := (*currentChild).GetDuplicationFactor()
		currentChildRepeats := 1 << currentChildDuplicationFactor
		for i := expansionFactor * currentIndex; i < expansionFactor*(currentIndex+currentChildRepeats); i++ {
			newChildren[i] = *currentChild
		}
		(*currentChild).SetDuplicationFactor(currentChildDuplicationFactor + log2ExpansionFactor)
		currentIndex += currentChildRepeats
	}
	self.Children = newChildren
	self.NumChildren = numNewChildren
	self.LinearModel.Expand(float64(expansionFactor))
	return numNewChildren
}

func (self *ModelNode) IsLeaf() bool {
	return false
}

func (self *ModelNode) GetCost() float64 {
	return self.Cost
}

func (self *ModelNode) SetCost(cost float64) {
	self.Cost = cost
}

func (self *ModelNode) GetLevel() int {
	return self.Level
}

func (self *ModelNode) SetLevel(level int) {
	self.Level = level
}

func (self *ModelNode) GetDuplicationFactor() int {
	return self.DuplicationFactor
}

func (self *ModelNode) SetDuplicationFactor(duplicationFactor int) {
	self.DuplicationFactor = duplicationFactor
}

func (self *ModelNode) GetLinearModel() *linear_model.LinearModel {
	return &self.LinearModel
}

func (self *ModelNode) SetLinearModel(linearModel linear_model.LinearModel) {
	self.LinearModel = linearModel
}

func (self *ModelNode) GetNodeSize() int64 {
	size := int64(unsafe.Sizeof(*self))
	// Pointers to Children
	size += int64(self.NumChildren) * int64(unsafe.Sizeof(uintptr(0)))
	return size
}

func NewModelNode(level int) *ModelNode {
	return &ModelNode{
		DuplicationFactor: 0,
		Level:             level,
		LinearModel:       linear_model.LinearModel{},
		Cost:              0.0,
		Children:          nil,
	}
}
