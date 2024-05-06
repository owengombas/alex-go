package node

import "alex_go/linear_model"

type Node interface {
	IsLeaf() bool

	// GetCost Could be either the expected or empirical Cost, depending on how this field is used
	GetCost() float64
	SetCost(cost float64)

	// GetLevel Node's Level in the RMI. Root node is Level 0
	GetLevel() int
	SetLevel(level int)

	// GetDuplicationFactor Power of 2 to which the pointer to this node is duplicated in its parent  model node
	// For example, if duplication_factor_ is 3, then there are 8 redundant
	// pointers to this node in its parent
	GetDuplicationFactor() int
	SetDuplicationFactor(duplicationFactor int)

	// GetLinearModel Both model nodes and data nodes nodes use models
	GetLinearModel() *linear_model.LinearModel
	SetLinearModel(linearModel linear_model.LinearModel)

	// GetNodeSize The size in bytes of all member variables in this class
	GetNodeSize() int64
}
