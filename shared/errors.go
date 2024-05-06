package shared

import "errors"

var NoGapFoundError = errors.New("no gap found")
var KeyNotFoundError = errors.New("key not found")
var CatastrophicCostInsertionError = errors.New("catastrophic cost insertion")
var SignificantCostDeviationInsertionError = errors.New("significant cost insertion")
var MaxCapacityInsertionError = errors.New("max capacity insertion")
var NoInsertionError = errors.New("no insertion")
