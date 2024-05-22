package linear_model

import (
	"alex_go/shared"
	"math"
)

type LinearModelBuilder struct {
	model *LinearModel
	count uint64
	xSum  float64
	ySum  float64
	xxSum float64
	xySum float64
	xMin  float64
	xMax  float64
	yMin  float64
	yMax  float64
}

func NewLinearModelBuilder(model *LinearModel) *LinearModelBuilder {
	return &LinearModelBuilder{
		model: model,
		count: 0,
		xSum:  0.0,
		ySum:  0.0,
		xxSum: 0.0,
		xySum: 0.0,
		xMin:  shared.MaxKey,
		xMax:  shared.MinKey,
		yMin:  math.MaxFloat64,
		yMax:  math.SmallestNonzeroFloat64,
	}
}

func (self *LinearModelBuilder) Add(x float64, y float64) {
	self.count++
	self.xSum += x
	self.ySum += y
	self.xxSum += x * x
	self.xySum += x * y

	self.xMin = min(x, self.xMin)
	self.xMax = max(x, self.xMax)
	self.yMin = min(y, self.yMin)
	self.yMax = max(y, self.yMax)
}

func (self *LinearModelBuilder) Build() {
	if self.count <= 1 {
		self.model.A = 0.0
		self.model.B = self.ySum
		return
	}

	// Zero variance check, fit horizontal line
	if float64(self.count)*self.xxSum-self.xSum*self.xSum == 0.0 {
		self.model.A = 0.0
		self.model.B = self.ySum / float64(self.count)
		return
	}

	slope := (float64(self.count)*self.xySum - self.xSum*self.ySum) / (float64(self.count)*self.xxSum - self.xSum*self.xSum)
	intercept := (self.ySum - slope*self.xSum) / float64(self.count)
	self.model.A = slope
	self.model.B = intercept

	// If floating point precision errors, fit spline
	if self.model.A <= 0.0 {
		if self.xMax-self.xMin == 0.0 {
			self.model.A = 0.0
			self.model.B = self.ySum / float64(self.count)
		} else {
			self.model.A = (self.yMax - self.yMin) / (self.xMax - self.xMin)
			self.model.B = -self.xMin * self.model.A
		}
	}
}
