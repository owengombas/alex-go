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

func (b *LinearModelBuilder) Add(x, y float64) {
	b.count++
	b.xSum += x
	b.ySum += y
	b.xxSum += x * x
	b.xySum += x * y

	b.xMin = min(x, b.xMin)
	b.xMax = max(x, b.xMax)
	b.yMin = min(y, b.yMin)
	b.yMax = max(y, b.yMax)
}

func (b *LinearModelBuilder) Build() {
	if b.count <= 1 {
		b.model.A = 0.0
		b.model.B = b.ySum
		return
	}

	if float64(b.count)*b.xxSum-b.xSum*b.xSum == 0.0 {
		b.model.A = 0.0
		b.model.B = b.ySum / float64(b.count)
		return
	}

	slope := (float64(b.count)*b.xySum - b.xSum*b.ySum) / (float64(b.count)*b.xxSum - b.xSum*b.xSum)
	intercept := (b.ySum - slope*b.xSum) / float64(b.count)
	b.model.A = slope
	b.model.B = intercept

	if b.model.A <= 0.0 {
		if b.xMax-b.xMin == 0.0 {
			b.model.A = 0.0
			b.model.B = b.ySum / float64(b.count)
		} else {
			b.model.A = (b.yMax - b.yMin) / (b.xMax - b.xMin)
			b.model.B = -b.xMin * b.model.A
		}
	}
}
