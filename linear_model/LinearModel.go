package linear_model

type LinearModel struct {
	A float64
	B float64
}

func (lin *LinearModel) Predict(x float64) int {
	return int(lin.A*x + lin.B)
}

func (lin *LinearModel) PredictDouble(x float64) float64 {
	return lin.A*x + lin.B
}

func (lin *LinearModel) Expand(factor float64) {
	lin.A *= factor
	lin.B *= factor
}

func NewLinearModel(a float64, b float64) *LinearModel {
	return &LinearModel{A: a, B: b}
}

func CopyLinearModel(lin *LinearModel) *LinearModel {
	return &LinearModel{A: lin.A, B: lin.B}
}
