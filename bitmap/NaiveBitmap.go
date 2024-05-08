package bitmap

type NaiveBitmap struct {
	bits  []bool
	count int
}

func (n *NaiveBitmap) Count() int {
	return n.count
}

func (n *NaiveBitmap) Contains(value uint32) bool {
	return n.bits[value]
}

func (n *NaiveBitmap) Set(value uint32) {
	n.bits[value] = true
	n.count++
}

func (n *NaiveBitmap) Remove(value uint32) {
	n.bits[value] = false
	n.count--
}

func NewNaiveBitmap(dataCapacity int) *NaiveBitmap {
	return &NaiveBitmap{
		bits:  make([]bool, dataCapacity),
		count: 0,
	}
}
