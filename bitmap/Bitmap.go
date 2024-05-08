package bitmap

type Bitmap interface {
	Count() int
	Contains(value uint32) bool
	Set(value uint32)
	Remove(value uint32)
}
