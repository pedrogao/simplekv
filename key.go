package simplekv

type keyType string

func (n keyType) LessThan(b interface{}) bool {
	value, _ := b.(keyType)
	return n < value
}
