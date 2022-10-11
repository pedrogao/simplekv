package simplekv

type keyType string

func (n keyType) LessThan(b any) bool {
	value, _ := b.(keyType)
	return n < value
}
