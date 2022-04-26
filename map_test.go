package simplekv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSizedMapOp(t *testing.T) {
	assert := assert.New(t)

	m := NewSizedMap()
	m.Set("name", "pedro")
	assert.Equal(m.GetTotalSize(), 9)
	m.Set("age", 26)
	assert.Equal(m.GetTotalSize(), 14)
	m.Set("gender", "male")
	assert.Equal(m.GetTotalSize(), 24)
}
