package simplekv

import (
	"encoding/json"
)

func sizeof(v interface{}) int {
	if s, ok := v.(string); ok {
		return len(s)
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	// size := binary.Size(v)
	return len(bytes)
}
