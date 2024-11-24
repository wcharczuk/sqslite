package uuid

import (
	"fmt"
	"testing"
)

func Test_V4(t *testing.T) {
	m := make(map[string]bool)
	for x := 1; x < 32; x++ {
		uuid := V4()
		s := fmt.Sprintf("%+v", uuid)
		if m[s] {
			t.Errorf("NewRandom returned duplicated UUID %s\n", s)
		}
		m[s] = true
		if v := uuid.Version(); v != 4 {
			t.Errorf("Random UUID of version %v\n", v)
		}
	}
}
