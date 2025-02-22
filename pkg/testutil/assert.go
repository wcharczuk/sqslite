package testutil

import (
	"fmt"
	"testing"
)

func Assert(t *testing.T, passed bool, msg ...any) {
	t.Helper()
	if !passed {
		if len(msg) > 0 {
			t.Errorf("assertion failed: %s", fmt.Sprint(msg...))
		} else {
			t.Errorf("assertion failed")
		}
		t.Fail()
	}
}

func Refute(t *testing.T, passed bool, msg ...any) {
	t.Helper()
	if passed {
		if len(msg) > 0 {
			t.Errorf("refutation failed: %s", fmt.Sprint(msg...))
		} else {
			t.Errorf("refutation failed")
		}
		t.Fail()
	}
}
