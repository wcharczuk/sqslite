package spy

import (
	"encoding/json"
	"io"
	"sync"
)

// WriteOutput returns a function that performs an interlocked write to
// a given output. The lock is referenced from the transitive closure of the
// funciton returned.
func WriteOutput(output io.Writer) func(Request) {
	encoderMu := &sync.Mutex{}
	encoder := json.NewEncoder(output)
	return func(details Request) {
		encoderMu.Lock()
		defer encoderMu.Unlock()
		_ = encoder.Encode(details)
	}
}
