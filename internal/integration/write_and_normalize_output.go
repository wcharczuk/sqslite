package integration

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/wcharczuk/sqslite/internal/spy"
)

func WriteOutput(output io.Writer) func(spy.Request) {
	encoderMu := &sync.Mutex{}
	encoder := json.NewEncoder(output)
	return func(details spy.Request) {
		encoderMu.Lock()
		defer encoderMu.Unlock()
		_ = encoder.Encode(details)
	}
}
