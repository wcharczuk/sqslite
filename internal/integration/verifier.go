package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

func NewVerifier(sourceFile string) (*Verifier, error) {
	f, err := os.Open(sourceFile)
	if err != nil {
		return nil, fmt.Errorf("unable to open verifier source file for read: %w", err)
	}
	return &Verifier{
		sourceFile: f,
		scanner:    bufio.NewScanner(f),
		failures:   make(chan *VerificationFailure),
	}, nil
}

type Verifier struct {
	mu         sync.Mutex
	sourceFile *os.File
	scanner    *bufio.Scanner
	failures   chan *VerificationFailure
}

func (v *Verifier) Close() error {
	return v.sourceFile.Close()
}

func (v *Verifier) VerificationFailures() chan *VerificationFailure {
	return v.failures
}

func (v *Verifier) HandleRequest(actualReq spy.Request) {
	expectedReq, ok := v.getNextExpectedResult()
	if !ok {
		v.failures <- &VerificationFailure{
			Actual:  actualReq,
			Message: "Unexpected actual request",
		}
	}
	if err := v.verifyRequest(expectedReq, actualReq); err != nil {
		v.failures <- err
	}
}

var (
	requestHeadersShouldMatch = []string{
		sqslite.HeaderAmzTarget,
		sqslite.HeaderAmzQueryMode,
	}
	responseHeadersShouldMatch = []string{
		"Content-Type",
	}
	responseHeadersShouldBePresent = []string{
		sqslite.HeaderAmznRequestID,
	}
)

func (v *Verifier) verifyRequest(expectedReq, actualReq spy.Request) *VerificationFailure {
	for _, requiredHeader := range requestHeadersShouldMatch {
		expectedValue, ok := expectedReq.RequestHeaders[requiredHeader]
		if !ok {
			break
		}
		actualValue, ok := actualReq.RequestHeaders[requiredHeader]
		if !ok {
			return &VerificationFailure{
				Expected: expectedReq,
				Actual:   actualReq,
				Message:  fmt.Sprintf("missing actual request header value for key %q", requiredHeader),
			}
		}
		if expectedValue != actualValue {
			return &VerificationFailure{
				Expected: expectedReq,
				Actual:   actualReq,
				Message: fmt.Sprintf(
					"incorrect actual request header value for key %q; expected %q vs. actual %q",
					requiredHeader,
					expectedValue,
					actualValue,
				),
			}
		}
	}
	for _, requiredHeader := range responseHeadersShouldMatch {
		expectedValue, ok := expectedReq.ResponseHeaders[requiredHeader]
		if !ok {
			break
		}
		actualValue, ok := actualReq.ResponseHeaders[requiredHeader]
		if !ok {
			return &VerificationFailure{
				Expected: expectedReq,
				Actual:   actualReq,
				Message:  fmt.Sprintf("missing actual response header value for key %q", requiredHeader),
			}
		}
		if expectedValue != actualValue {
			return &VerificationFailure{
				Expected: expectedReq,
				Actual:   actualReq,
				Message: fmt.Sprintf(
					"incorrect actual response header value for key %q; expected %q vs. actual %q",
					requiredHeader,
					expectedValue,
					actualValue,
				),
			}
		}
	}
	for _, maybePresentHeader := range responseHeadersShouldBePresent {
		_, ok := expectedReq.ResponseHeaders[maybePresentHeader]
		if !ok {
			break
		}
		_, ok = actualReq.ResponseHeaders[maybePresentHeader]
		if !ok {
			return &VerificationFailure{
				Expected: expectedReq,
				Actual:   actualReq,
				Message:  fmt.Sprintf("missing actual response header value for key %q", maybePresentHeader),
			}
		}
	}
	return nil
}

func (v *Verifier) getNextExpectedResult() (expectedReq spy.Request, ok bool) {
	ok = v.scanner.Scan()
	if !ok {
		return
	}
	line := v.scanner.Text()
	_ = json.Unmarshal([]byte(line), &expectedReq)
	return
}

type VerificationFailure struct {
	Actual   spy.Request
	Expected spy.Request
	Message  string
}

func (v VerificationFailure) Error() string {
	return "verification failure"
}
