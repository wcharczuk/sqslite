package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"

	"github.com/pmezard/go-difflib/difflib"
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
		slog.Error("verification failure")
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
	if expectedReq.Method != actualReq.Method {
		return &VerificationFailure{
			Expected: expectedReq,
			Actual:   actualReq,
			Message:  fmt.Sprintf("expected http verb %q, got %q", expectedReq.Method, actualReq.Method),
		}
	}
	if expectedReq.StatusCode != actualReq.StatusCode {
		return &VerificationFailure{
			Expected: expectedReq,
			Actual:   actualReq,
			Message:  fmt.Sprintf("expected status code %q, got %q", expectedReq.StatusCode, actualReq.StatusCode),
		}
	}
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

	var expectedRes, actualRes any
	if err := json.Unmarshal([]byte(expectedReq.ResponseBody), &expectedRes); err != nil {
		return &VerificationFailure{
			Expected: expectedReq,
			Actual:   actualReq,
			Message:  fmt.Sprintf("unable to deserialize expected response body: %v", err),
		}
	}
	if err := json.Unmarshal([]byte(actualReq.ResponseBody), &actualRes); err != nil {
		return &VerificationFailure{
			Expected: expectedReq,
			Actual:   actualReq,
			Message:  fmt.Sprintf("unable to deserialize actual response body: %v", err),
		}
	}
	if !reflect.DeepEqual(expectedRes, actualRes) {
		diff := diffBodies(expectedRes, actualRes)
		return &VerificationFailure{
			Expected: expectedReq,
			Actual:   actualReq,
			Message:  diff,
		}
	}

	return &VerificationFailure{
		Expected: expectedReq,
		Actual:   actualReq,
		Message:  "test failure!",
	}
	//return nil
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
	if v.Message != "" {
		return fmt.Sprintf("verification failure: %s", v.Message)
	}
	return "verification failure"
}

func diffBodies(expected, actual any) string {
	diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(marshalPrettyJSON(expected)),
		B:        difflib.SplitLines(marshalPrettyJSON(actual)),
		FromFile: "Expected",
		FromDate: "",
		ToFile:   "Actual",
		ToDate:   "",
		Context:  1,
	})
	return diff
}
