package spy

import (
	"time"
)

// Request holds metadata on http requests that flow through the spy proxy.
//
// It is meant to be easy to serialize as JSON for output.
type Request struct {
	Method          string            `validate:"mode=cmp" json:"method"`
	URL             string            `validate:"mode=cmp" json:"url"`
	StatusCode      string            `validate:"mode=cmp" json:"status_code"`
	RequestHeaders  map[string]string `validate:"mode=deep_equal" json:"request_headers"`
	RequestBody     string            `validate:"mode=typed_compare" json:"request_body"`
	ResponseHeaders map[string]string `validate:"mode=deep_equal" json:"response_headers"`
	ResponseBody    string            `validate:"mode=typed_compare" json:"response_body"`
	Elapsed         time.Duration     `validate:"mode=skip" json:"elapsed"`
}
