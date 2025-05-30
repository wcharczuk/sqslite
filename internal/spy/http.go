package spy

import (
	"time"
)

type Request struct {
	Method     string
	URL        string
	StatusCode string

	RequestHeaders map[string]string
	RequestBody    string

	ResponseHeaders map[string]string
	ResponseBody    string

	Elapsed time.Duration
}
