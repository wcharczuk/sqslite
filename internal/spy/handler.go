package spy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wcharczuk/sqslite/internal/httpz"
)

var _ http.Handler = (*Handler)(nil)

// Handler implements [http.Handler] and captures the details of a request and response
// that flows through it, calling the [Handler.Do] function once the request and response complete.
//
// You can use the provided [WriteOutput] helper to take the [Request] metadata and serialize it as json and write
// to the given writer (i.e. [os.Stdout]).
//
// Use the [Handler.Next] field to wrap another handler with the capturing mechanics of the spy handler, for example
// you may want to add a reverse proxy as the next handler.
type Handler struct {
	Do   func(Request)
	Next http.Handler
}

// ServeHTTP implements [http.Handler].
func (l *Handler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var details Request
	start := time.Now()

	details.Method = req.Method
	details.URL = req.URL.String()
	details.RequestHeaders = make(map[string]string)

	for key, values := range req.Header {
		if strings.EqualFold(key, httpz.HeaderAuthorization) {
			details.RequestHeaders[key] = "<redacted>"
			continue
		}
		for _, value := range values {
			details.RequestHeaders[key] = value
			break
		}
	}
	requestBody := new(bytes.Buffer)
	if req.Body != nil {
		io.Copy(requestBody, req.Body)
		details.RequestBody = requestBody.String()
		req.Body = io.NopCloser(requestBody)
	}

	statusWriter := &responseWriter{inner: rw, response: new(bytes.Buffer)}

	// make the next request
	if l.Next != nil {
		l.Next.ServeHTTP(statusWriter, req)
	} else {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintf(rw, "OK!\n")
	}

	details.ResponseHeaders = make(map[string]string)
	for key, values := range statusWriter.inner.Header() {
		for _, value := range values {
			details.ResponseHeaders[key] = value
			break
		}
	}
	details.ResponseBody = statusWriter.response.String()
	details.StatusCode = http.StatusText(statusWriter.statusCode)
	details.Elapsed = time.Since(start)

	if l.Do != nil {
		l.Do(details)
	}
}
