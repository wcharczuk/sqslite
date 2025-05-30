package spy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Handler struct {
	Out   io.Writer
	Next  http.Handler
	outMu sync.Mutex
}

func (l *Handler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var details Request
	start := time.Now()

	details.Method = req.Method
	details.URL = req.URL.String()
	details.RequestHeaders = make(map[string]string)

	for key, values := range req.Header {
		if key == "Authorization" {
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

	l.outMu.Lock()
	_ = json.NewEncoder(l.Out).Encode(details)
	l.outMu.Unlock()
}
