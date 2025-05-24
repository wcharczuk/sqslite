package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"
)

func bindAddr() string {
	if value := os.Getenv("BIND_ADDR"); value != "" {
		return value
	}
	return ":4567"
}

func main() {
	s := &http.Server{
		Addr:    bindAddr(),
		Handler: loggedHandler(httputil.NewSingleHostReverseProxy(must(url.Parse("http://localhost:4566")))),
	}
	slog.Info("listening on bind address", slog.String("bind_addr", bindAddr()))
	if err := s.ListenAndServe(); err != nil {
		slog.Error("server exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}

func loggedHandler(h http.Handler) http.Handler {
	return &loggedHandlerImpl{
		next: h,
	}
}

type loggedHandlerImpl struct {
	next http.Handler
}

var outMu sync.Mutex

func (l loggedHandlerImpl) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var details httpRequest
	start := time.Now()
	details.Method = req.Method
	details.URL = req.URL.String()
	details.RequestHeaders = make(map[string]string)
	for key, values := range req.Header {
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

	statusWriter := &statusResponseWriter{inner: rw, response: new(bytes.Buffer)}
	l.next.ServeHTTP(statusWriter, req)
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

	outMu.Lock()
	_ = json.NewEncoder(os.Stdout).Encode(details)
	outMu.Unlock()
}

type httpRequest struct {
	Method     string
	URL        string
	StatusCode string

	RequestHeaders map[string]string
	RequestBody    string

	ResponseHeaders map[string]string
	ResponseBody    string

	Elapsed time.Duration
}

// StatusResponseWriter a better response writer
type statusResponseWriter struct {
	inner         http.ResponseWriter
	response      *bytes.Buffer
	statusCode    int
	contentLength int
}

// Write writes the data to the response.
func (rw *statusResponseWriter) Write(b []byte) (int, error) {
	_, _ = rw.response.Write(b)
	bytesWritten, err := rw.inner.Write(b)
	rw.contentLength += bytesWritten
	return bytesWritten, err
}

// Header accesses the response header collection.
func (rw *statusResponseWriter) Header() http.Header {
	return rw.inner.Header()
}

// Hijack wraps response writer's Hijack function.
func (rw *statusResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.inner.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("inner responseWriter doesn't support Hijacker interface")
	}
	return hijacker.Hijack()
}

// WriteHeader writes the status code (it is a somewhat poorly chosen method name from the standard library).
func (rw *statusResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.inner.WriteHeader(code)
}

// Inner returns the backing writer.
func (rw *statusResponseWriter) Inner() http.ResponseWriter {
	return rw.inner
}

// Flush calls flush on the inner response writer if it is supported.
func (rw *statusResponseWriter) Flush() {
	if typed, ok := rw.inner.(http.Flusher); ok {
		typed.Flush()
	}
}

// StatusCode returns the status code.
func (rw *statusResponseWriter) StatusCode() int {
	return rw.statusCode
}

// ContentLength returns the content length
func (rw *statusResponseWriter) ContentLength() int {
	return rw.contentLength
}

// Close calls close on the inner response if it supports it.
func (rw *statusResponseWriter) Close() error {
	if typed, ok := rw.inner.(io.Closer); ok {
		return typed.Close()
	}
	return nil
}
