package httpz

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
)

// NewResponseWriter returns a new response writer.
func NewResponseWriter(rw http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{inner: rw}
}

// ResponseWriter wraps a response writer with status and content length information.
type ResponseWriter struct {
	inner         http.ResponseWriter
	statusCode    int
	contentLength int
}

// Write writes the data to the response.
func (rw *ResponseWriter) Write(b []byte) (int, error) {
	bytesWritten, err := rw.inner.Write(b)
	rw.contentLength += bytesWritten
	return bytesWritten, err
}

// Header accesses the response header collection.
func (rw *ResponseWriter) Header() http.Header {
	return rw.inner.Header()
}

// Hijack wraps response writer's Hijack function.
func (rw *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.inner.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("inner responseWriter doesn't support Hijacker interface")
	}
	return hijacker.Hijack()
}

// WriteHeader writes the status code (it is a somewhat poorly chosen method name from the standard library).
func (rw *ResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.inner.WriteHeader(code)
}

// Inner returns the backing writer.
func (rw *ResponseWriter) Inner() http.ResponseWriter {
	return rw.inner
}

// Flush calls flush on the inner response writer if it is supported.
func (rw *ResponseWriter) Flush() {
	if typed, ok := rw.inner.(http.Flusher); ok {
		typed.Flush()
	}
}

// StatusCode returns the status code.
func (rw *ResponseWriter) StatusCode() int {
	return rw.statusCode
}

// ContentLength returns the content length
func (rw *ResponseWriter) ContentLength() int {
	return rw.contentLength
}

// Close calls close on the inner response if it supports it.
func (rw *ResponseWriter) Close() error {
	if typed, ok := rw.inner.(io.Closer); ok {
		return typed.Close()
	}
	return nil
}
