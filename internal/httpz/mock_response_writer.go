package httpz

import (
	"io"
	"net/http"
)

func NewMockResponseWriter(wr io.Writer) *MockResponseWriter {
	return &MockResponseWriter{
		inner:   wr,
		headers: http.Header{},
	}
}

// ResponseWriter wraps a response writer with status and content length information.
type MockResponseWriter struct {
	inner         io.Writer
	headers       http.Header
	statusCode    int
	contentLength int
}

// Write writes the data to the response.
func (rw *MockResponseWriter) Write(b []byte) (int, error) {
	bytesWritten, err := rw.inner.Write(b)
	rw.contentLength += bytesWritten
	return bytesWritten, err
}

// Header accesses the response header collection.
func (rw *MockResponseWriter) Header() http.Header {
	return rw.headers
}

// WriteHeader writes the status code (it is a somewhat poorly chosen method name from the standard library).
func (rw *MockResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
}

// StatusCode returns the status code.
func (rw *MockResponseWriter) StatusCode() int {
	return rw.statusCode
}

// ContentLength returns the content length
func (rw *MockResponseWriter) ContentLength() int {
	return rw.contentLength
}

// Close calls close on the inner response if it supports it.
func (rw *MockResponseWriter) Close() error {
	if typed, ok := rw.inner.(io.Closer); ok {
		return typed.Close()
	}
	return nil
}
