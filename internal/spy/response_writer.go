package spy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
)

type responseWriter struct {
	inner         http.ResponseWriter
	response      *bytes.Buffer
	statusCode    int
	contentLength int
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	_, _ = rw.response.Write(b)
	bytesWritten, err := rw.inner.Write(b)
	rw.contentLength += bytesWritten
	return bytesWritten, err
}

func (rw *responseWriter) Header() http.Header {
	return rw.inner.Header()
}

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.inner.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("inner responseWriter doesn't support Hijacker interface")
	}
	return hijacker.Hijack()
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.inner.WriteHeader(code)
}

func (rw *responseWriter) Inner() http.ResponseWriter {
	return rw.inner
}

func (rw *responseWriter) Flush() {
	if typed, ok := rw.inner.(http.Flusher); ok {
		typed.Flush()
	}
}

func (rw *responseWriter) StatusCode() int {
	return rw.statusCode
}

func (rw *responseWriter) ContentLength() int {
	return rw.contentLength
}

func (rw *responseWriter) Close() error {
	if typed, ok := rw.inner.(io.Closer); ok {
		return typed.Close()
	}
	return nil
}
