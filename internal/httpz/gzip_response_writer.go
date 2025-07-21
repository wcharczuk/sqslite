package httpz

import (
	"compress/gzip"
	"net/http"
)

// NewGzipResponseWriter returns a new gzipped response writer.
func NewGzipResponseWriter(w http.ResponseWriter) *GzipResponseWriter {
	if typed, ok := w.(*ResponseWriter); ok {
		return &GzipResponseWriter{
			innerResponse: typed.Inner(),
			gzipWriter:    gzip.NewWriter(typed.Inner()),
		}
	}
	return &GzipResponseWriter{
		innerResponse: w,
		gzipWriter:    gzip.NewWriter(w),
	}
}

// GzipResponseWriter is a response writer that compresses output.
type GzipResponseWriter struct {
	gzipWriter    *gzip.Writer
	innerResponse http.ResponseWriter
	statusCode    int
	contentLength int
}

// InnerResponse returns the underlying response.
func (crw *GzipResponseWriter) InnerResponse() http.ResponseWriter {
	return crw.innerResponse
}

// Write writes the byes to the stream.
func (crw *GzipResponseWriter) Write(b []byte) (n int, err error) {
	n, err = crw.gzipWriter.Write(b)
	if err != nil {
		return
	}
	crw.contentLength += n
	return
}

// Header returns the headers for the response.
func (crw *GzipResponseWriter) Header() http.Header {
	return crw.innerResponse.Header()
}

// WriteHeader writes a status code.
func (crw *GzipResponseWriter) WriteHeader(code int) {
	crw.statusCode = code
	crw.innerResponse.WriteHeader(code)
}

// StatusCode returns the status code for the request.
func (crw *GzipResponseWriter) StatusCode() int {
	return crw.statusCode
}

// ContentLength returns the content length for the request.
func (crw *GzipResponseWriter) ContentLength() int {
	return crw.contentLength
}

// Flush pushes any buffered data out to the response.
func (crw *GzipResponseWriter) Flush() {
	crw.gzipWriter.Flush()
}

// Close closes any underlying resources.
func (crw *GzipResponseWriter) Close() error {
	return crw.gzipWriter.Close()
}
