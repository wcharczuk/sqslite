package httpz

import (
	"net/http"
)

// Gzipped wraps a given handler as a new handler that if requests
// accept the gzip encoding, compresses responses using a [gzip.Writer].
//
// If the request does not accept gzip encoding via. the accept-encoding header
// the original handler will be called directly.
func Gzipped(h http.Handler) http.Handler {
	return &gzipped{
		next: h,
	}
}

type gzipped struct {
	next http.Handler
}

func (g gzipped) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if HeaderAny(req.Header, HeaderAcceptEncoding, ContentEncodingGZIP) {
		gzrw := NewGzipResponseWriter(rw)
		defer gzrw.Close()
		gzrw.Header().Set(HeaderContentEncoding, ContentEncodingGZIP)
		gzrw.Header().Set(HeaderVary, HeaderAcceptEncoding)
		g.next.ServeHTTP(gzrw, req)
		return
	}
	g.next.ServeHTTP(rw, req)
}
