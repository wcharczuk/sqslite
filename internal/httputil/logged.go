package httputil

import (
	"log/slog"
	"net/http"
	"time"
)

// Logged wraps an input handler with logging at [slog.LevelDebug] level.
func Logged(h http.Handler) http.Handler {
	return &logged{
		next: h,
	}
}

type logged struct {
	next http.Handler
}

func (l logged) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	start := time.Now()
	srw := NewResponseWriter(rw)
	l.next.ServeHTTP(srw, req)
	attributes := []any{
		slog.String("verb", req.Method),
		slog.String("url", req.URL.String()),
		slog.String("user_agent", req.UserAgent()),
		slog.String("remote_addr", GetRemoteAddr(req)),
		slog.String("status_code", http.StatusText(srw.StatusCode())),
		slog.Duration("elapsed", time.Since(start)),
	}
	if target := req.Header.Get("X-Amz-Target"); target != "" {
		attributes = append(attributes, slog.String("method", target))
	}
	if contentType := rw.Header().Get(HeaderContentType); contentType != "" {
		attributes = append(attributes, slog.String("content-type", contentType))
	}
	if contentEncoding := rw.Header().Get(HeaderContentEncoding); contentEncoding != "" {
		attributes = append(attributes, slog.String("content-encoding", contentEncoding))
	}
	slog.Debug("http-request", attributes...)
}
