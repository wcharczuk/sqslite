package httputil

import (
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// LoggedHandler wraps a handler and adds logging via. [slog.Info].
func LoggedHandler(h http.Handler) http.Handler {
	return &loggedHandler{
		next: h,
	}
}

type loggedHandler struct {
	next http.Handler
}

var outMu sync.Mutex

func (l loggedHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
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
	slog.Debug("http-request", attributes...)
}
