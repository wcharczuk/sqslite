package httputil

import (
	"net/http"
	"strings"
)

// HeaderLastValue returns the last value of a potential csv of headers.
func HeaderLastValue(headers http.Header, key string) (string, bool) {
	if rawHeaderValue := headers.Get(key); rawHeaderValue != "" {
		if !strings.ContainsRune(rawHeaderValue, ',') {
			return strings.TrimSpace(rawHeaderValue), true
		}
		vals := strings.Split(rawHeaderValue, ",")
		return strings.TrimSpace(vals[len(vals)-1]), true
	}
	return "", false
}
