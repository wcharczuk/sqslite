package httputil

import (
	"net/http"
	"strings"
)

// HeaderAny returns if any pieces of a header match a given value.
func HeaderAny(headers http.Header, key, value string) bool {
	if rawHeaderValue := headers.Get(key); rawHeaderValue != "" {
		if !strings.ContainsRune(rawHeaderValue, ',') {
			return strings.TrimSpace(rawHeaderValue) == value
		}
		for headerValue := range strings.SplitSeq(rawHeaderValue, ",") {
			if strings.TrimSpace(headerValue) == value {
				return true
			}
		}
	}
	return false
}
