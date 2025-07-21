package httpz

import (
	"net/http"
	"strings"
)

// HeaderAny will return true if for a given key, any header values match a given value.
//
// If a header is a csv, similarly, the value will be split on comma and the value will
// be checked on the csv elements.
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
