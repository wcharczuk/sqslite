package httputil

import (
	"net"
	"net/http"
)

// GetRemoteAddr gets the origin/client ip for a request.
//
// The following headers are considered:
// - X-FORWARDED-FOR: If multiple IPs are included the first one is returned.
// - X-REAL-IP: If multiple IPs are included the last one is returned
//
// Finally the [http.Request.RemoteAddr] field is returned if no other headers are present.
func GetRemoteAddr(r *http.Request) string {
	if r == nil {
		return ""
	}
	tryHeader := func(key string) (string, bool) {
		return HeaderLastValue(r.Header, key)
	}
	for _, header := range []string{HeaderXForwardedFor, HeaderXRealIP} {
		if headerVal, ok := tryHeader(header); ok {
			return headerVal
		}
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}
