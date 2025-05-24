package httputil

import (
	"net"
	"net/http"
	"strings"
)

// GetRemoteAddr gets the origin/client ip for a request.
// X-FORWARDED-FOR is checked. If multiple IPs are included the first one is returned
// X-REAL-IP is checked. If multiple IPs are included the last one is returned
// Finally r.RemoteAddr is used
// Only benevolent services will allow access to the real IP.
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

// Header names in canonical form.
const (
	HeaderAccept                  = "Accept"
	HeaderAcceptEncoding          = "Accept-Encoding"
	HeaderAllow                   = "Allow"
	HeaderAuthorization           = "Authorization"
	HeaderCacheControl            = "Cache-Control"
	HeaderConnection              = "Connection"
	HeaderContentEncoding         = "Content-Encoding"
	HeaderContentLength           = "Content-Length"
	HeaderContentType             = "Content-Type"
	HeaderCookie                  = "Cookie"
	HeaderDate                    = "Date"
	HeaderETag                    = "ETag"
	HeaderForwarded               = "Forwarded"
	HeaderServer                  = "Server"
	HeaderSetCookie               = "Set-Cookie"
	HeaderStrictTransportSecurity = "Strict-Transport-Security"
	HeaderUserAgent               = "User-Agent"
	HeaderVary                    = "Vary"
	HeaderXContentTypeOptions     = "X-Content-Type-Options"
	HeaderXForwardedFor           = "X-Forwarded-For"
	HeaderXForwardedHost          = "X-Forwarded-Host"
	HeaderXForwardedPort          = "X-Forwarded-Port"
	HeaderXForwardedProto         = "X-Forwarded-Proto"
	HeaderXForwardedScheme        = "X-Forwarded-Scheme"
	HeaderXFrameOptions           = "X-Frame-Options"
	HeaderXRealIP                 = "X-Real-IP"
	HeaderXServedBy               = "X-Served-By"
	HeaderXXSSProtection          = "X-Xss-Protection"
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
