package httpz

const (
	// HeaderAcceptEncoding is a header that indicates a client can accept a non-plaintext encoding.
	//
	// It is used by the gzip response helpers to determine if a client should receive a gzipped response.
	HeaderAcceptEncoding = "Accept-Encoding"
	// HeaderAuthorization is the standard authorization header.
	HeaderAuthorization = "Authorization"
	// HeaderContentEncoding is the response header indicating the encoding of the response (e.g. gzipped).
	HeaderContentEncoding = "Content-Encoding"
	// HeaderContentLength is the header that indicates the length of the response.
	HeaderContentLength = "Content-Length"
	// HeaderContentType is the response header that indicates the content-type of the response.
	HeaderContentType = "Content-Type"
	// HeaderVary is a header used to negotiate content encodings.
	HeaderVary = "Vary"
	// HeaderXForwardedFor is added by proxies to requests to indicate the original remote_addr of the request.
	HeaderXForwardedFor = "X-Forwarded-For"
	// HeaderXRealIP is another name for [HeaderXForwardedFor].
	HeaderXRealIP = "X-Real-IP"
)

const (
	// ContentTypeApplicationJSON is a content type for JSON responses.
	// We specify chartset=utf-8 so that clients know to use the UTF-8 string encoding.
	ContentTypeApplicationJSON = "application/json; charset=utf-8"

	// ContentTypeHTML is a content type for html responses.
	// We specify chartset=utf-8 so that clients know to use the UTF-8 string encoding.
	ContentTypeHTML = "text/html; charset=utf-8"

	// ContentEncodingIdentity is the identity (uncompressed) content encoding.
	ContentEncodingIdentity = "identity"

	// ContentEncodingGZIP is the gzip (compressed) content encoding.
	ContentEncodingGZIP = "gzip"
)
