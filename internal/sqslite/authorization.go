package sqslite

import (
	"context"
	"net/http"
	"strings"
)

// DefaultAccountID is the default account ID I use
// in a few places and am just keeping it consistent.
const (
	DefaultHost      = "sqslite.local"
	DefaultRegion    = "us-west-2"
	DefaultAccountID = "sqslite-test-account"

	knownGoodSignatureType = "AWS4-HMAC-SHA256"
)

type Authorization struct {
	Host      string
	Region    string
	AccountID string
}

func (a Authorization) HostOrDefault() string {
	if a.Host != "" {
		return a.Host
	}
	return DefaultHost
}

func (a Authorization) RegionOrDefault() string {
	if a.Region != "" {
		return a.Region
	}
	return DefaultRegion
}

func (a Authorization) AccountIDOrDefault() string {
	if a.AccountID != "" {
		return a.AccountID
	}
	return DefaultAccountID
}

type authorizationKey struct{}

// WithContextAuthorization adds authorization to a given context.
func WithContextAuthorization(ctx context.Context, authz Authorization) context.Context {
	return context.WithValue(ctx, authorizationKey{}, authz)
}

// GetContextAuthorization returns the authorization from a given context.
func GetContextAuthorization(ctx context.Context) (authz Authorization, ok bool) {
	if value := ctx.Value(authorizationKey{}); value != nil {
		authz, ok = value.(Authorization)
	}
	return
}

func getRequestAuthorization(req *http.Request) (auth Authorization, err *Error) {
	auth.AccountID, err = getRequestAuthorizationAccountID(req)
	if err != nil {
		return
	}
	auth.Region, err = getRequestAuthorizationRegion(req)
	if err != nil {
		return
	}
	auth.Host = req.Host
	return
}

func getRequestAuthorizationAccountID(req *http.Request) (accountID string, err *Error) {
	authorizationHeader := req.Header.Get("Authorization")
	if authorizationHeader == "" {
		err = ErrorUnauthorized()
		return
	}
	if !strings.HasPrefix(authorizationHeader, knownGoodSignatureType) {
		err = ErrorUnauthorized()
		return
	}
	fields := strings.Fields(authorizationHeader)
	if len(fields) < 2 {
		err = ErrorUnauthorized()
		return
	}
	credentialsField := strings.TrimPrefix(fields[1], "Credential=")
	if credentialsField == "" {
		err = ErrorUnauthorized()
		return
	}
	accountID, _, _ = strings.Cut(credentialsField, "/")
	if accountID == "" {
		err = ErrorUnauthorized()
		return
	}
	return
}

func getRequestAuthorizationRegion(req *http.Request) (region string, err *Error) {
	authorizationHeader := req.Header.Get("Authorization")
	if authorizationHeader == "" {
		err = ErrorUnauthorized()
		return
	}
	if !strings.HasPrefix(authorizationHeader, knownGoodSignatureType) {
		err = ErrorUnauthorized()
		return
	}
	fields := strings.Fields(authorizationHeader)
	if len(fields) < 2 {
		err = ErrorUnauthorized()
		return
	}
	credentialsField := strings.TrimPrefix(fields[1], "Credential=")
	if credentialsField == "" {
		err = ErrorUnauthorized()
		return
	}
	parts := strings.Split(credentialsField, "/")
	if len(parts) < 3 {
		err = ErrorUnauthorized()
		return
	}
	region = parts[2]
	return
}
