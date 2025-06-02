package sqslite

import (
	"context"
	"net/http"
	"strings"
)

const (
	knownGoodSignatureType = "AWS4-HMAC-SHA256"
)

// DefaultAuthorization is an authorization that uses
// [DefaultAccountID] as the account identifier.
var DefaultAuthorization = Authorization{
	AccountID: DefaultAccountID,
}

type Authorization struct {
	Host      Optional[string]
	Region    Optional[string]
	AccountID string
}

func (a Authorization) HostOrDefault() string {
	if a.Host.IsSet {
		return a.Host.Value
	}
	return DefaultHost
}

func (a Authorization) RegionOrDefault() string {
	if a.Region.IsSet {
		return a.Region.Value
	}
	return DefaultRegion
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
	auth.Host = Some(req.Host)
	return
}

func getRequestAuthorizationAccountID(req *http.Request) (accountID string, err *Error) {
	authorizationHeader := req.Header.Get("Authorization")
	if authorizationHeader == "" {
		err = ErrorResponseInvalidSecurity()
		return
	}
	if !strings.HasPrefix(authorizationHeader, knownGoodSignatureType) {
		err = ErrorResponseInvalidSecurity()
		return
	}
	fields := strings.Fields(authorizationHeader)
	if len(fields) < 2 {
		err = ErrorResponseInvalidSecurity()
		return
	}
	credentialsField := strings.TrimPrefix(fields[1], "Credential=")
	if credentialsField == "" {
		err = ErrorResponseInvalidSecurity()
		return
	}
	accountID, _, _ = strings.Cut(credentialsField, "/")
	if accountID == "" {
		err = ErrorResponseInvalidSecurity()
		return
	}
	return
}

func getRequestAuthorizationRegion(req *http.Request) (region Optional[string], err *Error) {
	authorizationHeader := req.Header.Get("Authorization")
	if authorizationHeader == "" {
		err = ErrorResponseInvalidSecurity()
		return
	}
	if !strings.HasPrefix(authorizationHeader, knownGoodSignatureType) {
		err = ErrorResponseInvalidSecurity()
		return
	}
	fields := strings.Fields(authorizationHeader)
	if len(fields) < 2 {
		err = ErrorResponseInvalidSecurity()
		return
	}
	credentialsField := strings.TrimPrefix(fields[1], "Credential=")
	if credentialsField == "" {
		err = ErrorResponseInvalidSecurity()
		return
	}
	parts := strings.Split(credentialsField, "/")
	if len(parts) < 3 {
		err = ErrorResponseInvalidSecurity()
		return
	}
	region = Some(parts[2])
	return
}
