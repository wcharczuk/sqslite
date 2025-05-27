package sqslite

import (
	"context"
	"net/http"
	"strings"
)

// DefaultAccountID is the default account ID I use
// in a few places and am just keeping it consistent.
const DefaultAccountID = "sqslite-test-account"

type Authorization struct {
	AccountID string
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

func getRequestAuthorization(req *http.Request) (Authorization, *Error) {
	authorization := req.Header.Get("Authorization")
	if authorization == "" {
		return Authorization{}, ErrorUnauthorized()
	}
	fields := strings.Fields(authorization)
	if len(fields) < 2 {
		return Authorization{}, ErrorUnauthorized()
	}
	credentialsField := strings.TrimPrefix(fields[1], "Credential=")
	if credentialsField == "" {
		return Authorization{}, ErrorUnauthorized()
	}
	accountID, _, _ := strings.Cut(credentialsField, "/")
	if accountID == "" {
		return Authorization{}, ErrorUnauthorized()
	}
	return Authorization{
		AccountID: accountID,
	}, nil
}
