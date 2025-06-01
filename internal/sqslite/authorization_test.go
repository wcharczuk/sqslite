package sqslite

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getRequestAuthorization(t *testing.T) {
	r := new(http.Request)
	r.Host = "sqslite.test"
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=TEST_ACCOUNT_ID/20250522/us-east-1/sqs/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-date;x-amz-security-token;x-amz-target;x-amzn-query-mode, Signature=DEADBEEF")
	authz, err := getRequestAuthorization(r)
	require.Nil(t, err)
	require.Equal(t, "us-east-1", authz.Region.Value)
	require.Equal(t, "sqslite.test", authz.Host.Value)
	require.Equal(t, "TEST_ACCOUNT_ID", authz.AccountID)
}

func Test_getRequestAuthorization_malformed_empty(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, "", authz.AccountID)
}

func Test_getRequestAuthorization_malformed_wrongPrefix(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "NOT-AWS4-HMAC-SHA256 Credential=TEST_ACCOUNT_ID/20250522/us-east-1/sqs/aws4_request")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, "", authz.AccountID)
}

func Test_getRequestAuthorization_malformed_missingSignedHeaders(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=TEST_ACCOUNT_ID/20250522/us-east-1/sqs/aws4_request")
	authz, err := getRequestAuthorization(r)
	require.Nil(t, err)
	require.Equal(t, "TEST_ACCOUNT_ID", authz.AccountID)
}

func Test_getRequestAuthorization_malformed_missingCredentials(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, "", authz.AccountID)
}

func Test_getRequestAuthorizationAccountID_credentialsMissingDateRegion(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=BUFO_WAS_HERE")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, "BUFO_WAS_HERE", authz.AccountID)
	require.Equal(t, false, authz.Region.IsSet)
}

func Test_getRequestAuthorizationRegion_malformed_empty(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, false, authz.Region.IsSet)
}

func Test_getRequestAuthorizationRegion_malformed_missingSignedHeaders(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=TEST_ACCOUNT_ID/20250522/us-east-1/sqs/aws4_request")
	authz, err := getRequestAuthorization(r)
	require.Nil(t, err)
	require.Equal(t, "us-east-1", authz.Region.Value)
}

func Test_getRequestAuthorizationRegion_malformed_wrongPrefix(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "NOT-AWS4-HMAC-SHA256 Credential=TEST_ACCOUNT_ID/20250522/us-east-1/sqs/aws4_request")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, false, authz.Region.IsSet)
}

func Test_getRequestAuthorizationRegion_malformed_missingCredentials(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, false, authz.Region.IsSet)
}

func Test_getRequestAuthorizationRegion_credentialsMissingDateRegion(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=BUFO_WAS_HERE/something")
	authz, err := getRequestAuthorization(r)
	require.NotNil(t, err)
	require.Equal(t, false, authz.Region.IsSet)
}
