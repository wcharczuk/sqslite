package sqslite

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getAccountID(t *testing.T) {
	r := new(http.Request)
	r.Header = make(http.Header)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20250522/us-east-1/sqs/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-date;x-amz-security-token;x-amz-target;x-amzn-query-mode, Signature=2118a850a2f95dee1e3c6f89fe27cbd347883d64fc20da79405cb3e07dd29ae2")
	accountID, err := getAccountID(r)
	require.Nil(t, err)
	require.Equal(t, "AKID", accountID)
}
