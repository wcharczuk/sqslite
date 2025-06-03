package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/spy"
)

func Test_normalizeRequest(t *testing.T) {
	r := spy.Request{
		ResponseBody: `{\"QueueUrl\":\"https://127.0.0.1:55854/360888705397/test-queue-624b3f6b1c25440280165e5f65d17423\"}`,
	}

	output := normalizeRequest(r)
	require.EqualValues(t, `{\"QueueUrl\":\"http://sqslite.local/sqslite-test-account/test-queue-624b3f6b1c25440280165e5f65d17423\"}`, output.ResponseBody)
}

func Test_normalizeRequest_http(t *testing.T) {
	r := spy.Request{
		ResponseBody: `{"QueueUrl":"http://127.0.0.1:59217/sqslite-test-account/test-send-receive-1","ResultMetadata":{}}`,
	}
	output := normalizeRequest(r)
	require.EqualValues(t, `{"QueueUrl":"http://sqslite.local/sqslite-test-account/test-send-receive-1","ResultMetadata":{}}`, output.ResponseBody)
}

func Test_normalizeQueueURLs(t *testing.T) {
	body := `{\"QueueUrl\":\"https://127.0.0.1:55854/360888705397/test-queue-624b3f6b1c25440280165e5f65d17423\"}`
	actual := normalizeQueueURLs(body)
	require.EqualValues(t, `{\"QueueUrl\":\"http://sqslite.local/sqslite-test-account/test-queue-624b3f6b1c25440280165e5f65d17423\"}`, actual)
}

func Test_normalizeQueueURLs_http(t *testing.T) {
	body := `{\"QueueUrl\":\"http://127.0.0.1:55854/360888705397/test-queue-624b3f6b1c25440280165e5f65d17423\"}`
	actual := normalizeQueueURLs(body)
	require.EqualValues(t, `{\"QueueUrl\":\"http://sqslite.local/sqslite-test-account/test-queue-624b3f6b1c25440280165e5f65d17423\"}`, actual)
}

func Test_normalizeQueueARNs(t *testing.T) {
	body := `"{\"Attributes\":{\"QueueArn\":\"arn:aws:sqs:us-west-2:360888705397:test-queue-b9931705007043d688c49e15c7c450ca\"}}`
	actual := normalizeQueueARNs(body)
	require.EqualValues(t, `"{\"Attributes\":{\"QueueArn\":\"arn:aws:sqs:us-west-2:sqslite-test-account:test-queue-b9931705007043d688c49e15c7c450ca\"}}`, actual)
}

func Test_normalizeQueueARNs_regression(t *testing.T) {
	body := `"{\"Attributes\":{\"QueueArn\":\"arn:aws:sqs:us-west-2:AKIAVIBVEUF22S43OJ62:test-send-attribute-md5-1\"}}`
	actual := normalizeQueueARNs(body)
	require.EqualValues(t, `"{\"Attributes\":{\"QueueArn\":\"arn:aws:sqs:us-west-2:sqslite-test-account:test-send-attribute-md5-1\"}}`, actual)
}
