package sqslite

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/wcharczuk/sqslite/internal/httputil"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func testNewSendMessageInput(queueURL string) *sqs.SendMessageInput {
	return &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(`{"message":0}`),
	}
}

func testHelperCreateQueue(t *testing.T, testServer *httptest.Server, input *sqs.CreateQueueInput) *sqs.CreateQueueOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.CreateQueueInput, sqs.CreateQueueOutput](t, testServer, MethodCreateQueue, input)
}

func testHelperListQueues(t *testing.T, testServer *httptest.Server, input *sqs.ListQueuesInput) *sqs.ListQueuesOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ListQueuesInput, sqs.ListQueuesOutput](t, testServer, MethodListQueues, input)
}

func testHelperDeleteQueue(t *testing.T, testServer *httptest.Server, input *sqs.DeleteQueueInput) *sqs.DeleteQueueOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.DeleteQueueInput, sqs.DeleteQueueOutput](t, testServer, MethodDeleteQueue, input)
}

func testHelperSendMessage(t *testing.T, testServer *httptest.Server, input *sqs.SendMessageInput) *sqs.SendMessageOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.SendMessageInput, sqs.SendMessageOutput](t, testServer, MethodSendMessage, input)
}

func testHelperReceiveMessages(t *testing.T, testServer *httptest.Server, input *sqs.ReceiveMessageInput) *sqs.ReceiveMessageOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ReceiveMessageInput, sqs.ReceiveMessageOutput](t, testServer, MethodReceiveMessage, input)
}

func testHelperDoClientMethod[Input, Output any](t *testing.T, testServer *httptest.Server, method string, input *Input) *Output {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, testServer.URL, bytes.NewBufferString(marshalJSON(input)))
	require.NoError(t, err)
	req.Header.Set(httputil.HeaderAuthorization, testAuthorizationHeader)
	req.Header.Set(httputil.HeaderContentType, ContentTypeAmzJSON)
	req.Header.Set(HeaderAmzTarget, method)
	req.Header.Set(HeaderAmzQueryMode, "true")
	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	require.Equal(t, http.StatusOK, res.StatusCode)
	defer res.Body.Close()
	var output Output
	err = json.NewDecoder(res.Body).Decode(&output)
	require.NoError(t, err)
	return &output
}

func testHelperDoClientMethodWithoutAuth(t *testing.T, testServer *httptest.Server, method string, input any) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, testServer.URL, bytes.NewBufferString(marshalJSON(input)))
	require.NoError(t, err)
	// req.Header.Set(httputil.HeaderAuthorization, testAuthorizationHeader)
	req.Header.Set(httputil.HeaderContentType, ContentTypeAmzJSON)
	req.Header.Set(HeaderAmzTarget, method)
	req.Header.Set(HeaderAmzQueryMode, "true")
	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	return res
}

const (
	testAccountID           = "test-account"
	testDefaultQueueURL     = "http://sqslite.local/test-account/default"
	testDefaultDLQQueueURL  = "http://sqslite.local/test-account/default-dlq"
	testAuthorizationHeader = "AWS4-HMAC-SHA256 Credential=test-account/20250522/us-east-1/sqs/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-date;x-amz-security-token;x-amz-target;x-amzn-query-mode, Signature=DEADBEEF"
)

func startTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	server := NewServer().WithClock(clockwork.NewFakeClock())
	authz := Authorization{
		AccountID: testAccountID,
	}
	dlq, _ := NewQueueFromCreateQueueInput(server.Clock(), authz, &sqs.CreateQueueInput{
		QueueName: aws.String("default-dlq"),
	})
	dlq.Start()
	server.accounts.EnsureQueues(testAccountID).AddQueue(dlq)
	defaultQueue, _ := NewQueueFromCreateQueueInput(server.Clock(), authz, &sqs.CreateQueueInput{
		QueueName: aws.String("default"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(RedrivePolicy{
				DeadLetterTargetArn: dlq.ARN,
				MaxReceiveCount:     10,
			}),
		},
	})
	defaultQueue.Start()
	server.accounts.EnsureQueues(testAccountID).AddQueue(defaultQueue)
	svr := httptest.NewServer(httputil.Logged(server))
	t.Cleanup(server.Close)
	t.Cleanup(svr.Close)
	return server, svr
}

func createTestQueue(t *testing.T, clock clockwork.Clock) *Queue {
	t.Helper()
	q, err := NewQueueFromCreateQueueInput(clock, Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	t.Cleanup(q.Close)
	require.Nil(t, err)
	return q
}

func createTestMessage(body string) Message {
	return Message{
		MessageID: uuid.V4(),
		Body:      Some(body), // once told me
	}
}

func pushTestMessages(q *Queue, count int) []*MessageState {
	var messages []*MessageState
	for range count {
		msg := createTestMessage("test message body")
		msgState, _ := q.NewMessageState(msg, q.Clock().Now(), 0)
		messages = append(messages, msgState)
	}
	q.Push(messages...)
	return messages
}
