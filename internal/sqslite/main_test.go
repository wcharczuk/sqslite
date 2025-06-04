package sqslite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/wcharczuk/sqslite/internal/httputil"
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

func testHelperCreateQueueForError(t *testing.T, testServer *httptest.Server, input *sqs.CreateQueueInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodCreateQueue, input)
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

func testHelperSendMessageForError(t *testing.T, testServer *httptest.Server, input *sqs.SendMessageInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodSendMessage, input)
}

func testHelperSendMessageBatch(t *testing.T, testServer *httptest.Server, input *sqs.SendMessageBatchInput) *sqs.SendMessageBatchOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.SendMessageBatchInput, sqs.SendMessageBatchOutput](t, testServer, MethodSendMessageBatch, input)
}

func testHelperSendMessageBatchForError(t *testing.T, testServer *httptest.Server, input *sqs.SendMessageBatchInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodSendMessageBatch, input)
}

func testHelperReceiveMessages(t *testing.T, testServer *httptest.Server, input *sqs.ReceiveMessageInput) *sqs.ReceiveMessageOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ReceiveMessageInput, sqs.ReceiveMessageOutput](t, testServer, MethodReceiveMessage, input)
}

func testHelperChangeMessageVisibility(t *testing.T, testServer *httptest.Server, input *sqs.ChangeMessageVisibilityInput) *sqs.ChangeMessageVisibilityOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ChangeMessageVisibilityInput, sqs.ChangeMessageVisibilityOutput](t, testServer, MethodChangeMessageVisibility, input)
}

func testHelperDeleteMessage(t *testing.T, testServer *httptest.Server, input *sqs.DeleteMessageInput) *sqs.DeleteMessageOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.DeleteMessageInput, sqs.DeleteMessageOutput](t, testServer, MethodDeleteMessage, input)
}

func testHelperListDeadLetterSourceQueues(t *testing.T, testServer *httptest.Server, input *sqs.ListDeadLetterSourceQueuesInput) *sqs.ListDeadLetterSourceQueuesOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ListDeadLetterSourceQueuesInput, sqs.ListDeadLetterSourceQueuesOutput](t, testServer, MethodListDeadLetterSourceQueues, input)
}

func testHelperGetQueueAttributes(t *testing.T, testServer *httptest.Server, input *sqs.GetQueueAttributesInput) *sqs.GetQueueAttributesOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.GetQueueAttributesInput, sqs.GetQueueAttributesOutput](t, testServer, MethodGetQueueAttributes, input)
}

func testHelperSetQueueAttributes(t *testing.T, testServer *httptest.Server, input *sqs.SetQueueAttributesInput) *sqs.SetQueueAttributesOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.SetQueueAttributesInput, sqs.SetQueueAttributesOutput](t, testServer, MethodSetQueueAttributes, input)
}

func testHelperListQueueTags(t *testing.T, testServer *httptest.Server, input *sqs.ListQueueTagsInput) *sqs.ListQueueTagsOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ListQueueTagsInput, sqs.ListQueueTagsOutput](t, testServer, MethodListQueueTags, input)
}

func testHelperTagQueue(t *testing.T, testServer *httptest.Server, input *sqs.TagQueueInput) *sqs.TagQueueOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.TagQueueInput, sqs.TagQueueOutput](t, testServer, MethodTagQueue, input)
}

func testHelperUntagQueue(t *testing.T, testServer *httptest.Server, input *sqs.UntagQueueInput) *sqs.UntagQueueOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.UntagQueueInput, sqs.UntagQueueOutput](t, testServer, MethodUntagQueue, input)
}

func testHelperPurgeQueue(t *testing.T, testServer *httptest.Server, input *sqs.PurgeQueueInput) *sqs.PurgeQueueOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.PurgeQueueInput, sqs.PurgeQueueOutput](t, testServer, MethodPurgeQueue, input)
}

func testHelperDeleteMessageBatch(t *testing.T, testServer *httptest.Server, input *sqs.DeleteMessageBatchInput) *sqs.DeleteMessageBatchOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.DeleteMessageBatchInput, sqs.DeleteMessageBatchOutput](t, testServer, MethodDeleteMessageBatch, input)
}

func testHelperChangeMessageVisibilityBatch(t *testing.T, testServer *httptest.Server, input *sqs.ChangeMessageVisibilityBatchInput) *sqs.ChangeMessageVisibilityBatchOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ChangeMessageVisibilityBatchInput, sqs.ChangeMessageVisibilityBatchOutput](t, testServer, MethodChangeMessageVisibilityBatch, input)
}

// Error helper functions
func testHelperListDeadLetterSourceQueuesForError(t *testing.T, testServer *httptest.Server, input *sqs.ListDeadLetterSourceQueuesInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodListDeadLetterSourceQueues, input)
}

func testHelperGetQueueAttributesForError(t *testing.T, testServer *httptest.Server, input *sqs.GetQueueAttributesInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodGetQueueAttributes, input)
}

func testHelperSetQueueAttributesForError(t *testing.T, testServer *httptest.Server, input *sqs.SetQueueAttributesInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodSetQueueAttributes, input)
}

func testHelperPurgeQueueForError(t *testing.T, testServer *httptest.Server, input *sqs.PurgeQueueInput) *Error {
	t.Helper()
	return testHelperDoClientMethodForError(t, testServer, MethodPurgeQueue, input)
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
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var responseErr Error
		_ = json.NewDecoder(res.Body).Decode(&responseErr)
		require.Equal(t, http.StatusOK, res.StatusCode, fmt.Sprintf("%s %s", responseErr.Type, responseErr.Message))
	}

	var output Output
	err = json.NewDecoder(res.Body).Decode(&output)
	require.NoError(t, err)
	return &output
}

func testHelperDoClientMethodForError[Input any](t *testing.T, testServer *httptest.Server, method string, input *Input) *Error {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, testServer.URL, bytes.NewBufferString(marshalJSON(input)))
	require.NoError(t, err)
	req.Header.Set(httputil.HeaderAuthorization, testAuthorizationHeader)
	req.Header.Set(httputil.HeaderContentType, ContentTypeAmzJSON)
	req.Header.Set(HeaderAmzTarget, method)
	req.Header.Set(HeaderAmzQueryMode, "true")
	res, err := testServer.Client().Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	bodyData, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	var output Error
	err = json.Unmarshal(bodyData, &output)
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
	testMaxReceiveCount     = 3
	testDefaultQueueURL     = "http://sqslite.local/test-account/default"
	testDefaultDLQQueueURL  = "http://sqslite.local/test-account/default-dlq"
	testAuthorizationHeader = "AWS4-HMAC-SHA256 Credential=test-account/20250522/us-east-1/sqs/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-date;x-amz-security-token;x-amz-target;x-amzn-query-mode, Signature=DEADBEEF"
)

func startTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	server := NewServer(clockwork.NewFakeClock())
	authz := Authorization{
		AccountID: testAccountID,
	}
	dlq, _ := NewQueueFromCreateQueueInput(server.Clock(), authz, &sqs.CreateQueueInput{
		QueueName: aws.String("default-dlq"),
	})
	dlq.Start(t.Context())
	server.accounts.EnsureQueues(testAccountID).AddQueue(dlq)
	defaultQueue, _ := NewQueueFromCreateQueueInput(server.Clock(), authz, &sqs.CreateQueueInput{
		QueueName: aws.String("default"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(RedrivePolicy{
				DeadLetterTargetArn: dlq.ARN,
				MaxReceiveCount:     testMaxReceiveCount,
			}),
		},
	})
	defaultQueue.Start(t.Context())
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
		AccountID: testAccountID,
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	t.Cleanup(q.Close)
	require.Nil(t, err)
	return q
}

func createTestQueueWithName(t *testing.T, clock clockwork.Clock, name string) *Queue {
	t.Helper()
	queue, err := NewQueueFromCreateQueueInput(clock, Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: testAccountID,
	}, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	require.Nil(t, err)
	return queue
}

func createTestQueueWithNameWithRedriveAllowPolicy(t *testing.T, clock clockwork.Clock, name string, rap RedriveAllowPolicy) *Queue {
	t.Helper()
	queue, err := NewQueueFromCreateQueueInput(clock, Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: testAccountID,
	}, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedriveAllowPolicy): marshalJSON(rap),
		},
	})
	require.Nil(t, err)
	return queue
}

func createTestQueueWithNameWithDLQ(t *testing.T, clock clockwork.Clock, name string, dlq *Queue) *Queue {
	t.Helper()
	queue, err := NewQueueFromCreateQueueInput(clock, Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: testAccountID,
	}, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(RedrivePolicy{
				DeadLetterTargetArn: dlq.ARN,
				MaxReceiveCount:     3,
			}),
		},
	})
	queue.dlqTarget = dlq
	dlq.AddDLQSources(queue)
	require.Nil(t, err)
	return queue
}

func createTestSendMessageInput(body string) *sqs.SendMessageInput {
	return &sqs.SendMessageInput{
		MessageBody: aws.String(body),
	}
}

func pushTestMessages(q *Queue, count int) []*MessageState {
	var messages []*MessageState
	for range count {
		msg := createTestSendMessageInput("test message body")
		msgState := q.NewMessageStateFromSendMessageInput(msg)
		messages = append(messages, msgState)
	}
	q.Push(messages...)
	return messages
}
