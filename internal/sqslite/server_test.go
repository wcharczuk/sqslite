package sqslite

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/httputil"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func Test_Server_rejectsNonPostMethod(t *testing.T) {
	_, testServer := startTestServer(t)
	req, err := http.NewRequest(http.MethodGet, testServer.URL, nil)
	require.NoError(t, err)
	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func Test_Server_rejectsNonSlashPath(t *testing.T) {
	_, testServer := startTestServer(t)
	testServerURL := must(url.Parse(testServer.URL))
	testServerURL.Path = "/nogood"
	req, err := http.NewRequest(http.MethodGet, testServerURL.String(), nil)
	require.NoError(t, err)
	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func Test_Server_requiresAuthorization(t *testing.T) {
	_, testServer := startTestServer(t)
	res := testHelperDoClientMethodWithoutAuth(t, testServer, methodSendMessage, testNewSendMessageInput(testDefaultQueueURL))
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func Test_Server_handlesUnknownMethods(t *testing.T) {
	_, testServer := startTestServer(t)
	res := testHelperDoClientMethodWithoutAuth(t, testServer, uuid.V4().String(), testNewSendMessageInput(testDefaultQueueURL))
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func Test_Server_returnsWellFormedErrors(t *testing.T) {
	_, testServer := startTestServer(t)
	req, err := http.NewRequest(http.MethodGet, testServer.URL, nil)
	require.NoError(t, err)
	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	var errResponse Error
	err = json.NewDecoder(res.Body).Decode(&errResponse)
	require.NoError(t, err)
	require.Equal(t, "InvalidMethod", errResponse.Code)
	require.Equal(t, http.StatusBadRequest, errResponse.StatusCode)
	require.Equal(t, true, errResponse.SenderFault)
	require.Equal(t, "The http method GET is not valid for this endpoint.", errResponse.Message)
}

func Test_Server_receiveMessage_equalMaxNumberOfMessages(t *testing.T) {
	server, testServer := startTestServer(t)
	for range 5 {
		_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
	}
	require.Equal(t, int64(5), server.queues.queues[testDefaultQueueURL].Stats().NumMessages)

	received := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(testDefaultQueueURL),
		MaxNumberOfMessages: 5,
	})
	require.Len(t, received.Messages, 5)
}

func Test_Server_receiveMessage_belowMaxNumberOfMessages(t *testing.T) {
	server, testServer := startTestServer(t)
	for range 2 {
		_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
	}
	require.Equal(t, int64(2), server.queues.queues[testDefaultQueueURL].Stats().NumMessages)

	received := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(testDefaultQueueURL),
		MaxNumberOfMessages: 5,
	})
	require.Len(t, received.Messages, 2)
}

func Test_Server_receiveMessage_awaitsMessages(t *testing.T) {
	server, testServer := startTestServer(t)
	defaultQueue, _ := server.queues.queues[testDefaultQueueURL]
	serverClock, _ := server.Clock().(*clockwork.FakeClock)
	startedReceiveRequest := make(chan struct{})
	completedReceiveRequest := make(chan struct{})
	go func() {
		close(startedReceiveRequest)
		defer close(completedReceiveRequest)
		received := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(testDefaultQueueURL),
			MaxNumberOfMessages: 5,
		})
		require.True(t, len(received.Messages) > 0)
	}()
	<-startedReceiveRequest
	for range 2 {
		_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
	}
	serverClock.Advance(200 * time.Millisecond)
	<-completedReceiveRequest
	require.Equal(t, int64(2), defaultQueue.Stats().NumMessages)
	require.True(t, defaultQueue.Stats().NumMessagesInflight > 0)
}

func testNewSendMessageInput(queueURL string) *sqs.SendMessageInput {
	return &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(`{"message":0}`),
	}
}

func testHelperSendMessage(t *testing.T, testServer *httptest.Server, input *sqs.SendMessageInput) *sqs.SendMessageOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.SendMessageInput, sqs.SendMessageOutput](t, testServer, methodSendMessage, input)
}

func testHelperReceiveMessages(t *testing.T, testServer *httptest.Server, input *sqs.ReceiveMessageInput) *sqs.ReceiveMessageOutput {
	t.Helper()
	return testHelperDoClientMethod[sqs.ReceiveMessageInput, sqs.ReceiveMessageOutput](t, testServer, methodReceiveMessage, input)
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
		Host:      DefaultHost,
		Region:    DefaultRegion,
		AccountID: testAccountID,
	}
	ctx := WithContextAuthorization(context.Background(), authz)
	dlq, _ := NewQueueFromCreateQueueInput(server.Clock(), authz, &sqs.CreateQueueInput{
		QueueName: aws.String("default-dlq"),
	})
	dlq.Start()
	server.queues.AddQueue(
		ctx,
		dlq,
	)
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
	server.queues.AddQueue(
		ctx,
		defaultQueue,
	)
	svr := httptest.NewServer(httputil.Logged(server))
	t.Cleanup(server.Close)
	t.Cleanup(svr.Close)
	return server, svr
}
