package sqslite

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func Test_Server_createQueue(t *testing.T) {
	server, testServer := startTestServer(t)
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
	})
	// default + defaultDLQ + not-default
	require.EqualValues(t, 3, len(server.accounts.accounts[testAccountID].queues))
}

func Test_Server_listQueues(t *testing.T) {
	_, testServer := startTestServer(t)
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
	})
	results := testHelperListQueues(t, testServer, &sqs.ListQueuesInput{})
	require.EqualValues(t, 3, len(results.QueueUrls))
}

func Test_Server_listQueues_maxResults(t *testing.T) {
	_, testServer := startTestServer(t)
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
	})
	results := testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(2),
	})
	require.EqualValues(t, 2, len(results.QueueUrls))
}

func Test_Server_listQueues_pages(t *testing.T) {
	_, testServer := startTestServer(t)
	for range 10 {
		_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
			QueueName: aws.String("prefix-" + uuid.V4().String()),
		})
	}
	for range 10 {
		_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
			QueueName: aws.String("not-prefix-" + uuid.V4().String()),
		})
	}
	results := testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(5),
	})
	require.EqualValues(t, 5, len(results.QueueUrls))
	require.NotNil(t, results.NextToken)
	require.NotEmpty(t, *results.NextToken)

	parsedToken := parseNextPageToken(*results.NextToken)
	require.EqualValues(t, 5, parsedToken.Offset)

	results = testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(5),
		NextToken:  results.NextToken,
	})
	require.EqualValues(t, 5, len(results.QueueUrls))
	require.NotNil(t, results.NextToken)
	require.NotEmpty(t, *results.NextToken)

	parsedToken = parseNextPageToken(*results.NextToken)
	require.EqualValues(t, 10, parsedToken.Offset)

	results = testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(5),
		NextToken:  results.NextToken,
	})
	require.EqualValues(t, 5, len(results.QueueUrls))
	require.NotNil(t, results.NextToken)
	require.NotEmpty(t, *results.NextToken)

	parsedToken = parseNextPageToken(*results.NextToken)
	require.EqualValues(t, 15, parsedToken.Offset)

	results = testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(5),
		NextToken:  results.NextToken,
	})
	require.EqualValues(t, 5, len(results.QueueUrls))
	require.NotNil(t, results.NextToken)
	require.NotEmpty(t, *results.NextToken)

	parsedToken = parseNextPageToken(*results.NextToken)
	require.EqualValues(t, 20, parsedToken.Offset)

	results = testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(5),
		NextToken:  results.NextToken,
	})
	require.EqualValues(t, 2, len(results.QueueUrls), strings.Join(results.QueueUrls, ","))
	require.Nil(t, results.NextToken)
}

func Test_Server_listQueues_prefix(t *testing.T) {
	_, testServer := startTestServer(t)
	for range 10 {
		_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
			QueueName: aws.String("prefix-" + uuid.V4().String()),
		})
	}
	for range 10 {
		_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
			QueueName: aws.String("not-prefix-" + uuid.V4().String()),
		})
	}
	results := testHelperListQueues(t, testServer, &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String("prefix-"),
	})

	// should not include the default (2), nor the other 10 we created
	// with the other prefix
	require.EqualValues(t, 10, len(results.QueueUrls))

	for _, queueURL := range results.QueueUrls {
		require.True(t, strings.Contains(queueURL, "/prefix-"), queueURL)
	}
}

func Test_Server_deleteQueue(t *testing.T) {
	server, testServer := startTestServer(t)
	_ = testHelperDeleteQueue(t, testServer, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(testDefaultQueueURL),
	})
	// just defaultDLQ
	require.EqualValues(t, 1, len(server.accounts.accounts[testAccountID].queues))
}

func Test_Server_sendMessage(t *testing.T) {
	server, testServer := startTestServer(t)
	queue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	for range 2 {
		_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
	}
	require.EqualValues(t, int64(2), queue.Stats().NumMessages)
	require.EqualValues(t, int64(0), queue.Stats().NumMessagesInflight)
}

func Test_Server_receiveMessage_awaitsMessages(t *testing.T) {
	server, testServer := startTestServer(t)
	queue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
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
	require.Equal(t, int64(2), queue.Stats().NumMessages)
	require.True(t, queue.Stats().NumMessagesInflight > 0)
}

func Test_Server_rejectsNonPostMethod(t *testing.T) {
	_, testServer := startTestServer(t)
	req, err := http.NewRequest(http.MethodGet, testServer.URL, nil)
	require.NoError(t, err)
	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
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
	res := testHelperDoClientMethodWithoutAuth(t, testServer, MethodSendMessage, testNewSendMessageInput(testDefaultQueueURL))
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
	queue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	require.Equal(t, int64(5), queue.Stats().NumMessages)
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
	queue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	require.Equal(t, int64(2), queue.Stats().NumMessages)

	received := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(testDefaultQueueURL),
		MaxNumberOfMessages: 5,
	})
	require.Len(t, received.Messages, 2)
}
