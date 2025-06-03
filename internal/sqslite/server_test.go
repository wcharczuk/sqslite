package sqslite

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/httputil"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func Test_serialize(t *testing.T) {
	buf := new(bytes.Buffer)
	rw := httputil.NewMockResponseWriter(buf)
	serialize(rw, new(http.Request), &sqs.CreateQueueOutput{
		QueueUrl:       aws.String("test-queue-url"),
		ResultMetadata: middleware.Metadata{},
	})
	require.EqualValues(t, http.StatusOK, rw.StatusCode())
	require.EqualValues(t, "{\"QueueUrl\":\"test-queue-url\",\"ResultMetadata\":{}}\n", buf.String())
}

func Test_Server_createQueue(t *testing.T) {
	server, testServer := startTestServer(t)
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
	})
	// default + defaultDLQ + not-default
	require.EqualValues(t, 3, len(server.accounts.accounts[testAccountID].queues))
}

func Test_Server_createQueue_allowsDuplicatesWithSameAttributes(t *testing.T) {
	server, testServer := startTestServer(t)
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "10",
		},
	})
	// default + defaultDLQ + not-default
	require.EqualValues(t, 3, len(server.accounts.accounts[testAccountID].queues))
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "10",
		},
	})
	// default + defaultDLQ + not-default
	require.EqualValues(t, 3, len(server.accounts.accounts[testAccountID].queues))
}

func Test_Server_createQueue_blocksDuplicatesWithUpdatedAttributes(t *testing.T) {
	server, testServer := startTestServer(t)
	_ = testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "10",
		},
	})
	// default + defaultDLQ + not-default
	require.EqualValues(t, 3, len(server.accounts.accounts[testAccountID].queues))
	err := testHelperCreateQueueForError(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("not-default"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "20",
		},
	})
	// default + defaultDLQ + not-default
	require.EqualValues(t, 3, len(server.accounts.accounts[testAccountID].queues))
	require.EqualValues(t, "com.amazonaws.sqs#QueueNameExists", err.Type)
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

	queue, ok := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	require.True(t, ok)
	require.NotNil(t, queue)
	require.True(t, queue.IsDeleted())
}

func Test_Server_sendMessage_multiple(t *testing.T) {
	server, testServer := startTestServer(t)
	queue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	for range 2 {
		_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
	}
	require.EqualValues(t, int64(2), queue.Stats().NumMessages)
	require.EqualValues(t, int64(0), queue.Stats().NumMessagesInflight)
}

func Test_Server_sendMessage(t *testing.T) {
	_, testServer := startTestServer(t)
	res := testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    aws.String(testDefaultQueueURL),
		MessageBody: aws.String(`{"message_index":1}`),
	})
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", safeDeref(res.MD5OfMessageBody))
	require.Nil(t, res.MD5OfMessageAttributes)
	require.Nil(t, res.MD5OfMessageSystemAttributes)
}

func Test_Server_sendMessage_attributes(t *testing.T) {
	_, testServer := startTestServer(t)
	res := testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    aws.String(testDefaultQueueURL),
		MessageBody: aws.String(`{"message_index":1}`),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test-key": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test-value"),
			},
		},
	})
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", safeDeref(res.MD5OfMessageBody))
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", safeDeref(res.MD5OfMessageAttributes))
	require.Nil(t, res.MD5OfMessageSystemAttributes)
}

func Test_Server_sendMessage_attributes_systemAttributes(t *testing.T) {
	_, testServer := startTestServer(t)
	res := testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    aws.String(testDefaultQueueURL),
		MessageBody: aws.String(`{"message_index":1}`),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test-key": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test-value"),
			},
		},
		MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
			"AWSTraceHeader": {
				DataType:    aws.String("String"),
				StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"),
			},
		},
	})
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", safeDeref(res.MD5OfMessageBody))
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", safeDeref(res.MD5OfMessageAttributes))
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", safeDeref(res.MD5OfMessageSystemAttributes))
}

func Test_Server_sendMessageBatch(t *testing.T) {
	server, testServer := startTestServer(t)
	queue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	res := testHelperSendMessageBatch(t, testServer, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(testDefaultQueueURL),
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          aws.String("1"),
				MessageBody: aws.String(`{"message_index":1}`),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"test-key": {
						DataType:    aws.String("String"),
						StringValue: aws.String("test-value"),
					},
				},
				MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
					"AWSTraceHeader": {
						DataType:    aws.String("String"),
						StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"),
					},
				},
			},
			{
				Id:          aws.String("2"),
				MessageBody: aws.String(`{"message_index":2}`),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"test-key": {
						DataType:    aws.String("String"),
						StringValue: aws.String("test-value"),
					},
				},
				MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
					"AWSTraceHeader": {
						DataType:    aws.String("String"),
						StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"),
					},
				},
			},
		},
	})
	require.Len(t, res.Successful, 2)

	require.EqualValues(t, md5sum(`{"message_index":1}`), safeDeref(res.Successful[0].MD5OfMessageBody))
	require.EqualValues(t, md5sum(`{"message_index":2}`), safeDeref(res.Successful[1].MD5OfMessageBody))
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", safeDeref(res.Successful[0].MD5OfMessageAttributes))
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", safeDeref(res.Successful[1].MD5OfMessageAttributes))
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", safeDeref(res.Successful[0].MD5OfMessageSystemAttributes))
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", safeDeref(res.Successful[1].MD5OfMessageSystemAttributes))

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
	require.Equal(t, "com.amazonaws.sqs#UnsupportedOperation", errResponse.Type)
	require.Equal(t, "Invalid method GET", errResponse.Message)
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
	require.GreaterOrEqual(t, len(received.Messages), 1)
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
	require.GreaterOrEqual(t, len(received.Messages), 1)
}

func Test_Server_receive_delete(t *testing.T) {
	server, testServer := startTestServer(t)

	for range 3 {
		for range 10 {
			_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
		}
		for range 10 {
			messages := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(testDefaultQueueURL),
				VisibilityTimeout:   10,
				MaxNumberOfMessages: 1,
			})
			for _, msg := range messages.Messages {
				testHelperDeleteMessage(t, testServer, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(testDefaultQueueURL),
					ReceiptHandle: msg.ReceiptHandle,
				})
			}
		}
	}

	defaultQueue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	defaultDLQ := server.accounts.accounts[testAccountID].queues[testDefaultDLQQueueURL]

	defaultQueue.UpdateInflightVisibility()
	require.EqualValues(t, 0, defaultQueue.Stats().NumMessages)
	require.EqualValues(t, 0, defaultDLQ.Stats().NumMessages)
}

func Test_Server_transfersToDLQ(t *testing.T) {
	server, testServer := startTestServer(t)
	for range 10 {
		_ = testHelperSendMessage(t, testServer, testNewSendMessageInput(testDefaultQueueURL))
	}
	for range testMaxReceiveCount {
		for range 10 {
			messages := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(testDefaultQueueURL),
				VisibilityTimeout:   10,
				MaxNumberOfMessages: 1,
			})
			for _, msg := range messages.Messages {
				testHelperChangeMessageVisibility(t, testServer, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(testDefaultQueueURL),
					ReceiptHandle:     msg.ReceiptHandle,
					VisibilityTimeout: 0,
				})
			}
		}
	}
	defaultQueue := server.accounts.accounts[testAccountID].queues[testDefaultQueueURL]
	defaultDLQ := server.accounts.accounts[testAccountID].queues[testDefaultDLQQueueURL]
	defaultQueue.UpdateInflightVisibility()
	require.EqualValues(t, 10, defaultDLQ.Stats().NumMessages)
}
