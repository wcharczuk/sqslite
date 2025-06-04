package sqslite

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/httputil"
)

// Helper functions specific to this test file

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

// Tests for listDeadLetterSourceQueues

func Test_Server_listDeadLetterSourceQueues(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a queue without DLQ target
	regularQueue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("regular-queue"),
	})

	// Create a DLQ
	dlq := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-dlq"),
	})

	// Create a queue with DLQ target
	sourceQueue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("source-queue"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(RedrivePolicy{
				DeadLetterTargetArn: "arn:aws:sqs:us-east-1:test-account:test-dlq",
				MaxReceiveCount:     3,
			}),
		},
	})

	// List dead letter source queues - should return the source queue
	result := testHelperListDeadLetterSourceQueues(t, testServer, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: dlq.QueueUrl,
	})

	require.Len(t, result.QueueUrls, 2) // default queue + source-queue (both have DLQ targets)
	require.Contains(t, result.QueueUrls, *sourceQueue.QueueUrl)
	require.Contains(t, result.QueueUrls, testDefaultQueueURL) // default queue has DLQ
	require.Nil(t, result.NextToken)

	// Verify regular queue is not included
	require.NotContains(t, result.QueueUrls, *regularQueue.QueueUrl)
}

func Test_Server_listDeadLetterSourceQueues_withPagination(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a DLQ
	dlq := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("pagination-dlq"),
	})

	// Create multiple source queues
	for i := range 5 {
		testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
			QueueName: aws.String(fmt.Sprintf("source-queue-%d", i)),
			Attributes: map[string]string{
				string(types.QueueAttributeNameRedrivePolicy): marshalJSON(RedrivePolicy{
					DeadLetterTargetArn: "arn:aws:sqs:us-east-1:test-account:pagination-dlq",
					MaxReceiveCount:     3,
				}),
			},
		})
	}

	// Test pagination with MaxResults
	result := testHelperListDeadLetterSourceQueues(t, testServer, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl:   dlq.QueueUrl,
		MaxResults: aws.Int32(3),
	})

	require.Len(t, result.QueueUrls, 3)
	require.NotNil(t, result.NextToken)

	// Get next page
	nextResult := testHelperListDeadLetterSourceQueues(t, testServer, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl:   dlq.QueueUrl,
		MaxResults: aws.Int32(3),
		NextToken:  result.NextToken,
	})

	require.GreaterOrEqual(t, len(nextResult.QueueUrls), 1)
}

func Test_Server_listDeadLetterSourceQueues_invalidMaxResults(t *testing.T) {
	_, testServer := startTestServer(t)

	dlq := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-dlq"),
	})

	// Test with invalid MaxResults (too high)
	err := testHelperListDeadLetterSourceQueuesForError(t, testServer, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl:   dlq.QueueUrl,
		MaxResults: aws.Int32(1001),
	})

	require.Equal(t, "com.amazonaws.sqs#InvalidAttributeValue", err.Type)
	require.Contains(t, err.Message, "MaxResults must be greater than 0 and less than 1000")

	// Test with invalid MaxResults (too low)
	err = testHelperListDeadLetterSourceQueuesForError(t, testServer, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl:   dlq.QueueUrl,
		MaxResults: aws.Int32(-1),
	})

	require.Equal(t, "com.amazonaws.sqs#InvalidAttributeValue", err.Type)
	require.Contains(t, err.Message, "MaxResults must be greater than 0 and less than 1000")
}

func Test_Server_listDeadLetterSourceQueues_nextTokenWithoutMaxResults(t *testing.T) {
	_, testServer := startTestServer(t)

	dlq := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-dlq"),
	})

	// Test NextToken without MaxResults
	err := testHelperListDeadLetterSourceQueuesForError(t, testServer, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl:  dlq.QueueUrl,
		NextToken: aws.String("some-token"),
	})

	require.Equal(t, "com.amazonaws.sqs#InvalidAttributeValue", err.Type)
	require.Contains(t, err.Message, "MaxResults must be set if NextToken is set")
}

// Tests for getQueueAttributes

func Test_Server_getQueueAttributes(t *testing.T) {
	_, testServer := startTestServer(t)

	// Test getting all attributes for default queue
	result := testHelperGetQueueAttributes(t, testServer, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(testDefaultQueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameAll,
		},
	})

	require.NotNil(t, result.Attributes)
	require.NotEmpty(t, result.Attributes)

	// Check that some expected attributes are present
	_, hasArn := result.Attributes[string(types.QueueAttributeNameQueueArn)]
	require.True(t, hasArn)
}

func Test_Server_getQueueAttributes_specificAttributes(t *testing.T) {
	_, testServer := startTestServer(t)

	// Test getting specific attributes
	result := testHelperGetQueueAttributes(t, testServer, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(testDefaultQueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
			types.QueueAttributeNameVisibilityTimeout,
		},
	})

	require.NotNil(t, result.Attributes)
	require.Len(t, result.Attributes, 2)

	_, hasArn := result.Attributes[string(types.QueueAttributeNameQueueArn)]
	require.True(t, hasArn)

	_, hasVisibilityTimeout := result.Attributes[string(types.QueueAttributeNameVisibilityTimeout)]
	require.True(t, hasVisibilityTimeout)
}

func Test_Server_getQueueAttributes_nonExistentQueue(t *testing.T) {
	_, testServer := startTestServer(t)

	// Test with non-existent queue
	err := testHelperGetQueueAttributesForError(t, testServer, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String("http://sqslite.local/test-account/non-existent"),
	})

	require.Equal(t, "com.amazonaws.sqs#QueueDoesNotExist", err.Type)
}

// Tests for setQueueAttributes

func Test_Server_setQueueAttributes(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-attrs"),
	})

	// Set some attributes including duration-based ones
	result := testHelperSetQueueAttributes(t, testServer, &sqs.SetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout):             "60",
			string(types.QueueAttributeNameMaximumMessageSize):            "131072",
			string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds): "5",
		},
	})

	require.NotNil(t, result)

	// Verify attributes were set by getting them back
	getResult := testHelperGetQueueAttributes(t, testServer, &sqs.GetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameVisibilityTimeout,
			types.QueueAttributeNameMaximumMessageSize,
			types.QueueAttributeNameReceiveMessageWaitTimeSeconds,
			types.QueueAttributeNameQueueArn,
		},
	})

	require.Equal(t, "60", getResult.Attributes[string(types.QueueAttributeNameVisibilityTimeout)])
	require.Equal(t, "131072", getResult.Attributes[string(types.QueueAttributeNameMaximumMessageSize)])
	require.Equal(t, "5", getResult.Attributes[string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds)])
	require.NotEmpty(t, getResult.Attributes[string(types.QueueAttributeNameQueueArn)])
}

func Test_Server_setQueueAttributes_delaySeconds(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue with initial DelaySeconds
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-delay-seconds"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "30",
		},
	})

	// Verify DelaySeconds attribute can be retrieved
	getResult := testHelperGetQueueAttributes(t, testServer, &sqs.GetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameDelaySeconds,
		},
	})

	// Should return the DelaySeconds value as an integer (seconds)
	require.Equal(t, "30", getResult.Attributes[string(types.QueueAttributeNameDelaySeconds)])

	// Test setting DelaySeconds via setQueueAttributes
	testHelperSetQueueAttributes(t, testServer, &sqs.SetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "60",
		},
	})

	// Verify the updated value
	getResult = testHelperGetQueueAttributes(t, testServer, &sqs.GetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameDelaySeconds,
		},
	})

	require.Equal(t, "60", getResult.Attributes[string(types.QueueAttributeNameDelaySeconds)])
}

func Test_Server_setQueueAttributes_nonExistentQueue(t *testing.T) {
	_, testServer := startTestServer(t)

	// Test with non-existent queue
	err := testHelperSetQueueAttributesForError(t, testServer, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String("http://sqslite.local/test-account/non-existent"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): "60",
		},
	})

	require.Equal(t, "com.amazonaws.sqs#QueueDoesNotExist", err.Type)
}

// Tests for listQueueTags

func Test_Server_listQueueTags_emptyTags(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a queue without tags
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-no-tags"),
	})

	// List tags - should be empty
	result := testHelperListQueueTags(t, testServer, &sqs.ListQueueTagsInput{
		QueueUrl: queue.QueueUrl,
	})

	require.NotNil(t, result)
	if result.Tags != nil {
		require.Empty(t, result.Tags)
	}
}

func Test_Server_listQueueTags_nonExistentQueue(t *testing.T) {
	_, testServer := startTestServer(t)

	// Test with non-existent queue
	req, err := http.NewRequest(http.MethodPost, testServer.URL, bytes.NewBufferString(marshalJSON(&sqs.ListQueueTagsInput{
		QueueUrl: aws.String("http://sqslite.local/test-account/non-existent"),
	})))
	require.NoError(t, err)
	req.Header.Set(httputil.HeaderAuthorization, testAuthorizationHeader)
	req.Header.Set(httputil.HeaderContentType, ContentTypeAmzJSON)
	req.Header.Set(HeaderAmzTarget, MethodListQueueTags)
	req.Header.Set(HeaderAmzQueryMode, "true")

	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	defer res.Body.Close()

	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}

// Tests for tagQueue

func Test_Server_tagQueue(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-tags"),
	})

	// Add tags to the queue
	result := testHelperTagQueue(t, testServer, &sqs.TagQueueInput{
		QueueUrl: queue.QueueUrl,
		Tags: map[string]string{
			"Environment": "test",
			"Team":        "backend",
			"Project":     "sqslite",
		},
	})

	require.NotNil(t, result)

	// Verify tags were added by listing them
	listResult := testHelperListQueueTags(t, testServer, &sqs.ListQueueTagsInput{
		QueueUrl: queue.QueueUrl,
	})

	require.Len(t, listResult.Tags, 3)
	require.Equal(t, "test", listResult.Tags["Environment"])
	require.Equal(t, "backend", listResult.Tags["Team"])
	require.Equal(t, "sqslite", listResult.Tags["Project"])
}

func Test_Server_tagQueue_updateExistingTag(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue and add initial tags
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-update-tags"),
	})

	testHelperTagQueue(t, testServer, &sqs.TagQueueInput{
		QueueUrl: queue.QueueUrl,
		Tags: map[string]string{
			"Environment": "staging",
			"Version":     "1.0",
		},
	})

	// Update existing tag and add new one
	testHelperTagQueue(t, testServer, &sqs.TagQueueInput{
		QueueUrl: queue.QueueUrl,
		Tags: map[string]string{
			"Environment": "production", // Update existing
			"Owner":       "team-a",     // Add new
		},
	})

	// Verify tags
	listResult := testHelperListQueueTags(t, testServer, &sqs.ListQueueTagsInput{
		QueueUrl: queue.QueueUrl,
	})

	require.Len(t, listResult.Tags, 3)
	require.Equal(t, "production", listResult.Tags["Environment"]) // Updated
	require.Equal(t, "1.0", listResult.Tags["Version"])            // Unchanged
	require.Equal(t, "team-a", listResult.Tags["Owner"])           // New
}

// Tests for untagQueue

func Test_Server_untagQueue(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue and add tags
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-untag"),
	})

	testHelperTagQueue(t, testServer, &sqs.TagQueueInput{
		QueueUrl: queue.QueueUrl,
		Tags: map[string]string{
			"Environment": "test",
			"Team":        "backend",
			"Project":     "sqslite",
			"Owner":       "admin",
		},
	})

	// Remove some tags
	result := testHelperUntagQueue(t, testServer, &sqs.UntagQueueInput{
		QueueUrl: queue.QueueUrl,
		TagKeys:  []string{"Team", "Owner"},
	})

	require.NotNil(t, result)

	// Verify remaining tags
	listResult := testHelperListQueueTags(t, testServer, &sqs.ListQueueTagsInput{
		QueueUrl: queue.QueueUrl,
	})

	require.Len(t, listResult.Tags, 2)
	require.Equal(t, "test", listResult.Tags["Environment"])
	require.Equal(t, "sqslite", listResult.Tags["Project"])
	_, hasTeam := listResult.Tags["Team"]
	require.False(t, hasTeam)
	_, hasOwner := listResult.Tags["Owner"]
	require.False(t, hasOwner)
}

func Test_Server_untagQueue_nonExistentTags(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue with some tags
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-untag-nonexistent"),
	})

	testHelperTagQueue(t, testServer, &sqs.TagQueueInput{
		QueueUrl: queue.QueueUrl,
		Tags: map[string]string{
			"Environment": "test",
		},
	})

	// Try to remove non-existent tags (should not fail)
	result := testHelperUntagQueue(t, testServer, &sqs.UntagQueueInput{
		QueueUrl: queue.QueueUrl,
		TagKeys:  []string{"NonExistent", "AlsoNonExistent"},
	})

	require.NotNil(t, result)

	// Verify original tag still exists
	listResult := testHelperListQueueTags(t, testServer, &sqs.ListQueueTagsInput{
		QueueUrl: queue.QueueUrl,
	})

	require.Len(t, listResult.Tags, 1)
	require.Equal(t, "test", listResult.Tags["Environment"])
}

// Tests for purgeQueue

func Test_Server_purgeQueue(t *testing.T) {
	server, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-purge"),
	})

	// Send some messages to the queue
	for i := range 5 {
		testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
			QueueUrl:    queue.QueueUrl,
			MessageBody: aws.String(fmt.Sprintf("Test message %d", i)),
		})
	}

	// Verify messages are in the queue
	queueObj, ok := server.accounts.EnsureQueues(testAccountID).GetQueue(*queue.QueueUrl)
	require.True(t, ok)
	require.Equal(t, int64(5), queueObj.Stats().NumMessages)

	// Purge the queue
	result := testHelperPurgeQueue(t, testServer, &sqs.PurgeQueueInput{
		QueueUrl: queue.QueueUrl,
	})

	require.NotNil(t, result)

	// Verify queue is empty
	require.Equal(t, int64(0), queueObj.Stats().NumMessages)
}

func Test_Server_purgeQueue_nonExistentQueue(t *testing.T) {
	_, testServer := startTestServer(t)

	// Test with non-existent queue
	err := testHelperPurgeQueueForError(t, testServer, &sqs.PurgeQueueInput{
		QueueUrl: aws.String("http://sqslite.local/test-account/non-existent"),
	})

	require.Equal(t, "com.amazonaws.sqs#QueueDoesNotExist", err.Type)
}

func Test_Server_purgeQueue_emptyQueue(t *testing.T) {
	server, testServer := startTestServer(t)

	// Create a test queue (no messages)
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-purge-empty"),
	})

	// Verify queue is empty
	queueObj, ok := server.accounts.EnsureQueues(testAccountID).GetQueue(*queue.QueueUrl)
	require.True(t, ok)
	require.Equal(t, int64(0), queueObj.Stats().NumMessages)

	// Purge the empty queue (should succeed)
	result := testHelperPurgeQueue(t, testServer, &sqs.PurgeQueueInput{
		QueueUrl: queue.QueueUrl,
	})

	require.NotNil(t, result)

	// Verify queue is still empty
	require.Equal(t, int64(0), queueObj.Stats().NumMessages)
}

// Tests for deleteMessageBatch

func Test_Server_deleteMessageBatch(t *testing.T) {
	server, testServer := startTestServer(t)

	// Create a test queue with unique name
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-delete-batch-main"),
	})

	// Send some messages
	var messageIds []string
	for i := range 3 {
		sendResult := testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
			QueueUrl:    queue.QueueUrl,
			MessageBody: aws.String(fmt.Sprintf("Message %d", i)),
		})
		messageIds = append(messageIds, *sendResult.MessageId)
	}

	// Receive messages to get receipt handles (might need multiple calls)
	var allMessages []types.Message
	for len(allMessages) < 3 {
		receiveResult := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
			QueueUrl:            queue.QueueUrl,
			MaxNumberOfMessages: 10,
		})
		allMessages = append(allMessages, receiveResult.Messages...)
		if len(receiveResult.Messages) == 0 {
			break // No more messages available
		}
	}

	require.Len(t, allMessages, 3)

	// Prepare batch delete entries
	var entries []types.DeleteMessageBatchRequestEntry
	for i, msg := range allMessages {
		entries = append(entries, types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("entry-%d", i)),
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	// Delete messages in batch
	deleteResult := testHelperDeleteMessageBatch(t, testServer, &sqs.DeleteMessageBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries:  entries,
	})

	require.Len(t, deleteResult.Successful, 3)
	require.Empty(t, deleteResult.Failed)

	// Verify queue is empty
	queueObj, ok := server.accounts.EnsureQueues(testAccountID).GetQueue(*queue.QueueUrl)
	require.True(t, ok)
	require.Equal(t, int64(0), queueObj.Stats().NumMessages)
}

func Test_Server_deleteMessageBatch_invalidReceiptHandle(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-delete-batch-invalid"),
	})

	// Send a message
	testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    queue.QueueUrl,
		MessageBody: aws.String("Test message"),
	})

	// Try to delete with invalid receipt handle
	deleteResult := testHelperDeleteMessageBatch(t, testServer, &sqs.DeleteMessageBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String("entry-1"),
				ReceiptHandle: aws.String("invalid-receipt-handle"),
			},
		},
	})

	require.Empty(t, deleteResult.Successful)
	require.Len(t, deleteResult.Failed, 1)
	require.Equal(t, "entry-1", *deleteResult.Failed[0].Id)
	require.True(t, deleteResult.Failed[0].SenderFault)
}

func Test_Server_deleteMessageBatch_mixedResults(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-delete-batch-mixed"),
	})

	// Send a message
	testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    queue.QueueUrl,
		MessageBody: aws.String("Test message"),
	})

	// Receive message to get receipt handle
	receiveResult := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            queue.QueueUrl,
		MaxNumberOfMessages: 1,
	})

	require.Len(t, receiveResult.Messages, 1)
	validReceiptHandle := receiveResult.Messages[0].ReceiptHandle

	// Mix valid and invalid receipt handles
	deleteResult := testHelperDeleteMessageBatch(t, testServer, &sqs.DeleteMessageBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String("valid-entry"),
				ReceiptHandle: validReceiptHandle,
			},
			{
				Id:            aws.String("invalid-entry"),
				ReceiptHandle: aws.String("invalid-receipt-handle"),
			},
		},
	})

	require.Len(t, deleteResult.Successful, 1)
	require.Len(t, deleteResult.Failed, 1)
	require.Equal(t, "valid-entry", *deleteResult.Successful[0].Id)
	require.Equal(t, "invalid-entry", *deleteResult.Failed[0].Id)
}

// Tests for changeMessageVisibilityBatch

func Test_Server_changeMessageVisibilityBatch(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-change-visibility-batch"),
	})

	// Send some messages
	for i := range 3 {
		testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
			QueueUrl:    queue.QueueUrl,
			MessageBody: aws.String(fmt.Sprintf("Message %d", i)),
		})
	}

	// Receive messages to get receipt handles
	receiveResult := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            queue.QueueUrl,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   5, // Short visibility timeout
	})

	require.Len(t, receiveResult.Messages, 3)

	// Prepare batch change visibility entries
	var entries []types.ChangeMessageVisibilityBatchRequestEntry
	for i, msg := range receiveResult.Messages {
		entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(fmt.Sprintf("entry-%d", i)),
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: 60, // Extend visibility timeout
		})
	}

	// Change message visibility in batch
	changeResult := testHelperChangeMessageVisibilityBatch(t, testServer, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries:  entries,
	})

	require.Len(t, changeResult.Successful, 3)
	require.Empty(t, changeResult.Failed)

	// Verify all entries were successful
	for i, success := range changeResult.Successful {
		require.Equal(t, fmt.Sprintf("entry-%d", i), *success.Id)
	}
}

func Test_Server_changeMessageVisibilityBatch_invalidReceiptHandle(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-change-visibility-batch-invalid"),
	})

	// Send a message
	testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    queue.QueueUrl,
		MessageBody: aws.String("Test message"),
	})

	// Try to change visibility with invalid receipt handle
	changeResult := testHelperChangeMessageVisibilityBatch(t, testServer, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{
				Id:                aws.String("entry-1"),
				ReceiptHandle:     aws.String("invalid-receipt-handle"),
				VisibilityTimeout: 60,
			},
		},
	})

	require.Empty(t, changeResult.Successful)
	require.Len(t, changeResult.Failed, 1)
	require.Equal(t, "entry-1", *changeResult.Failed[0].Id)
	require.True(t, changeResult.Failed[0].SenderFault)
}

func Test_Server_changeMessageVisibilityBatch_mixedResults(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-change-visibility-batch-mixed"),
	})

	// Send a message
	testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    queue.QueueUrl,
		MessageBody: aws.String("Test message"),
	})

	// Receive message to get receipt handle
	receiveResult := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            queue.QueueUrl,
		MaxNumberOfMessages: 1,
	})

	require.Len(t, receiveResult.Messages, 1)
	validReceiptHandle := receiveResult.Messages[0].ReceiptHandle

	// Mix valid and invalid receipt handles
	changeResult := testHelperChangeMessageVisibilityBatch(t, testServer, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{
				Id:                aws.String("valid-entry"),
				ReceiptHandle:     validReceiptHandle,
				VisibilityTimeout: 60,
			},
			{
				Id:                aws.String("invalid-entry"),
				ReceiptHandle:     aws.String("invalid-receipt-handle"),
				VisibilityTimeout: 60,
			},
		},
	})

	require.Len(t, changeResult.Successful, 1)
	require.Len(t, changeResult.Failed, 1)
	require.Equal(t, "valid-entry", *changeResult.Successful[0].Id)
	require.Equal(t, "invalid-entry", *changeResult.Failed[0].Id)
}

func Test_Server_changeMessageVisibilityBatch_invalidVisibilityTimeout(t *testing.T) {
	_, testServer := startTestServer(t)

	// Create a test queue
	queue := testHelperCreateQueue(t, testServer, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue-change-visibility-batch-invalid-timeout"),
	})

	// Send a message
	testHelperSendMessage(t, testServer, &sqs.SendMessageInput{
		QueueUrl:    queue.QueueUrl,
		MessageBody: aws.String("Test message"),
	})

	// Receive message to get receipt handle
	receiveResult := testHelperReceiveMessages(t, testServer, &sqs.ReceiveMessageInput{
		QueueUrl:            queue.QueueUrl,
		MaxNumberOfMessages: 1,
	})

	require.Len(t, receiveResult.Messages, 1)
	validReceiptHandle := receiveResult.Messages[0].ReceiptHandle

	// Test with invalid visibility timeout (too high)
	req, err := http.NewRequest(http.MethodPost, testServer.URL, bytes.NewBufferString(marshalJSON(&sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: queue.QueueUrl,
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{
				Id:                aws.String("entry-1"),
				ReceiptHandle:     validReceiptHandle,
				VisibilityTimeout: 43201, // Too high (max is 43200)
			},
		},
	})))
	require.NoError(t, err)
	req.Header.Set(httputil.HeaderAuthorization, testAuthorizationHeader)
	req.Header.Set(httputil.HeaderContentType, ContentTypeAmzJSON)
	req.Header.Set(HeaderAmzTarget, MethodChangeMessageVisibilityBatch)
	req.Header.Set(HeaderAmzQueryMode, "true")

	res, sendErr := testServer.Client().Do(req)
	require.NoError(t, sendErr)
	defer res.Body.Close()

	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}
