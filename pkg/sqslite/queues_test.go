package sqslite

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test queue with custom name and URL
func createTestQueueWithNameAndURL(name, url string) *Queue {
	q, _ := NewQueueFromCreateQueueInput(ServerConfig{
		BaseURL: "http://sqslite.local",
	}, "test-account", &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	q.URL = url // Override URL for testing
	return q
}

func Test_NewQueues_returnsNewInstance(t *testing.T) {
	queues := NewQueues()

	require.NotNil(t, queues)
	require.NotNil(t, queues.queueURLs)
	require.NotNil(t, queues.queues)
	require.Empty(t, queues.queueURLs)
	require.Empty(t, queues.queues)
}

func Test_Queues_Close_closesAllQueues(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	// Create and add test queues
	queue1 := createTestQueueWithNameAndURL("test-queue-1", "http://sqslite.local/test-queue-1")
	queue2 := createTestQueueWithNameAndURL("test-queue-2", "http://sqslite.local/test-queue-2")

	queues.AddQueue(ctx, queue1)
	queues.AddQueue(ctx, queue2)

	queue1.Start()
	queue2.Start()

	// Close all queues
	queues.Close()

	require.Nil(t, queue1.visibilityWorkerCancel)
	require.Nil(t, queue1.delayWorkerCancel)
	require.Nil(t, queue1.retentionWorkerCancel)

	require.Nil(t, queue2.visibilityWorkerCancel)
	require.Nil(t, queue2.delayWorkerCancel)
	require.Nil(t, queue2.retentionWorkerCancel)

	// Verify queues are closed (we can't directly check if Close() was called,
	// but we can verify the structure is still intact)
	require.Len(t, queues.queues, 2)
}

func Test_Queues_AddQueue_addsNewQueue(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")

	err := queues.AddQueue(ctx, queue)

	require.Nil(t, err)
	require.Equal(t, "http://sqslite.local/test-queue", queues.queueURLs[QueueName{AccountID: "test-account", QueueName: "test-queue"}])
	require.Equal(t, queue, queues.queues["http://sqslite.local/test-queue"])
}

func Test_Queues_AddQueue_returnsErrorWhenQueueExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue1 := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue-1")
	queue2 := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue-2")

	// Create first queue
	err1 := queues.AddQueue(ctx, queue1)
	require.Nil(t, err1)

	// Attempt to create second queue with same name
	err2 := queues.AddQueue(ctx, queue2)

	require.NotNil(t, err2)
	require.Contains(t, err2.Message, "queue already exists with name: test-queue")
	require.Equal(t, "InvalidParameterValue", err2.Code)
}

func Test_Queues_AddQueue_allowsDifferentNames(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue1 := createTestQueueWithNameAndURL("queue-1", "http://sqslite.local/queue-1")
	queue2 := createTestQueueWithNameAndURL("queue-2", "http://sqslite.local/queue-2")

	err1 := queues.AddQueue(ctx, queue1)
	err2 := queues.AddQueue(ctx, queue2)

	require.Nil(t, err1)
	require.Nil(t, err2)
	require.Len(t, queues.queueURLs, 2)
	require.Len(t, queues.queues, 2)
}

func Test_Queues_PurgeQueue_returnsTrueWhenQueueExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queues.AddQueue(ctx, queue)

	ok := queues.PurgeQueue(ctx, "http://sqslite.local/test-queue")

	require.True(t, ok)
}

func Test_Queues_PurgeQueue_returnsFalseWhenQueueNotExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	ok := queues.PurgeQueue(ctx, "http://sqslite.local/nonexistent")

	require.False(t, ok)
}

func Test_Queues_ListQueues_returnsEmptyWhenNoQueues(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	result, err := queues.ListQueues(ctx)

	require.NoError(t, err)
	require.Empty(t, result)
}

func Test_Queues_ListQueues_returnsAllQueues(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue1 := createTestQueueWithNameAndURL("queue-1", "http://sqslite.local/queue-1")
	queue2 := createTestQueueWithNameAndURL("queue-2", "http://sqslite.local/queue-2")

	queues.AddQueue(ctx, queue1)
	queues.AddQueue(ctx, queue2)

	result, err := queues.ListQueues(ctx)

	require.NoError(t, err)
	require.Len(t, result, 2)
	// Note: order is not guaranteed in map iteration
	require.Contains(t, result, queue1)
	require.Contains(t, result, queue2)
}

func Test_Queues_GetQueueURL_returnsTrueWhenQueueExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queues.AddQueue(ctx, queue)

	url, ok := queues.GetQueueURL(ctx, "test-queue")

	require.True(t, ok)
	require.Equal(t, "http://sqslite.local/test-queue", url)
}

func Test_Queues_GetQueueURL_returnsFalseWhenQueueNotExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	url, ok := queues.GetQueueURL(ctx, "nonexistent")

	require.False(t, ok)
	require.Empty(t, url)
}

func Test_Queues_GetQueue_returnsTrueWhenQueueExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queue.created = time.Now().UTC().Add(-time.Minute)
	queues.AddQueue(ctx, queue)

	retrievedQueue, err := queues.GetQueue(ctx, "http://sqslite.local/test-queue")
	require.Nil(t, err)
	require.Equal(t, queue, retrievedQueue)
}

func Test_Queues_GetQueue_returnsFalseWhenQueueIsRecentlyCreated(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queues.AddQueue(ctx, queue)

	retrievedQueue, err := queues.GetQueue(ctx, "http://sqslite.local/test-queue")
	require.NotNil(t, err)
	require.Nil(t, retrievedQueue)
}

func Test_Queues_GetQueue_returnsFalseWhenQueueNotExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	retrievedQueue, err := queues.GetQueue(ctx, "http://sqslite.local/nonexistent")

	require.NotNil(t, err)
	require.Nil(t, retrievedQueue)
}

func Test_Queues_DeleteQueue_returnsTrueWhenQueueExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queues.AddQueue(ctx, queue)

	ok := queues.DeleteQueue(ctx, "http://sqslite.local/test-queue")

	require.True(t, ok)
	// Verify queue is removed from both maps
	_, urlExists := queues.queueURLs[QueueName{AccountID: "test-account", QueueName: "test-queue"}]
	_, queueExists := queues.queues["http://sqslite.local/test-queue"]
	require.False(t, urlExists)
	require.False(t, queueExists)
}

func Test_Queues_DeleteQueue_returnsFalseWhenQueueNotExists(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	ok := queues.DeleteQueue(ctx, "http://sqslite.local/nonexistent")

	require.False(t, ok)
}

func Test_Queues_DeleteQueue_removesFromBothMaps(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})
	queue := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queues.AddQueue(ctx, queue)

	// Verify queue exists in both maps
	_, urlExists := queues.queueURLs[QueueName{AccountID: "test-account", QueueName: "test-queue"}]
	_, queueExists := queues.queues["http://sqslite.local/test-queue"]
	require.True(t, urlExists)
	require.True(t, queueExists)

	queues.DeleteQueue(ctx, "http://sqslite.local/test-queue")

	// Verify queue is removed from both maps
	_, urlExists = queues.queueURLs[QueueName{AccountID: "test-account", QueueName: "test-queue"}]
	_, queueExists = queues.queues["http://sqslite.local/test-queue"]
	require.False(t, urlExists)
	require.False(t, queueExists)
}

func Test_Queues_createAndDeleteCycle_maintainConsistency(t *testing.T) {
	queues := NewQueues()
	ctx := WithContextAuthorization(context.Background(), Authorization{AccountID: "test-account"})

	// Create, delete, and recreate the same queue
	queue1 := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queue1.created = time.Now().UTC().Add(-time.Minute)
	err1 := queues.AddQueue(ctx, queue1)
	require.Nil(t, err1)

	ok := queues.DeleteQueue(ctx, "http://sqslite.local/test-queue")
	require.True(t, ok)

	// Should be able to create queue with same name again
	queue2 := createTestQueueWithNameAndURL("test-queue", "http://sqslite.local/test-queue")
	queue2.created = time.Now().UTC().Add(-time.Minute)
	err2 := queues.AddQueue(ctx, queue2)
	require.Nil(t, err2)

	// Verify new queue is accessible
	retrievedQueue, err := queues.GetQueue(ctx, "http://sqslite.local/test-queue")
	require.Nil(t, err)
	require.Equal(t, queue2, retrievedQueue)
}
