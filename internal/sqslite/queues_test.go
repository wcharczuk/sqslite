package sqslite

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueues_CreatesInstanceWithEmptyMaps(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)

	require.NotNil(t, queues)
	require.Equal(t, testAccountID, queues.AccountID())
}

func TestNewQueues_SetsProvidedClock(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)

	require.Equal(t, clock, queues.clock)
}

func TestNewQueues_InitializesQueueURLsMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)

	require.NotNil(t, queues.queueURLs)
	require.Equal(t, 0, len(queues.queueURLs))
}

func TestNewQueues_InitializesQueuesMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)

	require.NotNil(t, queues.queues)
	require.Equal(t, 0, len(queues.queues))
}

func TestNewQueues_InitializesMoveMessageTasksMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)

	assert.NotNil(t, queues.moveMessageTasks)
	assert.Equal(t, 0, len(queues.moveMessageTasks))
}

func TestQueues_Start_InitializesWorker(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	queues.Start(t.Context())

	require.NotNil(t, queues.deletedQueueWorker)
}

func TestQueues_Start_SetsCancelFunction(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	queues.Start(t.Context())

	require.NotNil(t, queues.deletedQueueWorkerCancel)
}

func TestQueues_Close_ClearsWorkerReference(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	queues.Start(t.Context())

	queues.Close()

	require.Nil(t, queues.deletedQueueWorker)
}

func TestQueues_Close_ClearsCancelFunction(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	queues.Start(t.Context())

	queues.Close()

	require.Nil(t, queues.deletedQueueWorkerCancel)
}

func TestQueues_AddQueue_AddsQueueSuccessfully(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()

	err := queues.AddQueue(queue)

	require.Nil(t, err)
}

func TestQueues_AddQueue_ReturnsErrorForDuplicateNameWithDifferentAttributes(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue1 := createTestQueueWithName(t, clock, "test-queue")
	defer queue1.Close()
	queue2, err2 := NewQueueFromCreateQueueInput(clock, Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName:  aws.String("test-queue"),
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.Nil(t, err2)
	defer queue2.Close()

	queues.AddQueue(queue1)
	err := queues.AddQueue(queue2)

	require.NotNil(t, err)
}

func TestQueues_AddQueue_SucceedsForDuplicateNameWithSameAttributes(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, DefaultAccountID)
	defer queues.Close()
	queue1 := createTestQueueWithName(t, clock, "test-queue")
	defer queue1.Close()
	queue2 := createTestQueueWithName(t, clock, "test-queue")
	defer queue2.Close()

	queues.AddQueue(queue1)
	err := queues.AddQueue(queue2)

	require.Nil(t, err)
}

func TestQueues_PurgeQueue_ReturnsTrueForExistingQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	ok := queues.PurgeQueue(queue.URL)

	require.True(t, ok)
}

func TestQueues_PurgeQueue_ReturnsFalseForNonExistentQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	ok := queues.PurgeQueue("non-existent-url")

	require.False(t, ok)
}

func TestQueues_GetQueueURL_ReturnsURLForExistingQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	url, ok := queues.GetQueueURL("test-queue")

	require.True(t, ok)
	require.Equal(t, queue.URL, url)
}

func TestQueues_GetQueueURL_ReturnsFalseForNonExistentQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	_, ok := queues.GetQueueURL("non-existent")

	require.False(t, ok)
}

func TestQueues_GetQueue_ReturnsQueueForExistingURL(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	retrievedQueue, ok := queues.GetQueue(queue.URL)

	require.True(t, ok)
	require.Equal(t, queue, retrievedQueue)
}

func TestQueues_GetQueue_ReturnsFalseForNonExistentURL(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	_, ok := queues.GetQueue("non-existent-url")

	require.False(t, ok)
}

func TestQueues_DeleteQueue_ReturnsTrueForExistingQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	ok := queues.DeleteQueue(queue.URL)

	require.True(t, ok)
}

func TestQueues_DeleteQueue_ReturnsFalseForNonExistentQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	ok := queues.DeleteQueue("non-existent-url")

	require.False(t, ok)
}

func TestQueues_DeleteQueue_MarksQueueAsDeleted(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	_, ok := queues.GetQueue(queue.URL)
	require.True(t, ok)

	queues.DeleteQueue(queue.URL)

	require.True(t, queue.IsDeleted())
}

func TestQueues_PurgeDeletedQueues_RemovesQueuesDeletedOver60Seconds(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)
	clock.Advance(61 * time.Second)

	queues.PurgeDeletedQueues()

	_, ok := queues.GetQueue(queue.URL)
	require.False(t, ok)
}

func TestQueues_PurgeDeletedQueues_KeepsQueuesDeletedUnder60Seconds(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)
	clock.Advance(59 * time.Second)

	queues.PurgeDeletedQueues()

	_, ok := queues.queues[queue.URL]
	require.True(t, ok)
}

func TestQueues_EachQueue_IteratesOverActiveQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	var count int
	for range queues.EachQueue() {
		count++
	}

	require.Equal(t, 1, count)
}

func TestQueues_EachQueue_SkipsDeletedQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)

	var count int
	for range queues.EachQueue() {
		count++
	}

	require.Equal(t, 0, count)
}

func TestQueues_StartMoveMessageTask_ReturnsErrorForNonExistentSourceArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	_, err := queues.StartMoveMessageTask(clock, "non-existent-arn", "destination-arn", 10)

	require.NotNil(t, err)
}

func TestQueues_StartMoveMessageTask_ReturnsErrorForNonExistentDestinationArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	queues.AddQueue(sourceQueue)

	_, err := queues.StartMoveMessageTask(clock, sourceQueue.ARN, "non-existent-arn", 10)

	require.NotNil(t, err)
}

func TestQueues_StartMoveMessageTask_ReturnsTaskForValidQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithName(t, clock, "dest-queue")
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)

	task, err := queues.StartMoveMessageTask(clock, sourceQueue.ARN, destQueue.ARN, 10)
	defer task.Close()

	require.Nil(t, err)
	require.NotNil(t, task)
}

func TestQueues_CancelMoveMessageTask_ReturnsErrorForNonExistentTask(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	_, err := queues.CancelMoveMessageTask("non-existent-handle")

	require.NotNil(t, err)
}

func TestQueues_CancelMoveMessageTask_TracksTaskInternalMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithName(t, clock, "dest-queue")
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)
	task, _ := queues.StartMoveMessageTask(clock, sourceQueue.ARN, destQueue.ARN, 10)
	defer task.Close()

	_, exists := queues.moveMessageTasks[task.TaskHandle]

	require.True(t, exists)
}

func TestQueues_EachMoveMessageTasks_ReturnsEmptyForNonExistentSourceArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()

	var count int
	for range queues.EachMoveMessageTasks("non-existent-arn") {
		count++
	}

	require.Equal(t, 0, count)
}

func TestQueues_EachMoveMessageTasks_IteratesOverTasksForSourceArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock, testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithName(t, clock, "dest-queue")
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)
	task, _ := queues.StartMoveMessageTask(clock, sourceQueue.ARN, destQueue.ARN, 10)
	defer task.Close()

	var count int
	for range queues.EachMoveMessageTasks(sourceQueue.ARN) {
		count++
	}

	require.Equal(t, 1, count)
}
