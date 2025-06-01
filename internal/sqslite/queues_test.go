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

func createTestQueueWithName(t *testing.T, clock clockwork.Clock, name string) *Queue {
	t.Helper()
	queue, err := NewQueueFromCreateQueueInput(clock, Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	require.Nil(t, err)
	return queue
}

func TestNewQueues_CreatesInstanceWithEmptyMaps(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)

	assert.NotNil(t, queues)
}

func TestNewQueues_SetsProvidedClock(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)

	assert.Equal(t, clock, queues.clock)
}

func TestNewQueues_InitializesQueueURLsMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)

	assert.NotNil(t, queues.queueURLs)
	assert.Equal(t, 0, len(queues.queueURLs))
}

func TestNewQueues_InitializesQueuesMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)

	assert.NotNil(t, queues.queues)
	assert.Equal(t, 0, len(queues.queues))
}

func TestNewQueues_InitializesMoveMessageTasksMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)

	assert.NotNil(t, queues.moveMessageTasks)
	assert.Equal(t, 0, len(queues.moveMessageTasks))
}

func TestQueues_Start_InitializesWorker(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	queues.Start()

	assert.NotNil(t, queues.deletedQueueWorker)
}

func TestQueues_Start_SetsCancelFunction(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	queues.Start()

	assert.NotNil(t, queues.deletedQueueWorkerCancel)
}

func TestQueues_Close_ClearsWorkerReference(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	queues.Start()

	queues.Close()

	assert.Nil(t, queues.deletedQueueWorker)
}

func TestQueues_Close_ClearsCancelFunction(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	queues.Start()

	queues.Close()

	assert.Nil(t, queues.deletedQueueWorkerCancel)
}

func TestQueues_AddQueue_AddsQueueSuccessfully(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()

	err := queues.AddQueue(queue)

	assert.Nil(t, err)
}

func TestQueues_AddQueue_ReturnsErrorForDuplicateNameWithDifferentAttributes(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
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

	assert.NotNil(t, err)
}

func TestQueues_AddQueue_SucceedsForDuplicateNameWithSameAttributes(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue1 := createTestQueueWithName(t, clock, "test-queue")
	defer queue1.Close()
	queue2 := createTestQueueWithName(t, clock, "test-queue")
	defer queue2.Close()

	queues.AddQueue(queue1)
	err := queues.AddQueue(queue2)

	assert.Nil(t, err)
}

func TestQueues_PurgeQueue_ReturnsTrueForExistingQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	ok := queues.PurgeQueue(queue.URL)

	assert.True(t, ok)
}

func TestQueues_PurgeQueue_ReturnsFalseForNonExistentQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	ok := queues.PurgeQueue("non-existent-url")

	assert.False(t, ok)
}

func TestQueues_GetQueueURL_ReturnsURLForExistingQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	url, ok := queues.GetQueueURL("test-queue")

	assert.True(t, ok)
	assert.Equal(t, queue.URL, url)
}

func TestQueues_GetQueueURL_ReturnsFalseForNonExistentQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	_, ok := queues.GetQueueURL("non-existent")

	assert.False(t, ok)
}

func TestQueues_GetQueue_ReturnsQueueForExistingURL(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	retrievedQueue, ok := queues.GetQueue(queue.URL)

	assert.True(t, ok)
	assert.Equal(t, queue, retrievedQueue)
}

func TestQueues_GetQueue_ReturnsFalseForNonExistentURL(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	_, ok := queues.GetQueue("non-existent-url")

	assert.False(t, ok)
}

func TestQueues_DeleteQueue_ReturnsTrueForExistingQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	ok := queues.DeleteQueue(queue.URL)

	assert.True(t, ok)
}

func TestQueues_DeleteQueue_ReturnsFalseForNonExistentQueue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	ok := queues.DeleteQueue("non-existent-url")

	assert.False(t, ok)
}

func TestQueues_DeleteQueue_MarksQueueAsDeleted(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	queues.DeleteQueue(queue.URL)

	assert.True(t, queue.IsDeleted())
}

func TestQueues_PurgeDeletedQueues_RemovesQueuesDeletedOver60Seconds(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)
	clock.Advance(61 * time.Second)

	queues.PurgeDeletedQueues()

	_, ok := queues.GetQueue(queue.URL)
	assert.False(t, ok)
}

func TestQueues_PurgeDeletedQueues_KeepsQueuesDeletedUnder60Seconds(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)
	clock.Advance(59 * time.Second)

	queues.PurgeDeletedQueues()

	_, ok := queues.queues[queue.URL]
	assert.True(t, ok)
}

func TestQueues_EachQueue_IteratesOverActiveQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	var count int
	for range queues.EachQueue() {
		count++
	}

	assert.Equal(t, 1, count)
}

func TestQueues_EachQueue_SkipsDeletedQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	queue := createTestQueueWithName(t, clock, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)

	var count int
	for range queues.EachQueue() {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestQueues_StartMoveMessageTask_ReturnsErrorForNonExistentSourceArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	_, err := queues.StartMoveMessageTask(clock, "non-existent-arn", "destination-arn", 10)

	assert.NotNil(t, err)
}

func TestQueues_StartMoveMessageTask_ReturnsErrorForNonExistentDestinationArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	queues.AddQueue(sourceQueue)

	_, err := queues.StartMoveMessageTask(clock, sourceQueue.ARN, "non-existent-arn", 10)

	assert.NotNil(t, err)
}

func TestQueues_StartMoveMessageTask_ReturnsTaskForValidQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithName(t, clock, "dest-queue")
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)

	task, err := queues.StartMoveMessageTask(clock, sourceQueue.ARN, destQueue.ARN, 10)
	defer task.Close()

	assert.Nil(t, err)
	assert.NotNil(t, task)
}

func TestQueues_CancelMoveMessageTask_ReturnsErrorForNonExistentTask(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	_, err := queues.CancelMoveMessageTask("non-existent-handle")

	assert.NotNil(t, err)
}

func TestQueues_CancelMoveMessageTask_TracksTaskInternalMap(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
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

	assert.True(t, exists)
}

func TestQueues_CancelMoveMessageTask_ReturnsErrorForNonRunningTask(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, clock, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithName(t, clock, "dest-queue")
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)
	task, _ := queues.StartMoveMessageTask(clock, sourceQueue.ARN, destQueue.ARN, 10)

	// Wait for task to complete (it will finish quickly with no messages)
	for range 1000 {
		if task.Status() != MessageMoveStatusRunning && task.Status() != MessageMoveStatusUnknown {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	_, err := queues.CancelMoveMessageTask(task.TaskHandle)

	assert.NotNil(t, err)
}

func TestQueues_EachMoveMessageTasks_ReturnsEmptyForNonExistentSourceArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
	defer queues.Close()

	var count int
	for range queues.EachMoveMessageTasks("non-existent-arn") {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestQueues_EachMoveMessageTasks_IteratesOverTasksForSourceArn(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queues := NewQueues(clock)
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

	assert.Equal(t, 1, count)
}
