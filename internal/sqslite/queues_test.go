package sqslite

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewQueues_createsInstanceWithEmptyMaps(t *testing.T) {
	queues := NewQueues(testAccountID)

	require.NotNil(t, queues)
	require.Equal(t, testAccountID, queues.AccountID())
}

func Test_NewQueues_initializesQueueURLsMap(t *testing.T) {
	queues := NewQueues(testAccountID)

	require.NotNil(t, queues.queueURLs)
	require.Equal(t, 0, len(queues.queueURLs))
}

func Test_NewQueues_initializesQueuesMap(t *testing.T) {
	queues := NewQueues(testAccountID)

	require.NotNil(t, queues.queues)
	require.Equal(t, 0, len(queues.queues))
}

func Test_NewQueues_initializesMoveMessageTasksMap(t *testing.T) {
	queues := NewQueues(testAccountID)

	assert.NotNil(t, queues.moveMessageTasks)
	assert.Equal(t, 0, len(queues.moveMessageTasks))
}

func Test_Queues_Start_initializesWorker(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	queues.Start(t.Context())

	require.NotNil(t, queues.deletedQueueWorker)
}

func Test_Queues_Start_setsCancelFunction(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	queues.Start(t.Context())

	require.NotNil(t, queues.deletedQueueWorkerCancel)
}

func Test_Queues_Close_clearsWorkerReference(t *testing.T) {
	queues := NewQueues(testAccountID)
	queues.Start(t.Context())

	queues.Close()

	require.Nil(t, queues.deletedQueueWorker)
}

func Test_Queues_Close_clearsCancelFunction(t *testing.T) {
	queues := NewQueues(testAccountID)
	queues.Start(t.Context())

	queues.Close()

	require.Nil(t, queues.deletedQueueWorkerCancel)
}

func Test_Queues_AddQueue_addsQueueSuccessfully(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()

	err := queues.AddQueue(queue)

	require.Nil(t, err)
}

func Test_Queues_AddQueue_returnsErrorForDuplicateNameWithDifferentAttributes(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue1 := createTestQueueWithName(t, "test-queue")
	defer queue1.Close()
	queue2, err2 := NewQueueFromCreateQueueInput(Authorization{
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

func Test_Queues_AddQueue_succeedsForDuplicateNameWithSameAttributes(t *testing.T) {
	queues := NewQueues(DefaultAccountID)
	defer queues.Close()
	queue1 := createTestQueueWithName(t, "test-queue")
	defer queue1.Close()
	queue2 := createTestQueueWithName(t, "test-queue")
	defer queue2.Close()

	queues.AddQueue(queue1)
	err := queues.AddQueue(queue2)

	require.Nil(t, err)
}

func Test_Queues_PurgeQueue_returnsTrueForExistingQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	ok := queues.PurgeQueue(queue.URL)

	require.True(t, ok)
}

func Test_Queues_PurgeQueue_returnsFalseForNonExistentQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	ok := queues.PurgeQueue("non-existent-url")

	require.False(t, ok)
}

func Test_Queues_GetQueueURL_returnsURLForExistingQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	url, ok := queues.GetQueueURL("test-queue")

	require.True(t, ok)
	require.Equal(t, queue.URL, url)
}

func Test_Queues_GetQueueURL_returnsFalseForNonExistentQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	_, ok := queues.GetQueueURL("non-existent")

	require.False(t, ok)
}

func Test_Queues_GetQueue_returnsQueueForExistingURL(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	retrievedQueue, ok := queues.GetQueue(queue.URL)

	require.True(t, ok)
	require.Equal(t, queue, retrievedQueue)
}

func Test_Queues_GetQueue_returnsFalseForNonExistentURL(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	_, ok := queues.GetQueue("non-existent-url")

	require.False(t, ok)
}

func Test_Queues_DeleteQueue_returnsTrueForExistingQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	ok := queues.DeleteQueue(queue.URL)

	require.True(t, ok)
}

func Test_Queues_DeleteQueue_returnsFalseForNonExistentQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	ok := queues.DeleteQueue("non-existent-url")

	require.False(t, ok)
}

func Test_Queues_DeleteQueue_marksQueueAsDeleted(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	_, ok := queues.GetQueue(queue.URL)
	require.True(t, ok)

	queues.DeleteQueue(queue.URL)

	require.True(t, queue.IsDeleted())
}

func Test_Queues_PurgeDeletedQueues_removesQueuesDeletedOver60Seconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queues := NewQueues(testAccountID)
		defer queues.Close()
		queue := createTestQueueWithName(t, "test-queue")
		defer queue.Close()
		queues.AddQueue(queue)
		queues.DeleteQueue(queue.URL)
		time.Sleep(61 * time.Second)

		queues.PurgeDeletedQueues()

		_, ok := queues.GetQueue(queue.URL)
		require.False(t, ok)
	})
}

func Test_Queues_PurgeDeletedQueues_keepsQueuesDeletedUnder60Seconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queues := NewQueues(testAccountID)
		defer queues.Close()
		queue := createTestQueueWithName(t, "test-queue")
		defer queue.Close()
		queues.AddQueue(queue)
		queues.DeleteQueue(queue.URL)
		time.Sleep(59 * time.Second)

		queues.PurgeDeletedQueues()

		_, ok := queues.queues[queue.URL]
		require.True(t, ok)
	})
}

func Test_Queues_EachQueue_iteratesOverActiveQueues(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)

	var count int
	for range queues.EachQueue() {
		count++
	}

	require.Equal(t, 1, count)
}

func Test_Queues_EachQueue_skipsDeletedQueues(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	queue := createTestQueueWithName(t, "test-queue")
	defer queue.Close()
	queues.AddQueue(queue)
	queues.DeleteQueue(queue.URL)

	var count int
	for range queues.EachQueue() {
		count++
	}

	require.Equal(t, 0, count)
}

func Test_Queues_StartMoveMessageTask_returnsErrorForNonExistentSourceArn(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	_, err := queues.StartMoveMessageTask("non-existent-arn", "destination-arn", 10)

	require.NotNil(t, err)
}

func Test_Queues_StartMoveMessageTask_returnsErrorForNonExistentDestinationArn(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, "source-queue")
	defer sourceQueue.Close()
	queues.AddQueue(sourceQueue)
	_, err := queues.StartMoveMessageTask(sourceQueue.ARN, "non-existent-arn", 10)
	require.NotNil(t, err)
}

func Test_Queues_StartMoveMessageTask_returnsTaskForValidQueues(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithNameWithDLQ(t, "dest-queue", sourceQueue)
	defer destQueue.Close()

	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)

	task, err := queues.StartMoveMessageTask(sourceQueue.ARN, destQueue.ARN, 10)
	defer task.Close()

	require.Nil(t, err)
	require.NotNil(t, task)
}

func Test_Queues_StartMoveMessageTask_allowsEmptyDestinationQueue(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithNameWithDLQ(t, "dest-queue", sourceQueue)
	defer destQueue.Close()

	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)

	task, err := queues.StartMoveMessageTask(sourceQueue.ARN, "", 10)
	defer task.Close()

	require.Nil(t, err)
	require.NotNil(t, task)
}

func Test_Queues_StartMoveMessageTask_enforcesRedriveAllowPolicy(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithNameWithRedriveAllowPolicy(t, "dest-queue", RedriveAllowPolicy{
		RedrivePermission: RedrivePermissionDenyAll,
	})
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)

	task, err := queues.StartMoveMessageTask(sourceQueue.ARN, "", 10)
	require.NotNil(t, err)
	require.Nil(t, task)
}

func Test_Queues_CancelMoveMessageTask_returnsErrorForNonExistentTask(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	_, err := queues.CancelMoveMessageTask("non-existent-handle")

	require.NotNil(t, err)
}

func Test_Queues_CancelMoveMessageTask_tracksTaskInternalMap(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithNameWithDLQ(t, "dest-queue", sourceQueue)
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)
	task, _ := queues.StartMoveMessageTask(sourceQueue.ARN, destQueue.ARN, 10)
	defer task.Close()

	_, exists := queues.moveMessageTasks[task.TaskHandle]

	require.True(t, exists)
}

func Test_Queues_EachMoveMessageTasks_returnsEmptyForNonExistentSourceArn(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()

	var count int
	for range queues.EachMoveMessageTasks("non-existent-arn") {
		count++
	}

	require.Equal(t, 0, count)
}

func Test_Queues_EachMoveMessageTasks_iteratesOverTasksForSourceArn(t *testing.T) {
	queues := NewQueues(testAccountID)
	defer queues.Close()
	sourceQueue := createTestQueueWithName(t, "source-queue")
	defer sourceQueue.Close()
	destQueue := createTestQueueWithNameWithDLQ(t, "dest-queue", sourceQueue)
	defer destQueue.Close()
	queues.AddQueue(sourceQueue)
	queues.AddQueue(destQueue)

	task, err := queues.StartMoveMessageTask(sourceQueue.ARN, destQueue.ARN, 10)
	require.Nil(t, err)
	defer task.Close()

	var count int
	for range queues.EachMoveMessageTasks(sourceQueue.ARN) {
		count++
	}

	require.Equal(t, 1, count)
}
