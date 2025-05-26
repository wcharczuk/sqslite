package sqslite

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/stretchr/testify/require"
)

func Test_Queue_NewQueueFromCreateQueueInput_minimalDefaults(t *testing.T) {
	q, err := NewQueueFromCreateQueueInput("http://sqslite.local", &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()

	require.Nil(t, err)
	require.Equal(t, "test-queue", q.Name)
	require.Equal(t, "http://sqslite.local/test-queue", q.URL)
	require.NotNil(t, q.messagesReadyOrdered)
	require.NotNil(t, q.messagesReady)
	require.NotNil(t, q.messagesDelayed)
	require.NotNil(t, q.messagesInflight)
	require.NotNil(t, q.messagesInflightByReceiptHandle)

	require.Equal(t, false, q.Delay.IsSet)
	require.Equal(t, 256*1024, q.MaximumMessageSizeBytes)
	require.Equal(t, 4*24*time.Hour, q.MessageRetentionPeriod)
	require.Equal(t, 20*time.Second, q.ReceiveMessageWaitTime)
	require.Equal(t, 30*time.Second, q.VisibilityTimeout)
	require.Equal(t, 120000, q.MaximumMessagesInflight)
}

func Test_Queue_NewMessageFromSendMessageInput(t *testing.T) {
	q, _ := NewQueueFromCreateQueueInput("http://sqslite.local", &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()
	msg := NewMessageFromSendMessageInput(&sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(`{"messageIndex":0}`),
	})
	require.Equal(t, "552cc6a91af25b6aef1e5d1b5e5f54a9", msg.MD5OfBody.Value)
}

func TestQueue_Receive_respectsMaxNumberOfMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push 5 messages to the queue
	pushTestMessages(q, 5)

	// Request only 3 messages
	received := q.Receive(3, 0)

	// Assert that we get at most 3 messages
	require.LessOrEqual(t, len(received), 3)
	require.Equal(t, 3, len(received))
}

func TestQueue_Receive_returnsEmptyWhenMaxInflightReached(t *testing.T) {
	q := createTestQueue(t)

	// Set a very low maximum inflight limit
	q.MaximumMessagesInflight = 2

	// Push 5 messages and receive 2 (filling the inflight limit)
	pushTestMessages(q, 5)
	received1 := q.Receive(2, 0)
	require.Equal(t, 2, len(received1))

	// Try to receive more messages - should return empty
	received2 := q.Receive(5, 0)
	require.Empty(t, received2)
}

func TestQueue_Receive_usesProvidedVisibilityTimeout(t *testing.T) {
	q := createTestQueue(t)
	q.VisibilityTimeout = 30 * time.Second // Default queue timeout

	pushTestMessages(q, 1)
	customTimeout := 60 * time.Second

	// Receive with custom visibility timeout
	received := q.Receive(1, customTimeout)
	require.Len(t, received, 1)

	// Check that the message in inflight state has the custom timeout
	q.mu.Lock()
	msgState := q.messagesInflight[received[0].MessageID]
	require.Equal(t, customTimeout, msgState.VisibilityTimeout)
	q.mu.Unlock()
}

func TestQueue_Receive_usesDefaultVisibilityTimeoutWhenZero(t *testing.T) {
	q := createTestQueue(t)
	defaultTimeout := 45 * time.Second
	q.VisibilityTimeout = defaultTimeout

	pushTestMessages(q, 1)

	// Receive with zero visibility timeout (should use default)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	// Check that the message in inflight state has the default timeout
	q.mu.Lock()
	msgState := q.messagesInflight[received[0].MessageID]
	require.Equal(t, defaultTimeout, msgState.VisibilityTimeout)
	q.mu.Unlock()
}

func TestQueue_Receive_movesMessagesFromReadyToInflight(t *testing.T) {
	q := createTestQueue(t)

	pushTestMessages(q, 3)

	// Verify messages are initially in ready state
	q.mu.Lock()
	require.Len(t, q.messagesReady, 3)
	require.Len(t, q.messagesInflight, 0)
	q.mu.Unlock()

	// Receive 2 messages
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Verify state transition
	q.mu.Lock()
	require.Len(t, q.messagesReady, 1)
	require.Len(t, q.messagesInflight, 2)

	// Verify the received messages are in inflight
	for _, msg := range received {
		_, exists := q.messagesInflight[msg.MessageID]
		require.True(t, exists)
	}
	q.mu.Unlock()
}

func TestQueue_Receive_updatesStatistics(t *testing.T) {
	q := createTestQueue(t)

	// Get initial stats
	initialStats := q.Stats()

	pushTestMessages(q, 3)

	// Receive 2 messages
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Check updated stats
	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesReceived+2, updatedStats.TotalMessagesReceived)
	require.Equal(t, initialStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
	require.Equal(t, initialStats.NumMessagesInflight+2, updatedStats.NumMessagesInflight)
}

func TestQueue_Receive_GeneratesReceiptHandles(t *testing.T) {
	q := createTestQueue(t)

	pushTestMessages(q, 2)

	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Verify each message has a receipt handle
	for _, msg := range received {
		require.True(t, msg.ReceiptHandle.IsSet)
		require.NotEmpty(t, msg.ReceiptHandle.Value)
	}

	// Verify receipt handles are tracked internally
	q.mu.Lock()
	for _, msg := range received {
		msgID, exists := q.messagesInflightByReceiptHandle[msg.ReceiptHandle.Value]
		require.True(t, exists)
		require.Equal(t, msg.MessageID, msgID)
	}
	q.mu.Unlock()
}

func TestQueue_Receive_ReturnsEmptyWhenQueueEmpty(t *testing.T) {
	q := createTestQueue(t)

	// Don't push any messages
	received := q.Receive(10, 0)

	// Should return empty slice
	require.Empty(t, received)
}

func TestQueue_Receive_IncrementsApproximateReceiveCount(t *testing.T) {
	q := createTestQueue(t)

	pushTestMessages(q, 1)

	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	// Check that ApproximateReceiveCount was set
	msg := received[0]
	require.NotNil(t, msg.Attributes)
	count, exists := msg.Attributes["ApproximateReceiveCount"]
	require.True(t, exists)
	require.Equal(t, "1", count)
}

func TestQueue_Receive_SetsLastReceivedTime(t *testing.T) {
	q := createTestQueue(t)

	pushTestMessages(q, 1)

	before := time.Now().UTC()
	received := q.Receive(1, 0)
	after := time.Now().UTC()

	require.Len(t, received, 1)

	// Check that LastReceived was set in the message state
	q.mu.Lock()
	msgState := q.messagesInflight[received[0].MessageID]
	require.True(t, msgState.LastReceived.IsSet)
	require.True(t, msgState.LastReceived.Value.After(before) || msgState.LastReceived.Value.Equal(before))
	require.True(t, msgState.LastReceived.Value.Before(after) || msgState.LastReceived.Value.Equal(after))
	q.mu.Unlock()
}
