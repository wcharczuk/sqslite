package sqslite

import (
	"sqslite/pkg/uuid"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test queue with default settings
func createTestQueue(t *testing.T) *Queue {
	q, err := NewQueueFromCreateQueueInput("http://sqslite.local", &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	require.Nil(t, err)
	t.Cleanup(func() { q.Close() })
	return q
}

// Helper function to create a test message
func createTestMessage(body string) Message {
	return Message{
		MessageID: uuid.V4(),
		Body:      Some(body),
		MD5OfBody: Some("test-md5"),
	}
}

// Helper function to create and push test messages to a queue
func pushTestMessages(q *Queue, count int) []*MessageState {
	var messages []*MessageState
	for range count {
		msg := createTestMessage("test message body")
		msgState, _ := q.NewMessageState(msg, 0)
		messages = append(messages, msgState)
	}
	q.Push(messages...)
	return messages
}

func TestQueue_Receive_RespectsMaxNumberOfMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push 5 messages to the queue
	pushTestMessages(q, 5)

	// Request only 3 messages
	received := q.Receive(3, 0)

	// Assert that we get at most 3 messages
	require.LessOrEqual(t, len(received), 3)
	require.Equal(t, 3, len(received))
}

func TestQueue_Receive_ReturnsEmptyWhenMaxInflightReached(t *testing.T) {
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

func TestQueue_Receive_UsesProvidedVisibilityTimeout(t *testing.T) {
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

func TestQueue_Receive_UsesDefaultVisibilityTimeoutWhenZero(t *testing.T) {
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

func TestQueue_Receive_MovesMessagesFromReadyToInflight(t *testing.T) {
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

func TestQueue_Receive_UpdatesStatistics(t *testing.T) {
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
	require.Equal(t, initialStats.NumMessagesReady+1, updatedStats.NumMessagesReady) // 3 pushed - 2 received = 1 ready
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

func TestQueue_Receive_RespectsFIFOOrdering(t *testing.T) {
	q := createTestQueue(t)

	// Push messages with different bodies to distinguish them
	messages := []Message{
		createTestMessage("first"),
		createTestMessage("second"),
		createTestMessage("third"),
	}

	var messageStates []*MessageState
	for _, msg := range messages {
		msgState, _ := q.NewMessageState(msg, 0)
		messageStates = append(messageStates, msgState)
	}
	q.Push(messageStates...)

	// Receive all messages
	received := q.Receive(3, 0)
	require.Len(t, received, 3)

	// Verify FIFO ordering
	require.Equal(t, "first", received[0].Body.Value)
	require.Equal(t, "second", received[1].Body.Value)
	require.Equal(t, "third", received[2].Body.Value)
}
