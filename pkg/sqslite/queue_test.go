package sqslite

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

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

func Test_Queue_NewQueueFromCreateQueueInput_invalidName(t *testing.T) {
	_, err := NewQueueFromCreateQueueInput("http://sqslite.local", &sqs.CreateQueueInput{
		QueueName: aws.String("test!!!queue"),
	})

	require.NotNil(t, err)
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

func TestQueue_Receive_generatesReceiptHandles(t *testing.T) {
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

func TestQueue_Receive_returnsEmptyWhenQueueEmpty(t *testing.T) {
	q := createTestQueue(t)

	// Don't push any messages
	received := q.Receive(10, 0)

	// Should return empty slice
	require.Empty(t, received)
}

func TestQueue_Receive_incrementsApproximateReceiveCount(t *testing.T) {
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

func TestQueue_Receive_setsLastReceivedTime(t *testing.T) {
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

func TestQueue_Push_singleMessageWithoutDelay_addsToReadyQueue(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0)

	q.Push(msgState)

	q.mu.Lock()
	require.Len(t, q.messagesReady, 1)
	q.mu.Unlock()
}

func TestQueue_Push_singleMessageWithDelay_addsToDelayedQueue(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 10) // 10 second delay

	q.Push(msgState)

	q.mu.Lock()
	require.Len(t, q.messagesDelayed, 1)
	q.mu.Unlock()
}

func TestQueue_Push_multipleMessages_handlesAllMessages(t *testing.T) {
	q := createTestQueue(t)

	msg1 := createTestMessage("test body 1")
	msgState1, _ := q.NewMessageState(msg1, 0)
	msg2 := createTestMessage("test body 2")
	msgState2, _ := q.NewMessageState(msg2, 0)

	q.Push(msgState1, msgState2)

	q.mu.Lock()
	require.Len(t, q.messagesReady, 2)
	q.mu.Unlock()
}

func TestQueue_Push_queueHasDefaultDelayMessageHasNone_appliesQueueDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(5 * time.Second)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0) // No delay on message

	q.Push(msgState)

	q.mu.Lock()
	require.Len(t, q.messagesDelayed, 1)
	q.mu.Unlock()
}

func TestQueue_Push_queueHasDefaultDelayMessageHasDelay_keepsMessageDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(5 * time.Second)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 10) // Message has its own delay

	q.Push(msgState)

	// Message should still be delayed, but keep its original delay
	q.mu.Lock()
	require.Equal(t, 10*time.Second, msgState.Delay.Value)
	q.mu.Unlock()
}

func TestQueue_Push_incrementsTotalMessagesSent(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesSent+1, updatedStats.TotalMessagesSent)
}

func TestQueue_Push_incrementsNumMessages(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessages+1, updatedStats.NumMessages)
}

func TestQueue_Push_nonDelayedMessage_incrementsNumMessagesReady(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func TestQueue_Push_delayedMessage_incrementsNumMessagesDelayed(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 10)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessagesDelayed+1, updatedStats.NumMessagesDelayed)
}

func TestQueue_Push_readyMessage_addsToMessagesReadyMap(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0)

	q.Push(msgState)

	q.mu.Lock()
	_, exists := q.messagesReady[msg.MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func TestQueue_Push_delayedMessage_addsToMessagesDelayedMap(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 10)

	q.Push(msgState)

	q.mu.Lock()
	_, exists := q.messagesDelayed[msg.MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func TestQueue_Push_readyMessage_addsToOrderedLinkedList(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, 0)

	q.Push(msgState)

	q.mu.Lock()
	require.Equal(t, 1, q.messagesReadyOrdered.Len())
	q.mu.Unlock()
}

func TestQueue_Push_noMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.Push() // Call with no arguments

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

func TestQueue_Push_mixedDelayedAndReadyMessages_handlesCorrectly(t *testing.T) {
	q := createTestQueue(t)

	readyMsg := createTestMessage("ready")
	readyMsgState, _ := q.NewMessageState(readyMsg, 0)
	delayedMsg := createTestMessage("delayed")
	delayedMsgState, _ := q.NewMessageState(delayedMsg, 10)

	q.Push(readyMsgState, delayedMsgState)

	q.mu.Lock()
	require.Len(t, q.messagesReady, 1)
	require.Len(t, q.messagesDelayed, 1)
	q.mu.Unlock()
}

func TestQueue_Delete_validReceiptHandle_returnsTrue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message to get it in inflight state
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	result := q.Delete(received[0].ReceiptHandle.Value)

	require.True(t, result)
}

func TestQueue_Delete_invalidReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	result := q.Delete("invalid-receipt-handle")

	require.False(t, result)
}

func TestQueue_Delete_removesMessageFromInflightMap(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	q.Delete(received[0].ReceiptHandle.Value)

	q.mu.Lock()
	_, exists := q.messagesInflight[messageID]
	require.False(t, exists)
	q.mu.Unlock()
}

func TestQueue_Delete_removesReceiptHandleFromMap(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	receiptHandle := received[0].ReceiptHandle.Value

	q.Delete(receiptHandle)

	q.mu.Lock()
	_, exists := q.messagesInflightByReceiptHandle[receiptHandle]
	require.False(t, exists)
	q.mu.Unlock()
}

func TestQueue_Delete_removesAllReceiptHandlesForMessage(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Simulate multiple receipt handles for the same message
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	extraHandle := "extra-handle"
	msgState.ReceiptHandles.Add(extraHandle)
	q.messagesInflightByReceiptHandle[extraHandle] = messageID
	q.mu.Unlock()

	q.Delete(received[0].ReceiptHandle.Value)

	q.mu.Lock()
	_, exists := q.messagesInflightByReceiptHandle[extraHandle]
	require.False(t, exists)
	q.mu.Unlock()
}

func TestQueue_Delete_incrementsTotalMessagesDeleted(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	q.Delete(received[0].ReceiptHandle.Value)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesDeleted+1, updatedStats.TotalMessagesDeleted)
}

func TestQueue_Delete_decrementsNumMessagesInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	afterReceiveStats := q.Stats()

	q.Delete(received[0].ReceiptHandle.Value)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesInflight-1, updatedStats.NumMessagesInflight)
}

func TestQueue_Delete_decrementsNumMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	afterReceiveStats := q.Stats()

	q.Delete(received[0].ReceiptHandle.Value)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessages-1, updatedStats.NumMessages)
}

func TestQueue_Delete_receiptHandleNotInMap_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message but use a different receipt handle
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	result := q.Delete("non-existent-receipt-handle")

	require.False(t, result)
}

func TestQueue_Delete_messageIdNotInInflight_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	receiptHandle := received[0].ReceiptHandle.Value
	messageID := received[0].MessageID

	// Manually remove the message from inflight but leave the receipt handle mapping
	q.mu.Lock()
	delete(q.messagesInflight, messageID)
	q.mu.Unlock()

	result := q.Delete(receiptHandle)

	require.False(t, result)
}

func TestQueue_Delete_emptyReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	result := q.Delete("")

	require.False(t, result)
}

func TestQueue_Delete_sameReceiptHandleTwice_secondReturnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	receiptHandle := received[0].ReceiptHandle.Value

	// First delete should succeed
	firstResult := q.Delete(receiptHandle)
	require.True(t, firstResult)

	// Second delete should fail
	secondResult := q.Delete(receiptHandle)

	require.False(t, secondResult)
}

func TestQueue_Delete_multipleMessages_deletesOnlySpecified(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive multiple messages
	pushTestMessages(q, 3)
	received := q.Receive(3, 0)
	require.Len(t, received, 3)

	// Delete only the first message
	q.Delete(received[0].ReceiptHandle.Value)

	q.mu.Lock()
	// First message should be gone
	_, exists1 := q.messagesInflight[received[0].MessageID]
	require.False(t, exists1)
	// Other messages should still be there
	_, exists2 := q.messagesInflight[received[1].MessageID]
	require.True(t, exists2)
	_, exists3 := q.messagesInflight[received[2].MessageID]
	require.True(t, exists3)
	q.mu.Unlock()
}

func TestQueue_DeleteBatch_allValidReceiptHandles_returnsAllSuccessful(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive multiple messages
	pushTestMessages(q, 3)
	received := q.Receive(3, 0)
	require.Len(t, received, 3)

	// Create batch request
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value)},
		{Id: aws.String("msg3"), ReceiptHandle: aws.String(received[2].ReceiptHandle.Value)},
	}

	successful, failed := q.DeleteBatch(entriesSlice)

	require.Len(t, successful, 3)
	require.Len(t, failed, 0)
}

func TestQueue_DeleteBatch_allInvalidReceiptHandles_returnsAllFailed(t *testing.T) {
	q := createTestQueue(t)

	// Create batch request with invalid receipt handles
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String("invalid1")},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String("invalid2")},
	}

	successful, failed := q.DeleteBatch(entriesSlice)

	require.Len(t, successful, 0)
	require.Len(t, failed, 2)
}

func TestQueue_DeleteBatch_mixedValidInvalid_returnsMixedResults(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive one message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	// Create batch request with one valid and one invalid
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("valid"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("invalid-handle")},
	}

	successful, failed := q.DeleteBatch(entriesSlice)

	require.Len(t, successful, 1)
	require.Len(t, failed, 1)
}

func TestQueue_DeleteBatch_emptySlice_returnsEmptyResults(t *testing.T) {
	q := createTestQueue(t)

	successful, failed := q.DeleteBatch([]types.DeleteMessageBatchRequestEntry{})

	require.Len(t, successful, 0)
	require.Len(t, failed, 0)
}

func TestQueue_DeleteBatch_removesSuccessfulMessagesFromInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request with one valid handle
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
	}

	q.DeleteBatch(entriesSlice)

	q.mu.Lock()
	_, exists := q.messagesInflight[received[0].MessageID]
	require.False(t, exists)
	// Second message should still be there
	_, exists = q.messagesInflight[received[1].MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func TestQueue_DeleteBatch_removesSuccessfulReceiptHandles(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
	}

	q.DeleteBatch(entriesSlice)

	q.mu.Lock()
	_, exists := q.messagesInflightByReceiptHandle[received[0].ReceiptHandle.Value]
	require.False(t, exists)
	// Second message receipt handle should still be there
	_, exists = q.messagesInflightByReceiptHandle[received[1].ReceiptHandle.Value]
	require.True(t, exists)
	q.mu.Unlock()
}

func TestQueue_DeleteBatch_removesAllReceiptHandlesForSuccessfulMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Add an extra receipt handle for the same message
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	extraHandle := "extra-handle"
	msgState.ReceiptHandles.Add(extraHandle)
	q.messagesInflightByReceiptHandle[extraHandle] = messageID
	q.mu.Unlock()

	// Delete the message
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
	}

	q.DeleteBatch(entriesSlice)

	q.mu.Lock()
	_, exists := q.messagesInflightByReceiptHandle[extraHandle]
	require.False(t, exists)
	q.mu.Unlock()
}

func TestQueue_DeleteBatch_incrementsTotalMessagesDeletedForSuccessfulOnly(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive one message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	// Create batch with one valid and one invalid
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("valid"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("invalid-handle")},
	}

	q.DeleteBatch(entriesSlice)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesDeleted+1, updatedStats.TotalMessagesDeleted)
}

func TestQueue_DeleteBatch_decrementsNumMessagesInflightForSuccessfulOnly(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)
	afterReceiveStats := q.Stats()

	// Create batch with one valid handle
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
	}

	q.DeleteBatch(entriesSlice)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesInflight-1, updatedStats.NumMessagesInflight)
}

func TestQueue_DeleteBatch_decrementsNumMessagesForSuccessfulOnly(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)
	afterReceiveStats := q.Stats()

	// Create batch with one valid handle
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
	}

	q.DeleteBatch(entriesSlice)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessages-1, updatedStats.NumMessages)
}

func TestQueue_DeleteBatch_receiptHandleNotFound_returnsInvalidParameterValueError(t *testing.T) {
	q := createTestQueue(t)

	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("not-found")},
	}

	successful, failed := q.DeleteBatch(entriesSlice)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1)
	require.Equal(t, "InvalidParameterValue", *failed[0].Code)
	require.Equal(t, "invalid", *failed[0].Id)
	require.True(t, failed[0].SenderFault)
}

func TestQueue_DeleteBatch_messageIdNotInInflight_returnsInconsistentStateError(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	receiptHandle := received[0].ReceiptHandle.Value
	messageID := received[0].MessageID

	// Manually remove the message from inflight but leave the receipt handle mapping
	q.mu.Lock()
	delete(q.messagesInflight, messageID)
	q.mu.Unlock()

	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("inconsistent"), ReceiptHandle: aws.String(receiptHandle)},
	}

	successful, failed := q.DeleteBatch(entriesSlice)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1)
	require.Equal(t, "InconsistentState", *failed[0].Code)
	require.Equal(t, "inconsistent", *failed[0].Id)
	require.False(t, failed[0].SenderFault)
}

func TestQueue_DeleteBatch_preservesFailedMessagesInInflightState(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch with one valid and one invalid
	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("valid"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("invalid-handle")},
	}

	q.DeleteBatch(entriesSlice)

	q.mu.Lock()
	// Valid message should be deleted
	_, exists1 := q.messagesInflight[received[0].MessageID]
	require.False(t, exists1)
	// Invalid request shouldn't affect other messages
	_, exists2 := q.messagesInflight[received[1].MessageID]
	require.True(t, exists2)
	q.mu.Unlock()
}

func TestQueue_DeleteBatch_returnsCorrectIDsInSuccessfulResults(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("first-msg"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value)},
		{Id: aws.String("second-msg"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value)},
	}

	successful, _ := q.DeleteBatch(entriesSlice)

	require.Len(t, successful, 2)
	require.Equal(t, "first-msg", *successful[0].Id)
	require.Equal(t, "second-msg", *successful[1].Id)
}

func TestQueue_DeleteBatch_returnsCorrectErrorDetailsInFailedResults(t *testing.T) {
	q := createTestQueue(t)

	entriesSlice := []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("error1"), ReceiptHandle: aws.String("invalid1")},
		{Id: aws.String("error2"), ReceiptHandle: aws.String("invalid2")},
	}

	_, failed := q.DeleteBatch(entriesSlice)

	require.Len(t, failed, 2)
	require.Equal(t, "error1", *failed[0].Id)
	require.Equal(t, "error2", *failed[1].Id)
	require.Equal(t, "ReceiptHandle not found", *failed[0].Message)
	require.Equal(t, "ReceiptHandle not found", *failed[1].Message)
}

func TestQueue_ChangeMessageVisibility_validReceiptHandle_returnsTrue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message to get it in inflight state
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	result := q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 60*time.Second)

	require.True(t, result)
}

func TestQueue_ChangeMessageVisibility_invalidReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	result := q.ChangeMessageVisibility("invalid-receipt-handle", 60*time.Second)

	require.False(t, result)
}

func TestQueue_ChangeMessageVisibility_updatesMessageVisibilityTimeout(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID
	newTimeout := 120 * time.Second

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, newTimeout)

	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	require.Equal(t, newTimeout, msgState.VisibilityTimeout)
	q.mu.Unlock()
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_makesMessageImmediatelyVisible(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	q.mu.Lock()
	msgState := q.messagesReady[messageID]
	require.True(t, msgState.Value.VisibilityDeadline.IsZero())
	q.mu.Unlock()
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_movesMessageToReadyQueue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	q.mu.Lock()
	_, exists := q.messagesReady[messageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_removesFromInflightQueue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	q.mu.Lock()
	_, exists := q.messagesInflight[messageID]
	require.False(t, exists)
	q.mu.Unlock()
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_incrementsTotalMessagesChangedVisibility(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesChangedVisibility+1, updatedStats.TotalMessagesChangedVisibility)
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_incrementsNumMessagesReady(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	afterReceiveStats := q.Stats()

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_decrementsNumMessagesInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	afterReceiveStats := q.Stats()

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesInflight-1, updatedStats.NumMessagesInflight)
}

func TestQueue_ChangeMessageVisibility_zeroTimeout_removesAllReceiptHandles(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Add an extra receipt handle for the same message
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	extraHandle := "extra-handle"
	msgState.ReceiptHandles.Add(extraHandle)
	q.messagesInflightByReceiptHandle[extraHandle] = messageID
	q.mu.Unlock()

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 0)

	q.mu.Lock()
	_, exists := q.messagesInflightByReceiptHandle[extraHandle]
	require.False(t, exists)
	q.mu.Unlock()
}

func TestQueue_ChangeMessageVisibility_nonZeroTimeout_keepsMessageInInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 60*time.Second)

	q.mu.Lock()
	_, exists := q.messagesInflight[messageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func TestQueue_ChangeMessageVisibility_receiptHandleNotFound_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message but use a different receipt handle
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	result := q.ChangeMessageVisibility("non-existent-receipt-handle", 60*time.Second)

	require.False(t, result)
}

func TestQueue_ChangeMessageVisibility_messageIdNotInInflight_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	receiptHandle := received[0].ReceiptHandle.Value
	messageID := received[0].MessageID

	// Manually remove the message from inflight but leave the receipt handle mapping
	q.mu.Lock()
	delete(q.messagesInflight, messageID)
	q.mu.Unlock()

	result := q.ChangeMessageVisibility(receiptHandle, 60*time.Second)

	require.False(t, result)
}

func TestQueue_ChangeMessageVisibility_emptyReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	result := q.ChangeMessageVisibility("", 60*time.Second)

	require.False(t, result)
}

func TestQueue_ChangeMessageVisibility_nonZeroTimeout_doesNotUpdateStatistics(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 60*time.Second)

	updatedStats := q.Stats()
	// TotalMessagesChangedVisibility should NOT be incremented for non-zero timeout
	require.Equal(t, initialStats.TotalMessagesChangedVisibility, updatedStats.TotalMessagesChangedVisibility)
}

func TestQueue_ChangeMessageVisibility_setsVisibilityDeadlineCorrectly(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID
	newTimeout := 90 * time.Second

	before := time.Now().UTC()
	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, newTimeout)
	after := time.Now().UTC()

	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	require.True(t, msgState.VisibilityDeadline.IsSet)
	// Check that the deadline is approximately correct (within reasonable bounds)
	expectedDeadline := before.Add(newTimeout)
	latestDeadline := after.Add(newTimeout)
	require.True(t, msgState.VisibilityDeadline.Value.After(expectedDeadline) || msgState.VisibilityDeadline.Value.Equal(expectedDeadline))
	require.True(t, msgState.VisibilityDeadline.Value.Before(latestDeadline) || msgState.VisibilityDeadline.Value.Equal(latestDeadline))
	q.mu.Unlock()
}
