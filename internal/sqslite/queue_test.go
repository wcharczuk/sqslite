package sqslite

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"

	"github.com/stretchr/testify/require"
)

func Test_Queue_NewQueueFromCreateQueueInput_minimalDefaults(t *testing.T) {
	q, err := NewQueueFromCreateQueueInput(clockwork.NewFakeClock(), Authorization{
		Region:    "us-west-2",
		AccountID: "test-account",
		Host:      "sqslite.local",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()

	require.Nil(t, err)
	require.Equal(t, "test-queue", q.Name)
	require.Equal(t, "http://sqslite.local/test-account/test-queue", q.URL)
	require.Equal(t, "arn:aws:sqs:us-west-2:test-account:test-queue", q.ARN)
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
	_, err := NewQueueFromCreateQueueInput(clockwork.NewFakeClock(), Authorization{
		Region:    "us-west-2",
		AccountID: "test-account",
		Host:      "sqslite.local",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test!!!queue"),
	})
	require.NotNil(t, err)
}

func Test_Queue_NewMessageFromSendMessageInput(t *testing.T) {
	q, _ := NewQueueFromCreateQueueInput(clockwork.NewFakeClock(), Authorization{
		Region:    "us-west-2",
		AccountID: "test-account",
		Host:      "sqslite.local",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()
	msg := NewMessageFromSendMessageInput(&sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(`{"messageIndex":0}`),
	})
	require.Equal(t, "552cc6a91af25b6aef1e5d1b5e5f54a9", msg.MD5OfBody.Value)
}

func Test_Queue_Receive_respectsMaxNumberOfMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push 5 messages to the queue
	pushTestMessages(q, 5)

	// Request only 3 messages
	received := q.Receive(3, 0)

	// Assert that we get at most 3 messages
	require.LessOrEqual(t, len(received), 3)
	require.Equal(t, 3, len(received))
}

func Test_Queue_Receive_setsApproximateReceiveCountAttribute(t *testing.T) {
	q := createTestQueue(t)

	// Push 5 messages to the queue
	pushTestMessages(q, 5)

	// Request only 3 messages
	received := q.Receive(3, 0)

	// Assert that we get at most 3 messages
	require.LessOrEqual(t, len(received), 3)
	require.Equal(t, 3, len(received))

	require.Equal(t, "1", received[0].Attributes[MessageAttributeApproximateReceiveCount])
	require.Equal(t, "1", received[1].Attributes[MessageAttributeApproximateReceiveCount])
	require.Equal(t, "1", received[2].Attributes[MessageAttributeApproximateReceiveCount])
}

func Test_Queue_Receive_returnsEmptyWhenMaxInflightReached(t *testing.T) {
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

func Test_Queue_Receive_usesProvidedVisibilityTimeout(t *testing.T) {
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

func Test_Queue_Receive_usesDefaultVisibilityTimeoutWhenZero(t *testing.T) {
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

func Test_Queue_Receive_movesMessagesFromReadyToInflight(t *testing.T) {
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

func Test_Queue_Receive_updatesStatistics(t *testing.T) {
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

func Test_Queue_Receive_generatesReceiptHandles(t *testing.T) {
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

func Test_Queue_Receive_returnsEmptyWhenQueueEmpty(t *testing.T) {
	q := createTestQueue(t)

	// Don't push any messages
	received := q.Receive(10, 0)

	// Should return empty slice
	require.Empty(t, received)
}

func Test_Queue_Receive_incrementsApproximateReceiveCount(t *testing.T) {
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

func Test_Queue_Receive_setsLastReceivedTime(t *testing.T) {
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

func Test_Queue_Push_singleMessageWithoutDelay_addsToReadyQueue(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)

	q.Push(msgState)

	q.mu.Lock()
	require.Len(t, q.messagesReady, 1)
	q.mu.Unlock()
}

func Test_Queue_Push_singleMessageWithDelay_addsToDelayedQueue(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 10) // 10 second delay

	q.Push(msgState)

	q.mu.Lock()
	require.Len(t, q.messagesDelayed, 1)
	q.mu.Unlock()
}

func Test_Queue_Push_multipleMessages_handlesAllMessages(t *testing.T) {
	q := createTestQueue(t)

	msg1 := createTestMessage("test body 1")
	msgState1, _ := q.NewMessageState(msg1, time.Now().UTC(), 0)
	msg2 := createTestMessage("test body 2")
	msgState2, _ := q.NewMessageState(msg2, time.Now().UTC(), 0)

	q.Push(msgState1, msgState2)

	q.mu.Lock()
	require.Len(t, q.messagesReady, 2)
	q.mu.Unlock()
}

func Test_Queue_Push_queueHasDefaultDelayMessageHasNone_appliesQueueDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(5 * time.Second)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0) // No delay on message

	q.Push(msgState)

	q.mu.Lock()
	require.Len(t, q.messagesDelayed, 1)
	q.mu.Unlock()
}

func Test_Queue_Push_queueHasDefaultDelayMessageHasDelay_keepsMessageDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(5 * time.Second)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 10) // Message has its own delay

	q.Push(msgState)

	// Message should still be delayed, but keep its original delay
	q.mu.Lock()
	require.Equal(t, 10*time.Second, msgState.Delay.Value)
	q.mu.Unlock()
}

func Test_Queue_Push_incrementsTotalMessagesSent(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesSent+1, updatedStats.TotalMessagesSent)
}

func Test_Queue_Push_incrementsNumMessages(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessages+1, updatedStats.NumMessages)
}

func Test_Queue_Push_nonDelayedMessage_incrementsNumMessagesReady(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func Test_Queue_Push_delayedMessage_incrementsNumMessagesDelayed(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 10)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessagesDelayed+1, updatedStats.NumMessagesDelayed)
}

func Test_Queue_Push_readyMessage_addsToMessagesReadyMap(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)

	q.Push(msgState)

	q.mu.Lock()
	_, exists := q.messagesReady[msg.MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_Push_delayedMessage_addsToMessagesDelayedMap(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 10)

	q.Push(msgState)

	q.mu.Lock()
	_, exists := q.messagesDelayed[msg.MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_Push_readyMessage_addsToOrderedLinkedList(t *testing.T) {
	q := createTestQueue(t)

	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)

	q.Push(msgState)

	q.mu.Lock()
	require.Equal(t, 1, q.messagesReadyOrdered.Len())
	q.mu.Unlock()
}

func Test_Queue_Push_noMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.Push() // Call with no arguments

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

func Test_Queue_Push_mixedDelayedAndReadyMessages_handlesCorrectly(t *testing.T) {
	q := createTestQueue(t)

	readyMsg := createTestMessage("ready")
	readyMsgState, _ := q.NewMessageState(readyMsg, time.Now().UTC(), 0)
	delayedMsg := createTestMessage("delayed")
	delayedMsgState, _ := q.NewMessageState(delayedMsg, time.Now().UTC(), 10)

	q.Push(readyMsgState, delayedMsgState)

	q.mu.Lock()
	require.Len(t, q.messagesReady, 1)
	require.Len(t, q.messagesDelayed, 1)
	q.mu.Unlock()
}

func Test_Queue_Delete_validReceiptHandle_returnsTrue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message to get it in inflight state
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	err := q.Delete(received[0].ReceiptHandle.Value)

	require.Nil(t, err)
}

func Test_Queue_Delete_invalidReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	err := q.Delete("invalid-receipt-handle")

	require.NotNil(t, err)
}

func Test_Queue_Delete_removesMessageFromInflightMap(t *testing.T) {
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

func Test_Queue_Delete_removesReceiptHandleFromMap(t *testing.T) {
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

func Test_Queue_Delete_removesAllReceiptHandlesForMessage(t *testing.T) {
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

func Test_Queue_Delete_incrementsTotalMessagesDeleted(t *testing.T) {
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

func Test_Queue_Delete_decrementsNumMessagesInflight(t *testing.T) {
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

func Test_Queue_Delete_decrementsNumMessages(t *testing.T) {
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

func Test_Queue_Delete_receiptHandleNotInMap_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message but use a different receipt handle
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	err := q.Delete("non-existent-receipt-handle")

	require.NotNil(t, err)
}

func Test_Queue_Delete_messageIdNotInInflight_returnsFalse(t *testing.T) {
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

	err := q.Delete(receiptHandle)

	require.NotNil(t, err)
}

func Test_Queue_Delete_emptyReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	err := q.Delete("")

	require.NotNil(t, err)
}

func Test_Queue_Delete_sameReceiptHandleTwice_secondReturnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	receiptHandle := received[0].ReceiptHandle.Value

	// First delete should succeed
	err := q.Delete(receiptHandle)
	require.Nil(t, err)

	// Second delete should fail
	err = q.Delete(receiptHandle)

	require.NotNil(t, err)
}

func Test_Queue_Delete_multipleMessages_deletesOnlySpecified(t *testing.T) {
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

func Test_Queue_DeleteBatch_allValidReceiptHandles_returnsAllSuccessful(t *testing.T) {
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

func Test_Queue_DeleteBatch_allInvalidReceiptHandles_returnsAllFailed(t *testing.T) {
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

func Test_Queue_DeleteBatch_mixedValidInvalid_returnsMixedResults(t *testing.T) {
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

func Test_Queue_DeleteBatch_emptySlice_returnsEmptyResults(t *testing.T) {
	q := createTestQueue(t)

	successful, failed := q.DeleteBatch([]types.DeleteMessageBatchRequestEntry{})

	require.Len(t, successful, 0)
	require.Len(t, failed, 0)
}

func Test_Queue_DeleteBatch_removesSuccessfulMessagesFromInflight(t *testing.T) {
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

func Test_Queue_DeleteBatch_removesSuccessfulReceiptHandles(t *testing.T) {
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

func Test_Queue_DeleteBatch_removesAllReceiptHandlesForSuccessfulMessages(t *testing.T) {
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

func Test_Queue_DeleteBatch_incrementsTotalMessagesDeletedForSuccessfulOnly(t *testing.T) {
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

func Test_Queue_DeleteBatch_decrementsNumMessagesInflightForSuccessfulOnly(t *testing.T) {
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

func Test_Queue_DeleteBatch_decrementsNumMessagesForSuccessfulOnly(t *testing.T) {
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

func Test_Queue_DeleteBatch_receiptHandleNotFound_returnsInvalidParameterValueError(t *testing.T) {
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

func Test_Queue_DeleteBatch_messageIdNotInInflight_returnsInconsistentStateError(t *testing.T) {
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

func Test_Queue_DeleteBatch_preservesFailedMessagesInInflightState(t *testing.T) {
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

func Test_Queue_DeleteBatch_returnsCorrectIDsInSuccessfulResults(t *testing.T) {
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

func Test_Queue_DeleteBatch_returnsCorrectErrorDetailsInFailedResults(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_validReceiptHandle_returnsTrue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message to get it in inflight state
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	result := q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 60*time.Second)

	require.True(t, result)
}

func Test_Queue_ChangeMessageVisibility_invalidReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	result := q.ChangeMessageVisibility("invalid-receipt-handle", 60*time.Second)

	require.False(t, result)
}

func Test_Queue_ChangeMessageVisibility_updatesMessageVisibilityTimeout(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_makesMessageImmediatelyVisible(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_movesMessageToReadyQueue(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_removesFromInflightQueue(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_incrementsTotalMessagesChangedVisibility(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_incrementsNumMessagesReady(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_decrementsNumMessagesInflight(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_zeroTimeout_removesAllReceiptHandles(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_nonZeroTimeout_keepsMessageInInflight(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_receiptHandleNotFound_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message but use a different receipt handle
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	result := q.ChangeMessageVisibility("non-existent-receipt-handle", 60*time.Second)

	require.False(t, result)
}

func Test_Queue_ChangeMessageVisibility_messageIdNotInInflight_returnsFalse(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibility_emptyReceiptHandle_returnsFalse(t *testing.T) {
	q := createTestQueue(t)

	result := q.ChangeMessageVisibility("", 60*time.Second)

	require.False(t, result)
}

func Test_Queue_ChangeMessageVisibility_nonZeroTimeout_doesNotUpdateStatistics(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	q.ChangeMessageVisibility(received[0].ReceiptHandle.Value, 60*time.Second)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesChangedVisibility+1, updatedStats.TotalMessagesChangedVisibility)
}

func Test_Queue_ChangeMessageVisibility_setsVisibilityDeadlineCorrectly(t *testing.T) {
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

func Test_Queue_ChangeMessageVisibilityBatch_allValidEntries_returnsAllSuccessful(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive multiple messages
	pushTestMessages(q, 3)
	received := q.Receive(3, 0)
	require.Len(t, received, 3)

	// Create batch request
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 60},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 90},
		{Id: aws.String("msg3"), ReceiptHandle: aws.String(received[2].ReceiptHandle.Value), VisibilityTimeout: 120},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entriesSlice)

	require.Len(t, successful, 3)
	require.Len(t, failed, 0)
}

func Test_Queue_ChangeMessageVisibilityBatch_allInvalidEntries_returnsAllFailed(t *testing.T) {
	q := createTestQueue(t)

	// Create batch request with invalid receipt handles
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String("invalid1"), VisibilityTimeout: 60},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String("invalid2"), VisibilityTimeout: 90},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entriesSlice)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1) // Early return, only first failure reported
}

func Test_Queue_ChangeMessageVisibilityBatch_mixedValidInvalid_failsEarly(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive one message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)

	// Create batch request with valid first, invalid second
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("valid"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 60},
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("invalid-handle"), VisibilityTimeout: 90},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entriesSlice)

	// Should process valid entry and fail on invalid, early return
	require.Len(t, successful, 1)
	require.Len(t, failed, 1)
}

func Test_Queue_ChangeMessageVisibilityBatch_emptySlice_returnsEmptyResults(t *testing.T) {
	q := createTestQueue(t)

	successful, failed := q.ChangeMessageVisibilityBatch([]types.ChangeMessageVisibilityBatchRequestEntry{})

	require.Len(t, successful, 0)
	require.Len(t, failed, 0)
}

func Test_Queue_ChangeMessageVisibilityBatch_updatesVisibilityTimeoutsForSuccessfulEntries(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request with different timeouts
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 60},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 90},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	q.mu.Lock()
	msgState1 := q.messagesInflight[received[0].MessageID]
	msgState2 := q.messagesInflight[received[1].MessageID]
	require.Equal(t, 60*time.Second, msgState1.VisibilityTimeout)
	require.Equal(t, 90*time.Second, msgState2.VisibilityTimeout)
	q.mu.Unlock()
}

func Test_Queue_ChangeMessageVisibilityBatch_zeroTimeoutEntries_movesToReadyQueue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request with zero timeout for first message
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 60},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	q.mu.Lock()
	// First message should be in ready queue
	_, exists := q.messagesReady[received[0].MessageID]
	require.True(t, exists)
	// Second message should still be in inflight
	_, exists = q.messagesInflight[received[1].MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_ChangeMessageVisibilityBatch_zeroTimeoutEntries_removesFromInflightQueue(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request with zero timeout for first message
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 60},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	q.mu.Lock()
	// First message should NOT be in inflight queue
	_, exists := q.messagesInflight[received[0].MessageID]
	require.False(t, exists)
	// Second message should still be in inflight
	_, exists = q.messagesInflight[received[1].MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_ChangeMessageVisibilityBatch_incrementsTotalMessagesChangedVisibilityForAllEntries(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request with mixed zero and non-zero timeouts
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 60},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesChangedVisibility+2, updatedStats.TotalMessagesChangedVisibility)
}

func Test_Queue_ChangeMessageVisibilityBatch_zeroTimeout_incrementsNumMessagesReady(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)
	afterReceiveStats := q.Stats()

	// Create batch request with one zero timeout
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 60},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func Test_Queue_ChangeMessageVisibilityBatch_zeroTimeout_decrementsNumMessagesInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)
	afterReceiveStats := q.Stats()

	// Create batch request with one zero timeout
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 60},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesInflight-1, updatedStats.NumMessagesInflight)
}

func Test_Queue_ChangeMessageVisibilityBatch_zeroTimeout_removesAllReceiptHandles(t *testing.T) {
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

	// Create batch request with zero timeout
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	q.mu.Lock()
	_, exists := q.messagesInflightByReceiptHandle[extraHandle]
	require.False(t, exists)
	q.mu.Unlock()
}

func Test_Queue_ChangeMessageVisibilityBatch_nonZeroTimeout_keepsMessagesInInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Create batch request with non-zero timeouts
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("msg1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 60},
		{Id: aws.String("msg2"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 90},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	q.mu.Lock()
	_, exists1 := q.messagesInflight[received[0].MessageID]
	_, exists2 := q.messagesInflight[received[1].MessageID]
	require.True(t, exists1)
	require.True(t, exists2)
	q.mu.Unlock()
}

func Test_Queue_ChangeMessageVisibilityBatch_receiptHandleNotFound_returnsInvalidParameterValueError(t *testing.T) {
	q := createTestQueue(t)

	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("not-found"), VisibilityTimeout: 60},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entriesSlice)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1)
	require.Equal(t, "InvalidParameterValue", *failed[0].Code)
	require.Equal(t, "invalid", *failed[0].Id)
	require.True(t, failed[0].SenderFault)
}

func Test_Queue_ChangeMessageVisibilityBatch_messageIdNotInInflight_returnsInternalServerError(t *testing.T) {
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

	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("inconsistent"), ReceiptHandle: aws.String(receiptHandle), VisibilityTimeout: 60},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entriesSlice)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1)
	require.Equal(t, "InternalServerError", *failed[0].Code)
	require.Equal(t, "inconsistent", *failed[0].Id)
	require.False(t, failed[0].SenderFault)
}

func Test_Queue_ChangeMessageVisibilityBatch_earlyReturnBehavior_stopsProcessingOnFirstFailure(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)
	initialStats := q.Stats()

	// Create batch with invalid first entry, valid second entry
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("invalid"), ReceiptHandle: aws.String("invalid-handle"), VisibilityTimeout: 60},
		{Id: aws.String("valid"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 90},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	updatedStats := q.Stats()
	// Should NOT process any entries due to early return
	require.Equal(t, initialStats.TotalMessagesChangedVisibility, updatedStats.TotalMessagesChangedVisibility)
}

func Test_Queue_ChangeMessageVisibilityBatch_returnsCorrectIDsInSuccessfulResults(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("first-msg"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 60},
		{Id: aws.String("second-msg"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 90},
	}

	successful, _ := q.ChangeMessageVisibilityBatch(entriesSlice)

	require.Len(t, successful, 2)
	require.Equal(t, "first-msg", *successful[0].Id)
	require.Equal(t, "second-msg", *successful[1].Id)
}

func Test_Queue_ChangeMessageVisibilityBatch_returnsCorrectErrorDetailsInFailedResults(t *testing.T) {
	q := createTestQueue(t)

	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("error1"), ReceiptHandle: aws.String("invalid1"), VisibilityTimeout: 60},
	}

	_, failed := q.ChangeMessageVisibilityBatch(entriesSlice)

	require.Len(t, failed, 1)
	require.Equal(t, "error1", *failed[0].Id)
	require.Equal(t, "ReceiptHandle not found", *failed[0].Message)
}

func Test_Queue_ChangeMessageVisibilityBatch_mixedZeroAndNonZeroTimeouts_handlesCorrectly(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive messages
	pushTestMessages(q, 3)
	received := q.Receive(3, 0)
	require.Len(t, received, 3)

	// Create batch with mixed timeouts
	entriesSlice := []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("zero1"), ReceiptHandle: aws.String(received[0].ReceiptHandle.Value), VisibilityTimeout: 0},
		{Id: aws.String("nonzero"), ReceiptHandle: aws.String(received[1].ReceiptHandle.Value), VisibilityTimeout: 60},
		{Id: aws.String("zero2"), ReceiptHandle: aws.String(received[2].ReceiptHandle.Value), VisibilityTimeout: 0},
	}

	q.ChangeMessageVisibilityBatch(entriesSlice)

	q.mu.Lock()
	// Zero timeout messages should be in ready queue
	_, exists1 := q.messagesReady[received[0].MessageID]
	_, exists3 := q.messagesReady[received[2].MessageID]
	require.True(t, exists1)
	require.True(t, exists3)
	// Non-zero timeout message should still be in inflight
	_, exists2 := q.messagesInflight[received[1].MessageID]
	require.True(t, exists2)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_noInflightMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.UpdateInflightToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

func Test_Queue_UpdateInflightToReady_noVisibleMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message with a future visibility deadline
	pushTestMessages(q, 1)
	received := q.Receive(1, 60*time.Second) // 60 second visibility timeout
	require.Len(t, received, 1)
	initialStats := q.Stats()

	q.UpdateInflightToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesInflightToReady, updatedStats.TotalMessagesInflightToReady)
}

func Test_Queue_UpdateInflightToReady_withVisibleMessages_movesToReady(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Make the message visible by setting visibility deadline to past
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, exists := q.messagesReady[messageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_mixedVisibleNonVisible_movesOnlyVisible(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Make first message visible (past deadline), keep second non-visible (future deadline)
	q.mu.Lock()
	msgState1 := q.messagesInflight[received[0].MessageID]
	msgState2 := q.messagesInflight[received[1].MessageID]
	msgState1.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	msgState2.VisibilityDeadline = Some(time.Now().UTC().Add(60 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	// First message should be in ready
	_, exists1 := q.messagesReady[received[0].MessageID]
	require.True(t, exists1)
	// Second message should still be in inflight
	_, exists2 := q.messagesInflight[received[1].MessageID]
	require.True(t, exists2)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_removesVisibleMessagesFromInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Make the message visible
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, exists := q.messagesInflight[messageID]
	require.False(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_addsVisibleMessagesToReady(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Make the message visible
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, exists := q.messagesReady[messageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_addsVisibleMessagesToOrderedList(t *testing.T) {
	q := createTestQueue(t)
	initialListLen := q.messagesReadyOrdered.Len()

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Make the message visible
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	require.Equal(t, initialListLen+1, q.messagesReadyOrdered.Len())
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_removesAllReceiptHandlesForVisibleMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID
	receiptHandle := received[0].ReceiptHandle.Value

	// Add an extra receipt handle for the same message
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	extraHandle := "extra-handle"
	msgState.ReceiptHandles.Add(extraHandle)
	q.messagesInflightByReceiptHandle[extraHandle] = messageID
	// Make the message visible
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, exists1 := q.messagesInflightByReceiptHandle[receiptHandle]
	_, exists2 := q.messagesInflightByReceiptHandle[extraHandle]
	require.False(t, exists1)
	require.False(t, exists2)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_incrementsTotalMessagesInflightToReady(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Make both messages visible
	q.mu.Lock()
	msgState1 := q.messagesInflight[received[0].MessageID]
	msgState2 := q.messagesInflight[received[1].MessageID]
	msgState1.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	msgState2.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesInflightToReady+2, updatedStats.TotalMessagesInflightToReady)
}

func Test_Queue_UpdateInflightToReady_incrementsNumMessagesReadyForVisibleMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	afterReceiveStats := q.Stats()

	// Make the message visible
	q.mu.Lock()
	msgState := q.messagesInflight[received[0].MessageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func Test_Queue_UpdateInflightToReady_decrementsNumMessagesInflightForVisibleMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	afterReceiveStats := q.Stats()

	// Make the message visible
	q.mu.Lock()
	msgState := q.messagesInflight[received[0].MessageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	updatedStats := q.Stats()
	require.Equal(t, afterReceiveStats.NumMessagesInflight-1, updatedStats.NumMessagesInflight)
}

func Test_Queue_UpdateInflightToReady_expiredVisibilityDeadline_movesMessage(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Set visibility deadline to 1 second ago
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, inInflight := q.messagesInflight[messageID]
	_, inReady := q.messagesReady[messageID]
	require.False(t, inInflight)
	require.True(t, inReady)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_futureVisibilityDeadline_keepsMessageInInflight(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Set visibility deadline to 60 seconds in the future
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = Some(time.Now().UTC().Add(60 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, inInflight := q.messagesInflight[messageID]
	_, inReady := q.messagesReady[messageID]
	require.True(t, inInflight)
	require.False(t, inReady)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_zeroVisibilityDeadline_movesMessage(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive a message
	pushTestMessages(q, 1)
	received := q.Receive(1, 0)
	require.Len(t, received, 1)
	messageID := received[0].MessageID

	// Set visibility deadline to zero (no deadline)
	q.mu.Lock()
	msgState := q.messagesInflight[messageID]
	msgState.VisibilityDeadline = None[time.Time]()
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	_, inInflight := q.messagesInflight[messageID]
	_, inReady := q.messagesReady[messageID]
	require.False(t, inInflight)
	require.True(t, inReady)
	q.mu.Unlock()
}

func Test_Queue_UpdateInflightToReady_preservesNonVisibleMessagesInInflightState(t *testing.T) {
	q := createTestQueue(t)

	// Push and receive two messages
	pushTestMessages(q, 2)
	received := q.Receive(2, 0)
	require.Len(t, received, 2)

	// Make first message visible, keep second non-visible
	q.mu.Lock()
	msgState1 := q.messagesInflight[received[0].MessageID]
	msgState2 := q.messagesInflight[received[1].MessageID]
	msgState1.VisibilityDeadline = Some(time.Now().UTC().Add(-1 * time.Second))
	msgState2.VisibilityDeadline = Some(time.Now().UTC().Add(60 * time.Second))
	q.mu.Unlock()

	q.UpdateInflightToReady()

	q.mu.Lock()
	// Second message should remain unchanged in inflight
	_, exists := q.messagesInflight[received[1].MessageID]
	require.True(t, exists)
	// Its receipt handle should still be there
	_, exists = q.messagesInflightByReceiptHandle[received[1].ReceiptHandle.Value]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_noDelayedMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.UpdateDelayedToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

func Test_Queue_UpdateDelayedToReady_noReadyMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with future delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 10) // 10 second delay
	q.Push(msgState)
	initialStats := q.Stats()

	q.UpdateDelayedToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesDelayedToReady, updatedStats.TotalMessagesDelayedToReady)
}

func Test_Queue_UpdateDelayedToReady_withReadyMessages_movesToReady(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1) // 1 second delay
	q.Push(msgState)
	messageID := msg.MessageID

	// Make the message ready by setting creation time to past
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[messageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second) // Created 2 seconds ago
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	_, exists := q.messagesReady[messageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_mixedReadyDelayed_movesOnlyReady(t *testing.T) {
	q := createTestQueue(t)

	// Push two messages with delays
	msg1 := createTestMessage("test body 1")
	msgState1, _ := q.NewMessageState(msg1, time.Now().UTC(), 1) // 1 second delay
	msg2 := createTestMessage("test body 2")
	msgState2, _ := q.NewMessageState(msg2, time.Now().UTC(), 10) // 10 second delay
	q.Push(msgState1, msgState2)

	// Make first message ready (expired delay), keep second delayed
	q.mu.Lock()
	delayedMsg1 := q.messagesDelayed[msg1.MessageID]
	delayedMsg1.Created = time.Now().UTC().Add(-2 * time.Second) // Created 2 seconds ago, delay expired
	// msg2 keeps its current creation time, so delay is still active
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	// First message should be in ready
	_, exists1 := q.messagesReady[msg1.MessageID]
	require.True(t, exists1)
	// Second message should still be in delayed
	_, exists2 := q.messagesDelayed[msg2.MessageID]
	require.True(t, exists2)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_removesReadyMessagesFromDelayed(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	messageID := msg.MessageID
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1) // 1 second delay
	q.Push(msgState)

	// Make the message ready
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[messageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	_, exists := q.messagesDelayed[messageID]
	require.False(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_addsReadyMessagesToReady(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1) // 1 second delay
	q.Push(msgState)
	messageID := msg.MessageID

	// Make the message ready
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[messageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	_, exists := q.messagesReady[messageID]
	require.True(t, exists)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_addsReadyMessagesToOrderedList(t *testing.T) {
	q := createTestQueue(t)
	initialListLen := q.messagesReadyOrdered.Len()

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1) // 1 second delay
	q.Push(msgState)
	messageID := msg.MessageID

	// Make the message ready
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[messageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	require.Equal(t, initialListLen+1, q.messagesReadyOrdered.Len())
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_incrementsTotalMessagesDelayedToReady(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	// Push two messages with delays
	msg1 := createTestMessage("test body 1")
	msgState1, _ := q.NewMessageState(msg1, time.Now().UTC(), 1)
	msg2 := createTestMessage("test body 2")
	msgState2, _ := q.NewMessageState(msg2, time.Now().UTC(), 1)
	q.Push(msgState1, msgState2)

	// Make both messages ready
	q.mu.Lock()
	delayedMsg1 := q.messagesDelayed[msg1.MessageID]
	delayedMsg2 := q.messagesDelayed[msg2.MessageID]
	delayedMsg1.Created = time.Now().UTC().Add(-2 * time.Second)
	delayedMsg2.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats.TotalMessagesDelayedToReady+2, updatedStats.TotalMessagesDelayedToReady)
}

func Test_Queue_UpdateDelayedToReady_incrementsNumMessagesReadyForReadyMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1)
	q.Push(msgState)
	afterPushStats := q.Stats()

	// Make the message ready
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[msg.MessageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	updatedStats := q.Stats()
	require.Equal(t, afterPushStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func Test_Queue_UpdateDelayedToReady_decrementsNumMessagesDelayedForReadyMessages(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1)
	q.Push(msgState)
	afterPushStats := q.Stats()

	// Make the message ready
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[msg.MessageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	updatedStats := q.Stats()
	require.Equal(t, afterPushStats.NumMessagesDelayed-1, updatedStats.NumMessagesDelayed)
}

func Test_Queue_UpdateDelayedToReady_expiredDelayPeriod_movesMessage(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1) // 1 second delay
	q.Push(msgState)
	messageID := msg.MessageID

	// Set creation time to 2 seconds ago (delay expired)
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[messageID]
	delayedMsg.Created = time.Now().UTC().Add(-2 * time.Second)
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	_, inDelayed := q.messagesDelayed[messageID]
	_, inReady := q.messagesReady[messageID]
	require.False(t, inDelayed)
	require.True(t, inReady)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_futureDelayPeriod_keepsMessageInDelayed(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 10) // 10 second delay
	q.Push(msgState)
	messageID := msg.MessageID

	// Keep current creation time (delay still active)
	q.UpdateDelayedToReady()

	q.mu.Lock()
	_, inDelayed := q.messagesDelayed[messageID]
	_, inReady := q.messagesReady[messageID]
	require.True(t, inDelayed)
	require.False(t, inReady)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_zeroDelay_movesMessage(t *testing.T) {
	q := createTestQueue(t)

	// Push a message with delay
	msg := createTestMessage("test body")
	msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 1)
	q.Push(msgState)
	messageID := msg.MessageID

	// Set delay to zero (no delay)
	q.mu.Lock()
	delayedMsg := q.messagesDelayed[messageID]
	delayedMsg.Delay = None[time.Duration]()
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	_, inDelayed := q.messagesDelayed[messageID]
	_, inReady := q.messagesReady[messageID]
	require.False(t, inDelayed)
	require.True(t, inReady)
	q.mu.Unlock()
}

func Test_Queue_UpdateDelayedToReady_preservesStillDelayedMessagesInDelayedState(t *testing.T) {
	q := createTestQueue(t)

	// Push two messages with delays
	msg1 := createTestMessage("test body 1")
	msgState1, _ := q.NewMessageState(msg1, time.Now().UTC(), 1)
	msg2 := createTestMessage("test body 2")
	msgState2, _ := q.NewMessageState(msg2, time.Now().UTC(), 10)
	q.Push(msgState1, msgState2)

	// Make first message ready, keep second delayed
	q.mu.Lock()
	delayedMsg1 := q.messagesDelayed[msg1.MessageID]
	delayedMsg1.Created = time.Now().UTC().Add(-2 * time.Second)
	// msg2 keeps its delay active
	q.mu.Unlock()

	q.UpdateDelayedToReady()

	q.mu.Lock()
	// Second message should remain unchanged in delayed
	_, exists := q.messagesDelayed[msg2.MessageID]
	require.True(t, exists)
	q.mu.Unlock()
}
