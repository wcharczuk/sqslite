package sqslite

import (
	"maps"
	"slices"
	"strings"
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
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
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
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test!!!queue"),
	})
	require.NotNil(t, err)
}

func Test_validateMessageBody(t *testing.T) {
	err := validateMessageBody(aws.String(`{"message":0}`), 256*1024)
	require.Nil(t, err)

	err = validateMessageBody(aws.String(strings.Repeat("a", 512)), 256)
	require.NotNil(t, err)

	err = validateMessageBody(aws.String(""), 256)
	require.Nil(t, err)
}

func Test_Queue_NewMessageStateFromSendMessageInput(t *testing.T) {
	q, _ := NewQueueFromCreateQueueInput(clockwork.NewFakeClock(), Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()
	msg := q.NewMessageStateFromSendMessageInput(&sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(`{"messageIndex":0}`),
	})
	require.Equal(t, "552cc6a91af25b6aef1e5d1b5e5f54a9", msg.MD5OfBody().Value)
}

func Test_Queue_Receive_basic(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())

	pushTestMessages(q, 100)
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	require.GreaterOrEqual(t, len(received), 1)
	require.LessOrEqual(t, len(received), 10)
	require.Equal(t, "1", received[0].Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)], received[0].Attributes)
	require.Len(t, q.messagesInflight, len(received))
}

func Test_Queue_Receive_single(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())

	// Push 5 messages to the queue
	pushTestMessages(q, 10)

	// Request only 3 messages
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 1,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})

	// Assert that we get at leaset 1 messages
	require.GreaterOrEqual(t, len(received), 1)

	// Assert that message attributes are added if we ask for them
	require.Equal(t, "1", received[0].Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)], received[0].Attributes)
	require.Len(t, q.messagesInflight, 1)
	firstKey := slices.Collect(maps.Keys(q.messagesInflight))[0]
	msgState := q.messagesInflight[firstKey]
	require.EqualValues(t, msgState.MessageID.String(), safeDeref(received[0].MessageId))
	require.EqualValues(t, 1, msgState.ReceiveCount)
	require.EqualValues(t, 1, msgState.ReceiptHandles.Len())
	require.EqualValues(t, true, msgState.ReceiptHandles.Has(safeDeref(received[0].ReceiptHandle)))

	require.EqualValues(t, 10, q.Stats().NumMessages)
	require.EqualValues(t, 9, q.Stats().NumMessagesReady)
	require.EqualValues(t, 1, q.Stats().NumMessagesInflight)
}

func Test_Queue_Receive_returnsMessagesInMultiplePasses(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())

	pushTestMessages(q, 5)

	remaining := 5
	for range 5 {
		received := q.Receive(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   10,
		})
		require.GreaterOrEqual(t, len(received), 1, remaining)
		remaining = remaining - len(received)
		if remaining == 0 {
			break
		}
	}
	require.EqualValues(t, 0, remaining)
}

func Test_Queue_Receive_respectsMaximumMessagesInFlight(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	q.MaximumMessagesInflight = 10

	pushTestMessages(q, 20)

	remaining := 10
	for range 20 {
		received := q.Receive(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   10,
		})
		remaining = remaining - len(received)
	}
	require.EqualValues(t, 0, remaining)
	require.EqualValues(t, 10, len(q.messagesInflight))
}

func Test_Queue_Receive_usesProvidedVisibilityTimeout(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	q.VisibilityTimeout = 30 * time.Second // Default queue timeout

	pushTestMessages(q, 1)
	customTimeout := 60 * time.Second

	// Receive with custom visibility timeout
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   60,
	})
	require.Len(t, received, 1)

	// Check that the message in inflight state has the custom timeout
	firstKey := slices.Collect(maps.Keys(q.messagesInflight))[0]
	msgState := q.messagesInflight[firstKey]
	require.Equal(t, customTimeout, msgState.VisibilityTimeout)
	require.Equal(t, true, msgState.FirstReceived.IsSet)
	require.Equal(t, testAccountID, msgState.SenderID.Value)
}

func Test_Queue_Receive_usesDefaultVisibilityTimeoutWhenZero(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	q.VisibilityTimeout = 30 * time.Second // Default queue timeout

	pushTestMessages(q, 1)

	// Receive with custom visibility timeout
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   0,
	})
	require.Len(t, received, 1)

	firstKey := slices.Collect(maps.Keys(q.messagesInflight))[0]
	msgState := q.messagesInflight[firstKey]
	require.Equal(t, 30*time.Second, msgState.VisibilityTimeout)
}

func Test_Queue_Push_singleMessageWithoutDelay_addsToReadyQueue(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	initialStats := q.Stats()

	msg := createTestSendMessageInput("test body")
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	require.NotNil(t, msgState.OriginalSourceQueue)
	require.EqualValues(t, q.URL, msgState.OriginalSourceQueue.URL)

	q.Push(msgState)
	updatedStats := q.Stats()
	require.Len(t, q.messagesReady, 1)

	_, exists := q.messagesReady[msgState.MessageID]
	require.True(t, exists)

	require.Equal(t, initialStats.TotalMessagesSent+1, updatedStats.TotalMessagesSent)
	require.Equal(t, initialStats.NumMessages+1, updatedStats.NumMessages)
	require.Equal(t, initialStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func Test_Queue_Push_singleMessageWithDelay_addsToDelayedQueue(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	initialStats := q.Stats()

	msg := createTestSendMessageInput("test body")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Len(t, q.messagesReady, 0)
	require.Len(t, q.messagesDelayed, 1)
	require.Equal(t, initialStats.NumMessagesDelayed+1, updatedStats.NumMessagesDelayed)
}

func Test_Queue_Push_multipleMessages_handlesAllMessages(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())

	msg1 := createTestSendMessageInput("test body 1")
	msgState1 := q.NewMessageStateFromSendMessageInput(msg1)
	msg2 := createTestSendMessageInput("test body 2")
	msgState2 := q.NewMessageStateFromSendMessageInput(msg2)
	q.Push(msgState1, msgState2)

	require.Len(t, q.messagesReady, 2)
}

func Test_Queue_Push_queueHasDefaultDelayMessageHasNone_appliesQueueDelay(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	q.Delay = Some(5 * time.Second)

	msg := createTestSendMessageInput("test body")
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	require.Len(t, q.messagesReady, 0)
	require.Len(t, q.messagesDelayed, 1)
}

func Test_Queue_Push_queueHasDefaultDelayMessageHasDelay_keepsMessageDelay(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	q.Delay = Some(5 * time.Second)

	msg := createTestSendMessageInput("test body")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	require.Equal(t, 10*time.Second, msgState.Delay.Value)
}

func Test_Queue_Push_noMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())
	initialStats := q.Stats()

	q.Push() // Call with no arguments

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

func Test_Queue_Push_mixedDelayedAndReadyMessages_handlesCorrectly(t *testing.T) {
	q := createTestQueue(t, clockwork.NewFakeClock())

	readyMsg := createTestSendMessageInput("ready")
	readyMsgState := q.NewMessageStateFromSendMessageInput(readyMsg)
	delayedMsg := createTestSendMessageInput("delayed")
	delayedMsg.DelaySeconds = 10
	delayedMsgState := q.NewMessageStateFromSendMessageInput(delayedMsg)

	q.Push(readyMsgState, delayedMsgState)

	require.Len(t, q.messagesReady, 1)
	require.Len(t, q.messagesDelayed, 1)
}
