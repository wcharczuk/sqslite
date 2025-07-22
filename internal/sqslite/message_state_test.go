package sqslite

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func Test_NewMessageStateFromSendMessageInput(t *testing.T) {
	q := createTestQueue(t)

	msgInput := &sqs.SendMessageInput{
		QueueUrl:    aws.String("https://sqslite.local/sqslite-test-account/test-queue"),
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test-key": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test-value"),
			},
		},
		MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
			"AWSTraceHeader": {
				DataType:    aws.String("String"),
				StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"),
			},
		},
		DelaySeconds: 10,
	}

	msg := q.NewMessageStateFromSendMessageInput(msgInput)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.NotEmpty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody().Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes().Value)
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", msg.MD5OfMessageSystemAttributes().Value)
}

func Test_NewMessageStateFromSendMessageInput_noSystemAttributes(t *testing.T) {
	q := createTestQueue(t)

	msgInput := &sqs.SendMessageInput{
		QueueUrl:    aws.String("https://sqslite.local/sqslite-test-account/test-queue"),
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test-key": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test-value"),
			},
		},
		DelaySeconds: 10,
	}

	msg := q.NewMessageStateFromSendMessageInput(msgInput)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody().Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes().Value)
	require.False(t, msg.MD5OfMessageSystemAttributes().IsSet)
}

func Test_NewMessageStateFromSendMessageInput_noSystemAttributes_noAttributes(t *testing.T) {
	q := createTestQueue(t)
	msgInput := &sqs.SendMessageInput{
		QueueUrl:     aws.String("https://sqslite.local/sqslite-test-account/test-queue"),
		MessageBody:  aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		DelaySeconds: 10,
	}

	msg := q.NewMessageStateFromSendMessageInput(msgInput)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.Empty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody().Value)
	require.False(t, msg.MD5OfMessageAttributes().IsSet)
	require.False(t, msg.MD5OfMessageSystemAttributes().IsSet)
}

func Test_NewMessageStateFromSendMessageBatchEntry(t *testing.T) {
	q := createTestQueue(t)

	msgInput := types.SendMessageBatchRequestEntry{
		Id:          aws.String("test-message-id"),
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test-key": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test-value"),
			},
		},
		MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
			"AWSTraceHeader": {
				DataType:    aws.String("String"),
				StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"),
			},
		},
		DelaySeconds: 10,
	}
	msg := q.NewMessageStateFromSendMessageBatchEntry(msgInput)
	require.Equal(t, "test-message-id", msg.UserProvidedID.Value)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.NotEmpty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody().Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes().Value)
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", msg.MD5OfMessageSystemAttributes().Value)
}

func Test_NewMessageFromSendMessageBatchEntry_noSystemAttributes(t *testing.T) {
	q := createTestQueue(t)

	msgInput := types.SendMessageBatchRequestEntry{
		Id:          aws.String("test-message-id"),
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test-key": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test-value"),
			},
		},
		DelaySeconds: 10,
	}
	msg := q.NewMessageStateFromSendMessageBatchEntry(msgInput)
	require.Equal(t, "test-message-id", msg.UserProvidedID.Value)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody().Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes().Value)
	require.False(t, msg.MD5OfMessageSystemAttributes().IsSet)
}

func Test_NewMessageFromSendMessageBatchEntry_noSystemAttributes_noAttributes(t *testing.T) {
	q := createTestQueue(t)

	msgInput := types.SendMessageBatchRequestEntry{
		Id:           aws.String("test-message-id"),
		MessageBody:  aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		DelaySeconds: 10,
	}
	msg := q.NewMessageStateFromSendMessageBatchEntry(msgInput)
	require.Equal(t, "test-message-id", msg.UserProvidedID.Value)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.Empty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody().Value)
	require.False(t, msg.MD5OfMessageAttributes().IsSet)
	require.False(t, msg.MD5OfMessageSystemAttributes().IsSet)
}

func Test_MessageState_GetAttributes_all(t *testing.T) {
	testQueue := createTestQueue(t)
	m := &MessageState{
		MessageID:           uuid.V4(),
		OriginalSourceQueue: testQueue,
		SenderID:            Some(testAccountID),
		FirstReceived:       Some(time.Now().Add(-time.Minute)),
		ReceiveCount:        5,
		Sent:                time.Now().Add(-5 * time.Minute),
		LastReceived:        Some(time.Now().Add(-30 * time.Second)),
	}

	attributes := m.GetAttributes(types.MessageSystemAttributeNameAll)
	require.Len(t, attributes, 5)

	require.Equal(t, fmt.Sprint(time.Now().Add(-time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp)])
	require.Equal(t, testQueue.ARN, attributes[string(types.MessageSystemAttributeNameDeadLetterQueueSourceArn)])
	require.Equal(t, "5", attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	require.Equal(t, testAccountID, attributes[string(types.MessageSystemAttributeNameSenderId)])
	require.Equal(t, fmt.Sprint(time.Now().Add(-5*time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameSentTimestamp)])
}

func Test_MessageState_GetAttributes_deadLetterQueueSourceArn_unset(t *testing.T) {
	synctest.Run(func() {
		m := &MessageState{
			MessageID:     uuid.V4(),
			SenderID:      Some(testAccountID),
			FirstReceived: Some(time.Now().Add(-time.Minute)),
			ReceiveCount:  5,
			Sent:          time.Now().Add(-5 * time.Minute),
			LastReceived:  Some(time.Now().Add(-30 * time.Second)),
		}

		attributes := m.GetAttributes(types.MessageSystemAttributeNameAll)
		require.Len(t, attributes, 4)

		require.Equal(t, fmt.Sprint(time.Now().Add(-time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp)])
		require.Equal(t, "5", attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		require.Equal(t, testAccountID, attributes[string(types.MessageSystemAttributeNameSenderId)])
		require.Equal(t, fmt.Sprint(time.Now().Add(-5*time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameSentTimestamp)])
	})
}

func Test_MessageState_GetAttributes_subset(t *testing.T) {
	synctest.Run(func() {
		m := &MessageState{
			MessageID:     uuid.V4(),
			SenderID:      Some(testAccountID),
			FirstReceived: Some(time.Now().Add(-time.Minute)),
			ReceiveCount:  5,
			Sent:          time.Now().Add(-5 * time.Minute),
			LastReceived:  Some(time.Now().Add(-30 * time.Second)),
		}

		attributes := m.GetAttributes(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp, types.MessageSystemAttributeNameApproximateReceiveCount)
		require.Len(t, attributes, 2)

		require.Equal(t, fmt.Sprint(time.Now().Add(-time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp)])
		require.Equal(t, "5", attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	})
}

func Test_MessageState_GetAttributes_mixesAllAndSubset(t *testing.T) {
	synctest.Run(func() {
		testQueue := createTestQueue(t)
		m := &MessageState{
			MessageID:           uuid.V4(),
			OriginalSourceQueue: testQueue,
			SenderID:            Some(testAccountID),
			FirstReceived:       Some(time.Now().Add(-time.Minute)),
			ReceiveCount:        5,
			Sent:                time.Now().Add(-5 * time.Minute),
			LastReceived:        Some(time.Now().Add(-30 * time.Second)),
		}

		attributes := m.GetAttributes(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp, types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameAll)
		require.Len(t, attributes, 5)

		require.Equal(t, fmt.Sprint(time.Now().Add(-time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp)])
		require.Equal(t, testQueue.ARN, attributes[string(types.MessageSystemAttributeNameDeadLetterQueueSourceArn)])
		require.Equal(t, "5", attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		require.Equal(t, testAccountID, attributes[string(types.MessageSystemAttributeNameSenderId)])
		require.Equal(t, fmt.Sprint(time.Now().Add(-5*time.Minute).UnixMilli()), attributes[string(types.MessageSystemAttributeNameSentTimestamp)])
	})
}

func Test_MessageState_IsVisible(t *testing.T) {
	testQueue := createTestQueue(t)
	m := &MessageState{
		MessageID:           uuid.V4(),
		OriginalSourceQueue: testQueue,
		SenderID:            Some(testAccountID),
		FirstReceived:       Some(time.Now().Add(-time.Minute)),
		ReceiveCount:        5,
		Sent:                time.Now().Add(-5 * time.Minute),
		LastReceived:        Some(time.Now().Add(-30 * time.Second)),
		VisibilityTimeout:   10 * time.Second,
		VisibilityDeadline:  Some(time.Now().Add(-20 * time.Second)),
	}
	require.True(t, m.IsVisible(time.Now()))
	m.VisibilityDeadline = None[time.Time]()
	require.True(t, m.IsVisible(time.Now()))
	m.VisibilityDeadline = Some(time.Now().Add(10 * time.Second))
	require.False(t, m.IsVisible(time.Now()))
}

func Test_MessageState_IsDelayed(t *testing.T) {
	testQueue := createTestQueue(t)
	m := &MessageState{
		MessageID:           uuid.V4(),
		OriginalSourceQueue: testQueue,
		SenderID:            Some(testAccountID),
		FirstReceived:       Some(time.Now().Add(-time.Minute)),
		ReceiveCount:        5,
		Sent:                time.Now().Add(-5 * time.Second),
		LastReceived:        Some(time.Now().Add(-30 * time.Second)),
		VisibilityTimeout:   10 * time.Second,
		Delay:               Some(10 * time.Second),
	}
	require.True(t, m.IsDelayed(time.Now()))
	m.Delay = Some(2 * time.Second)
	require.False(t, m.IsDelayed(time.Now()))
	m.Delay = None[time.Duration]()
	require.False(t, m.IsDelayed(time.Now()))
}

func Test_MessageState_IsExpired(t *testing.T) {
	testQueue := createTestQueue(t)
	m := &MessageState{
		MessageID:              uuid.V4(),
		OriginalSourceQueue:    testQueue,
		SenderID:               Some(testAccountID),
		FirstReceived:          Some(time.Now().Add(-time.Minute)),
		ReceiveCount:           5,
		Sent:                   time.Now().Add(-6 * time.Minute),
		LastReceived:           Some(time.Now().Add(-30 * time.Second)),
		VisibilityTimeout:      10 * time.Second,
		MessageRetentionPeriod: 5 * time.Minute,
	}

	require.Equal(t, true, m.IsExpired(time.Now()))
	m.MessageRetentionPeriod = 10 * time.Minute
	require.Equal(t, false, m.IsExpired(time.Now()))
}
