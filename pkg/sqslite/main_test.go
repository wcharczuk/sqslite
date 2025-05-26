package sqslite

import (
	"testing"

	"sqslite/pkg/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test queue with default settings
func createTestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := NewQueueFromCreateQueueInput("http://sqslite.local", &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	require.Nil(t, err)
	t.Cleanup(func() { q.Close() })
	return q
}

func createTestMessage(body string) Message {
	return Message{
		MessageID: uuid.V4(),
		Body:      Some(body), // once told me
	}
}

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
