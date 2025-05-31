package sqslite

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

func createTestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := NewQueueFromCreateQueueInput(clockwork.NewFakeClock(), Authorization{
		Region:    "us-west-2",
		AccountID: "test-account",
		Host:      "sqslite.local",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	t.Cleanup(q.Close)
	require.Nil(t, err)
	return q
}

func createTestQueueWithNameAndURL(name, url string) *Queue {
	q, _ := NewQueueFromCreateQueueInput(clockwork.NewFakeClock(), Authorization{
		Region:    "us-west-2",
		AccountID: "test-account",
		Host:      "sqslite.local",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	q.URL = url // Override URL for testing
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
		msgState, _ := q.NewMessageState(msg, time.Now().UTC(), 0)
		messages = append(messages, msgState)
	}
	q.Push(messages...)
	return messages
}
