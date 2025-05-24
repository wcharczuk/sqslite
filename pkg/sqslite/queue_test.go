package sqslite

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/stretchr/testify/require"
)

func Test_Queue_NewMessageStateFromInput(t *testing.T) {
	q, _ := NewQueueFromCreateQueueInput(&sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	msg := NewMessageFromSendMessageInput(&sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(`{"messageIndex":0}`),
	})
	require.Equal(t, "552cc6a91af25b6aef1e5d1b5e5f54a9", msg.MD5OfBody.Value)
}
