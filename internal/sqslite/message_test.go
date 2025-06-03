package sqslite

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func Test_NewMessageFromSendMessageInput(t *testing.T) {
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

	msg := NewMessageFromSendMessageInput(msgInput)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.NotEmpty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody.Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes.Value)
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", msg.MD5OfMessageSystemAttributes.Value)
}

func Test_NewMessageFromSendMessageInput_noSystemAttributes(t *testing.T) {
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

	msg := NewMessageFromSendMessageInput(msgInput)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody.Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes.Value)
	require.False(t, msg.MD5OfMessageSystemAttributes.IsSet)
}

func Test_NewMessageFromSendMessageInput_noSystemAttributes_noAttributes(t *testing.T) {
	msgInput := &sqs.SendMessageInput{
		QueueUrl:     aws.String("https://sqslite.local/sqslite-test-account/test-queue"),
		MessageBody:  aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		DelaySeconds: 10,
	}

	msg := NewMessageFromSendMessageInput(msgInput)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.Empty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody.Value)
	require.False(t, msg.MD5OfMessageAttributes.IsSet)
	require.False(t, msg.MD5OfMessageSystemAttributes.IsSet)
}

func Test_NewMessageFromSendMessageBatchEntry(t *testing.T) {
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
	msg := NewMessageFromSendMessageBatchEntry(msgInput)
	require.Equal(t, "test-message-id", msg.ID)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.NotEmpty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody.Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes.Value)
	require.EqualValues(t, "5ae4d5d7636402d80f4eb6d213245a88", msg.MD5OfMessageSystemAttributes.Value)
}

func Test_NewMessageFromSendMessageBatchEntry_noSystemAttributes(t *testing.T) {
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
	msg := NewMessageFromSendMessageBatchEntry(msgInput)
	require.Equal(t, "test-message-id", msg.ID)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.NotEmpty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody.Value)
	require.EqualValues(t, "befa18540a897f7d022bf07057754a03", msg.MD5OfMessageAttributes.Value)
	require.False(t, msg.MD5OfMessageSystemAttributes.IsSet)
}

func Test_NewMessageFromSendMessageBatchEntry_noSystemAttributes_noAttributes(t *testing.T) {
	msgInput := types.SendMessageBatchRequestEntry{
		Id:           aws.String("test-message-id"),
		MessageBody:  aws.String(fmt.Sprintf(`{"message_index":%d}`, 1)),
		DelaySeconds: 10,
	}
	msg := NewMessageFromSendMessageBatchEntry(msgInput)
	require.Equal(t, "test-message-id", msg.ID)
	require.Equal(t, `{"message_index":1}`, msg.Body.Value)
	require.Empty(t, msg.MessageAttributes)
	require.Empty(t, msg.MessageSystemAttributes)
	require.EqualValues(t, "4504dd781f625d681c31cda87e260702", msg.MD5OfBody.Value)
	require.False(t, msg.MD5OfMessageAttributes.IsSet)
	require.False(t, msg.MD5OfMessageSystemAttributes.IsSet)
}
