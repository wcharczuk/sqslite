package sqslite

import (
	"github.com/wcharcuzk/sqslite/pkg/uuid"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func NewMessageFromSendMessageInput(input *sqs.SendMessageInput) Message {
	return Message{
		MessageID:              uuid.V4(),
		Body:                   SomePtr(input.MessageBody),
		MD5OfBody:              Some(md5sum(safeDeref(input.MessageBody))),
		MD5OfMessageAttributes: Some(md5sum(keysAndValues(input.MessageAttributes)...)),
	}
}

func NewMessageFromSendMessageBatchEntry(input types.SendMessageBatchRequestEntry) Message {
	return Message{
		ID:                     string(*input.Id),
		MessageID:              uuid.V4(),
		Body:                   SomePtr(input.MessageBody),
		MD5OfBody:              Some(md5sum(safeDeref(input.MessageBody))),
		MD5OfMessageAttributes: Some(md5sum(keysAndValues(input.MessageAttributes)...)),
	}
}

type Message struct {
	MessageID              uuid.UUID
	ID                     string
	ReceiptHandle          Optional[string]
	MD5OfBody              Optional[string]
	Body                   Optional[string]
	Attributes             map[string]string `json:"Attributes,omitempty"`
	MessageAttributes      map[string]string `json:"MessageAttributes,omitempty"`
	MD5OfMessageAttributes Optional[string]
}
