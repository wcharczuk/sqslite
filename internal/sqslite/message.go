package sqslite

import (
	"github.com/wcharczuk/sqslite/internal/uuid"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func NewMessageFromSendMessageInput(input *sqs.SendMessageInput) Message {
	var maMD5 Optional[string]
	if len(input.MessageAttributes) > 0 {
		maMD5 = Some(md5OfMessageAttributes(input.MessageAttributes))
	}
	var msaMD5 Optional[string]
	if len(input.MessageSystemAttributes) > 0 {
		msaMD5 = Some(md5OfMessageSystemAttributes(input.MessageSystemAttributes))
	}
	return Message{
		MessageID:                    uuid.V4(),
		Body:                         SomePtr(input.MessageBody),
		MessageAttributes:            input.MessageAttributes,
		MessageSystemAttributes:      input.MessageSystemAttributes,
		MD5OfBody:                    Some(md5sum(safeDeref(input.MessageBody))),
		MD5OfMessageAttributes:       maMD5,
		MD5OfMessageSystemAttributes: msaMD5,
	}
}

func NewMessageFromSendMessageBatchEntry(input types.SendMessageBatchRequestEntry) Message {
	var maMD5 Optional[string]
	if len(input.MessageAttributes) > 0 {
		maMD5 = Some(md5OfMessageAttributes(input.MessageAttributes))
	}
	var msaMD5 Optional[string]
	if len(input.MessageSystemAttributes) > 0 {
		msaMD5 = Some(md5OfMessageSystemAttributes(input.MessageSystemAttributes))
	}
	return Message{
		ID:                           string(*input.Id),
		MessageID:                    uuid.V4(),
		Body:                         SomePtr(input.MessageBody),
		MessageAttributes:            input.MessageAttributes,
		MessageSystemAttributes:      input.MessageSystemAttributes,
		MD5OfBody:                    Some(md5sum(safeDeref(input.MessageBody))),
		MD5OfMessageAttributes:       maMD5,
		MD5OfMessageSystemAttributes: msaMD5,
	}
}

type Message struct {
	MessageID                    uuid.UUID
	ID                           string
	ReceiptHandle                Optional[string]
	MD5OfBody                    Optional[string]
	Body                         Optional[string]
	Attributes                   map[string]string
	MessageAttributes            map[string]types.MessageAttributeValue
	MessageSystemAttributes      map[string]types.MessageSystemAttributeValue
	MD5OfMessageAttributes       Optional[string]
	MD5OfMessageSystemAttributes Optional[string]
}

const (
	AttributeTypeString = "String"
	AttributeTypeNumber = "Number"
	AttributeTypeBinary = "Binary"
)
