package sqslite

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"slices"
	"sort"

	"github.com/wcharczuk/sqslite/internal/uuid"

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
	MessageID               uuid.UUID
	ID                      string
	ReceiptHandle           Optional[string]
	MD5OfBody               Optional[string]
	Body                    Optional[string]
	Attributes              map[string]string                `json:"Attributes,omitempty"`
	MessageAttributes       map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	MessageSystemAttributes map[string]MessageAttributeValue `json:"MessageSystemAttributes,omitempty"`
	MD5OfMessageAttributes  Optional[string]
}

type MessageAttributeValue struct {
	Type        string
	StringValue string
	BinaryValue []byte
}

const (
	AttributeTypeString = "String"
	AttributeTypeNumber = "Number"
	AttributeTypeBinary = "Binary"
)

func md5OfMessageAttributes(attributes map[string]MessageAttributeValue) string {
	names := slices.Collect(maps.Keys(attributes))
	sort.Strings(names)

	var buffer []byte
	for _, name := range names {
		attribute := attributes[name]

		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(name)))
		buffer = append(buffer, []byte(name)...)

		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(attribute.Type)))
		buffer = append(buffer, []byte(attribute.Type)...)

		if attribute.StringValue != "" {
			buffer = append(buffer, byte(1))
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(attribute.StringValue)))
			buffer = append(buffer, []byte(attribute.StringValue)...)
		} else {
			buffer = append(buffer, byte(2))
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(attribute.BinaryValue)))
			buffer = append(buffer, attribute.BinaryValue...)
		}
	}
	return hex.EncodeToString(md5.New().Sum(buffer))
}
