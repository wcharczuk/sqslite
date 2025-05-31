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
		MD5OfMessageAttributes: Some(md5OfMessageAttributes(input.MessageAttributes)),
	}
}

type Message struct {
	MessageID               uuid.UUID
	ID                      string
	ReceiptHandle           Optional[string]
	MD5OfBody               Optional[string]
	Body                    Optional[string]
	Attributes              map[string]string                      `json:"Attributes,omitempty"`
	MessageAttributes       map[string]types.MessageAttributeValue `json:"MessageAttributes,omitempty"`
	MessageSystemAttributes map[string]types.MessageAttributeValue `json:"MessageSystemAttributes,omitempty"`
	MD5OfMessageAttributes  Optional[string]
}

const (
	AttributeTypeString = "String"
	AttributeTypeNumber = "Number"
	AttributeTypeBinary = "Binary"
)

// md5OfMessageAttributes performs a checksum on the message attribute values.
//
// This is required to function when we send messages to the queue.
func md5OfMessageAttributes(attributes map[string]types.MessageAttributeValue) string {
	keys := slices.Collect(maps.Keys(attributes))
	sort.Strings(keys)
	var buffer []byte
	for _, key := range keys {
		attribute := attributes[key]
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(key)))
		buffer = append(buffer, []byte(key)...)
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(*attribute.DataType)))
		buffer = append(buffer, []byte(*attribute.DataType)...)
		if attribute.StringValue != nil && *attribute.StringValue != "" {
			buffer = append(buffer, byte(1))
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(*attribute.StringValue)))
			buffer = append(buffer, []byte(*attribute.StringValue)...)
		} else {
			buffer = append(buffer, byte(2))
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(attribute.BinaryValue)))
			buffer = append(buffer, attribute.BinaryValue...)
		}
	}
	return hex.EncodeToString(md5.New().Sum(buffer))
}
