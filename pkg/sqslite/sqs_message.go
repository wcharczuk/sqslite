package sqslite

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// NewSqsMessage returns a new [SqsMessage] with a given set of required parameters.
func NewSqsMessage(priority float64, message Message, messageDeduplicationID, messageGroupID, sequenceNumber Optional[string]) *SqsMessage {
	sqsm := &SqsMessage{
		Created:                float64(time.Now().UTC().Unix()),
		Message:                message,
		ReceiptHandles:         make(Set[string]),
		Priority:               priority,
		MessageDeduplicationID: messageDeduplicationID.Value,
		MessageGroupID:         messageGroupID.Value,
		SequenceNumber:         sequenceNumber.Value,
	}
	if sqsm.Message.Attributes == nil {
		sqsm.Message.Attributes = make(MessageSystemAttributeMap)
	}
	sqsm.Message.Attributes["ApproximateReceiveCount"] = "0"
	if sqsm.MessageGroupID != "" {
		sqsm.Message.Attributes["MessageGroupId"] = sqsm.MessageGroupID
	}
	if sqsm.MessageDeduplicationID != "" {
		sqsm.Message.Attributes["MessageDeduplicationId"] = sqsm.MessageDeduplicationID
	}
	if sqsm.SequenceNumber != "" {
		sqsm.Message.Attributes["SequenceNumber"] = sqsm.SequenceNumber
	}
	return sqsm
}

type SqsMessage struct {
	Message                Message           `json:"message"`
	Created                float64           `json:"created"`
	VisibilityTimeout      int64             `json:"visibility_timeout"`
	ReceiveCount           int               `json:"receive_count"`
	DelaySeconds           Optional[int]     `json:"delay_second,omitempty"`
	ReceiptHandles         Set[string]       `json:"receipt_handles,omitempty"`
	LastReceived           Optional[float64] `json:"last_received,omitempty"`
	FirstReceived          Optional[float64] `json:"first_received,omitempty"`
	VisibilityDeadline     Optional[float64] `json:"visibility_deadline,omitempty"`
	Deleted                bool              `json:"deleted"`
	Priority               float64           `json:"priority"`
	MessageDeduplicationID string            `json:"message_deduplication_id"`
	MessageGroupID         string            `json:"message_group_id"`
	SequenceNumber         string            `json:"sequence_number"`
}

func (s *SqsMessage) IncrementApproximateReceiveCount() {
	if s.Message.Attributes == nil {
		s.Message.Attributes = make(MessageSystemAttributeMap)
	}
	if value, ok := s.Message.Attributes["ApproximateReceiveCount"]; ok {
		valueParsed, _ := strconv.Atoi(value)
		s.Message.Attributes["ApproximateReceiveCount"] = strconv.Itoa(valueParsed + 1)
	} else {
		s.Message.Attributes["ApproximateReceiveCount"] = "1"
	}
}

func (s *SqsMessage) SetLastReceived(timestamp float64) {
	s.LastReceived = Some(timestamp)
	updatedVisibilityDeadline := TimeFromFloat(timestamp).Add(time.Duration(s.VisibilityTimeout) * time.Second)
	s.VisibilityDeadline = Some(TimeToFloat(updatedVisibilityDeadline))
}

func (s *SqsMessage) UpdateVisibilityTimeout(timeout int64) {
	s.VisibilityTimeout = timeout
	s.VisibilityDeadline = Some(TimeToFloat(time.Now().UTC().Add(time.Duration(timeout) * time.Second)))
}

func (s *SqsMessage) IsVisible() bool {
	if s.VisibilityDeadline.IsZero() {
		return true
	}
	return time.Now().UTC().After(TimeFromFloat(s.VisibilityDeadline.Value))
}

func (s *SqsMessage) IsDelayed() bool {
	if s.DelaySeconds.IsZero() {
		return false
	}
	return time.Now().UTC().Before(
		TimeFromFloat(s.Created).Add(time.Duration(s.DelaySeconds.Value) * time.Second),
	)
}

func (s *SqsMessage) String() string {
	return fmt.Sprintf("SqsMessage(id=%s,group=%s)", s.Message.MessageID, s.MessageGroupID)
}

func TimeFromFloat(timestamp float64) time.Time {
	sec, dec := math.Modf(timestamp)
	return time.Unix(int64(sec), int64(dec*(1e9)))
}

func TimeToFloat(timestamp time.Time) float64 {
	unixNanos := timestamp.UnixNano()
	return float64(unixNanos) / 1e9
}

type Message struct {
	MessageID              string
	ReceiptHandle          Optional[string]
	MD5OfBody              Optional[string]
	Body                   Optional[string]
	Attributes             MessageSystemAttributeMap `json:"Attributes,omitempty"`
	MD5OfMessageAttributes Optional[string]
	MessageAttributes      MessageBodyAttributeMap `json:"MessageAttributes,omitempty"`
}

type MessageSystemAttributeMap map[string]string

type MessageBodyAttributeMap map[string]MessageBodyAttributeValue

type MessageBodyAttributeValue struct {
	StringValue     Optional[string]
	BinaryValue     Optional[[]byte]
	StringListValue Optional[[]string]
	BinaryListValue Optional[[][]byte]
	DataType        string
}
