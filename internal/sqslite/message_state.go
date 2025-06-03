package sqslite

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func (q *Queue) NewEmptyMessageState() *MessageState {
	return &MessageState{
		MessageID:              uuid.V4(),
		Sent:                   q.clock.Now(),
		MessageRetentionPeriod: q.MessageRetentionPeriod,
		ReceiptHandles:         NewSafeSet[string](),
		OriginalSourceQueue:    q,
	}
}

func (q *Queue) NewMessageStateFromSendMessageInput(input *sqs.SendMessageInput) *MessageState {
	output := q.NewEmptyMessageState()
	output.Body = FromPtr(input.MessageBody)
	output.MessageRetentionPeriod = q.MessageRetentionPeriod
	output.MessageAttributes = input.MessageAttributes
	output.MessageSystemAttributes = input.MessageSystemAttributes
	if input.DelaySeconds > 0 {
		output.Delay = Some(time.Duration(input.DelaySeconds) * time.Second)
	}
	return output
}

func (q *Queue) NewMessageStateFromSendMessageBatchEntry(input types.SendMessageBatchRequestEntry) *MessageState {
	output := q.NewEmptyMessageState()
	output.UserProvidedID = FromPtr(input.Id)
	output.Body = FromPtr(input.MessageBody)
	output.MessageRetentionPeriod = q.MessageRetentionPeriod
	output.MessageAttributes = input.MessageAttributes
	output.MessageSystemAttributes = input.MessageSystemAttributes
	if input.DelaySeconds > 0 {
		output.Delay = Some(time.Duration(input.DelaySeconds) * time.Second)
	}
	return output
}

type MessageState struct {
	MessageID               uuid.UUID
	Body                    Optional[string]
	MessageAttributes       map[string]types.MessageAttributeValue
	MessageSystemAttributes map[string]types.MessageSystemAttributeValue
	Delay                   Optional[time.Duration]
	MessageRetentionPeriod  time.Duration
	VisibilityTimeout       time.Duration
	ReceiptHandles          *SafeSet[string]
	OriginalSourceQueue     *Queue
	UserProvidedID          Optional[string] /*maps to the Id field*/

	/* these aren't mutated after the initial send */
	SenderID Optional[string]
	Sent     time.Time

	/* these require the parent queue mutex */
	FirstReceived      Optional[time.Time]
	LastReceived       Optional[time.Time]
	VisibilityDeadline Optional[time.Time]
	ReceiveCount       uint32
}

func (s *MessageState) IncrementApproximateReceiveCount() uint32 {
	return atomic.AddUint32(&s.ReceiveCount, 1)
}

func (s *MessageState) SetLastReceived(timestamp time.Time) {
	s.LastReceived = Some(timestamp)
	s.VisibilityDeadline = Some(timestamp.Add(s.VisibilityTimeout))
}

func (s *MessageState) MaybeSetFirstReceived(timestamp time.Time) {
	if s.FirstReceived.IsZero() {
		s.FirstReceived = Some(timestamp)
	}
}

func (s *MessageState) UpdateVisibilityTimeout(timeout time.Duration, timestamp time.Time) {
	s.VisibilityTimeout = timeout
	if timeout == 0 {
		s.VisibilityDeadline = None[time.Time]()
	} else {
		s.VisibilityDeadline = Some(timestamp.Add(timeout))
	}
}

func (s *MessageState) IsVisible(timestamp time.Time) bool {
	if s.VisibilityDeadline.IsZero() {
		return true
	}
	return timestamp.After(s.VisibilityDeadline.Value)
}

func (s *MessageState) IsDelayed(timestamp time.Time) bool {
	if s.Delay.IsZero() {
		return false
	}
	return timestamp.Before(s.Sent.Add(s.Delay.Value))
}

func (s *MessageState) IsExpired(timestamp time.Time) bool {
	return timestamp.After(s.Sent.Add(s.MessageRetentionPeriod))
}

func (s *MessageState) String() string {
	return fmt.Sprintf("Message(id=%s)", s.MessageID)
}

func (s *MessageState) GetSystemAttributes(attributeNames []types.MessageSystemAttributeName) map[string]string {
	return make(map[string]string)
}

func (s *MessageState) ForReceiveMessageOutput(input *sqs.ReceiveMessageInput, receiptHandle ReceiptHandle) types.Message {
	return types.Message{
		Body:                   s.Body.Ptr(),
		MD5OfBody:              s.MD5OfBody().Ptr(),
		MessageId:              aws.String(s.MessageID.String()),
		MessageAttributes:      s.MessageAttributes,
		Attributes:             s.GetSystemAttributes(input.MessageSystemAttributeNames),
		ReceiptHandle:          aws.String(receiptHandle.String()),
		MD5OfMessageAttributes: s.MD5OfMessageAttributes().Ptr(),
	}
}

func (s *MessageState) ForSendMessageOutput() *sqs.SendMessageOutput {
	return &sqs.SendMessageOutput{
		MessageId:                    aws.String(s.MessageID.String()),
		MD5OfMessageBody:             s.MD5OfBody().Ptr(),
		MD5OfMessageAttributes:       s.MD5OfMessageAttributes().Ptr(),
		MD5OfMessageSystemAttributes: s.MD5OfMessageSystemAttributes().Ptr(),
		/* SequenceNumber: only for fifo queues!*/
	}
}

func (s *MessageState) ForSendMessageBatchResultEntry() types.SendMessageBatchResultEntry {
	return types.SendMessageBatchResultEntry{
		Id:                           s.UserProvidedID.Ptr(),
		MessageId:                    aws.String(s.MessageID.String()),
		MD5OfMessageBody:             s.MD5OfBody().Ptr(),
		MD5OfMessageAttributes:       s.MD5OfMessageAttributes().Ptr(),
		MD5OfMessageSystemAttributes: s.MD5OfMessageSystemAttributes().Ptr(),
		/* SequenceNumber: only for fifo queues!*/
	}
}

func (s *MessageState) MD5OfBody() Optional[string] {
	if s.Body.IsSet {
		return Some(md5sum(s.Body.Value))
	}
	return None[string]()
}

func (s *MessageState) MD5OfMessageAttributes() Optional[string] {
	if len(s.MessageAttributes) > 0 {
		return Some(md5OfMessageAttributes(s.MessageAttributes))
	}
	return None[string]()
}

func (s *MessageState) MD5OfMessageSystemAttributes() Optional[string] {
	if len(s.MessageSystemAttributes) > 0 {
		return Some(md5OfMessageSystemAttributes(s.MessageSystemAttributes))
	}
	return None[string]()
}
