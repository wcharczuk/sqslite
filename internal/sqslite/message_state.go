package sqslite

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Message attributes
const (
	MessageAttributeApproximateReceiveCount = "ApproximateReceiveCount"
	MessageAttributeMessageGroupID          = "MessageGroupId"
	MessageAttributeMessageDeduplicationId  = "MessageDeduplicationId"
)

type MessageState struct {
	Message                Message
	Created                time.Time
	Delay                  Optional[time.Duration]
	MessageRetentionPeriod time.Duration
	VisibilityTimeout      time.Duration
	ReceiptHandles         *SafeSet[string]
	OriginalSourceQueue    *Queue

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
	return timestamp.Before(s.Created.Add(s.Delay.Value))
}

func (s *MessageState) IsExpired(timestamp time.Time) bool {
	return timestamp.After(s.Created.Add(s.MessageRetentionPeriod))
}

func (s *MessageState) String() string {
	return fmt.Sprintf("Message(id=%s)", s.Message.MessageID)
}
