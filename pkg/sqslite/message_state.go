package sqslite

import (
	"fmt"
	"strconv"
	"time"
)

type MessageState struct {
	Message            Message
	Created            time.Time
	VisibilityTimeout  time.Duration
	ReceiveCount       int
	Delay              Optional[time.Duration]
	ReceiptHandles     *SafeSet[string]
	LastReceived       Optional[time.Time]
	FirstReceived      Optional[time.Time]
	VisibilityDeadline Optional[time.Time]
	RetentionDeadline  time.Time
	SequenceNumber     uint64
}

func (s *MessageState) IncrementApproximateReceiveCount() {
	if s.Message.Attributes == nil {
		s.Message.Attributes = make(map[string]string)
	}
	if value, ok := s.Message.Attributes["ApproximateReceiveCount"]; ok {
		valueParsed, _ := strconv.Atoi(value)
		s.Message.Attributes["ApproximateReceiveCount"] = strconv.Itoa(valueParsed + 1)
	} else {
		s.Message.Attributes["ApproximateReceiveCount"] = "1"
	}
}

func (s *MessageState) SetLastReceived(timestamp time.Time) {
	s.LastReceived = Some(timestamp)
	s.VisibilityDeadline = Some(timestamp.Add(s.VisibilityTimeout))
}

func (s *MessageState) UpdateVisibilityTimeout(timeout time.Duration) {
	s.VisibilityTimeout = timeout
	if timeout == 0 {
		s.VisibilityDeadline = None[time.Time]()
	} else {
		s.VisibilityDeadline = Some(time.Now().UTC().Add(timeout))
	}
}

func (s *MessageState) IsVisible() bool {
	if s.VisibilityDeadline.IsZero() {
		return true
	}
	return time.Now().UTC().After(s.VisibilityDeadline.Value)
}

func (s *MessageState) IsDelayed() bool {
	if s.Delay.IsZero() {
		return false
	}
	return time.Now().UTC().Before(s.Created.Add(s.Delay.Value))
}

func (s *MessageState) IsExpired() bool {
	return time.Now().UTC().After(s.RetentionDeadline)
}

func (s *MessageState) String() string {
	return fmt.Sprintf("Message(id=%s)", s.Message.MessageID)
}
