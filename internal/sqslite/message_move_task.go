package sqslite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/wcharczuk/sqslite/internal/uuid"
	"golang.org/x/time/rate"
)

// NewMessagesMoveTask returns a new move message task.
func NewMessagesMoveTask(clock clockwork.Clock, source /*must not be nil*/, destination /*can be nil*/ *Queue, maxNumberOfMessagesPerSecond int) *MessageMoveTask {
	mmt := &MessageMoveTask{
		AccountID:                    source.AccountID,
		TaskHandle:                   uuid.V4().String(),
		SourceQueue:                  source,
		DestinationQueue:             destination,
		MaxNumberOfMessagesPerSecond: maxNumberOfMessagesPerSecond,
		started:                      clock.Now(),
	}
	if maxNumberOfMessagesPerSecond > 0 {
		mmt.limiter = rate.NewLimiter(rate.Limit(maxNumberOfMessagesPerSecond), 1 /*burstBalance has to be > 0*/)
	}
	return mmt
}

type MessageMoveStatus uint32

const (
	MessageMoveStatusUnknown   MessageMoveStatus = iota
	MessageMoveStatusRunning   MessageMoveStatus = iota
	MessageMoveStatusCompleted MessageMoveStatus = iota
	MessageMoveStatusFailed    MessageMoveStatus = iota
	MessageMoveStatusCanceling MessageMoveStatus = iota
	MessageMoveStatusCanceled  MessageMoveStatus = iota
)

func (m MessageMoveStatus) String() string {
	switch m {
	case MessageMoveStatusUnknown:
		return "UNKNOWN"
	case MessageMoveStatusRunning:
		return "RUNNING"
	case MessageMoveStatusCompleted:
		return "COMPLETED"
	case MessageMoveStatusFailed:
		return "FAILED"
	case MessageMoveStatusCanceling:
		//nolint:misspell
		return "CANCELLING"
	case MessageMoveStatusCanceled:
		//nolint:misspell
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

type MessageMoveTask struct {
	AccountID                    string
	TaskHandle                   string
	SourceQueue                  *Queue
	DestinationQueue             *Queue
	MaxNumberOfMessagesPerSecond int

	FailureReason string

	mu      sync.Mutex
	limiter *rate.Limiter
	cancel  func()

	started time.Time
	status  uint32
	stats   MessageMoveTaskStats
}

func (m *MessageMoveTask) Started() time.Time {
	return m.started
}

func (m *MessageMoveTask) Status() MessageMoveStatus {
	return MessageMoveStatus(atomic.LoadUint32(&m.status))
}

func (m *MessageMoveTask) Stats() (out MessageMoveTaskStats) {
	out.ApproximateNumberOfMessagesMoved = atomic.LoadUint64(&m.stats.ApproximateNumberOfMessagesMoved)
	out.ApproximateNumberOfMessagesToMove = atomic.LoadInt64(&m.stats.ApproximateNumberOfMessagesToMove)
	return
}

type MessageMoveTaskStats struct {
	ApproximateNumberOfMessagesMoved  uint64
	ApproximateNumberOfMessagesToMove int64
}

func (m *MessageMoveTask) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		return
	}
	var opCtx context.Context
	opCtx, m.cancel = context.WithCancel(ctx)
	go m.moveMessages(opCtx)
}

func (m *MessageMoveTask) moveMessages(ctx context.Context) {
	atomic.StoreUint32(&m.status, uint32(MessageMoveStatusRunning))
	for {
		select {
		case <-ctx.Done():
			atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCanceled))
			return
		default:
		}
		if m.limiter != nil {
			if err := m.limiter.Wait(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCanceled))
					return
				}
				m.markFailedByRateLimiterError(err)
				return
			}
		}
		msg, ok := m.SourceQueue.PopMessageForMove()
		if !ok {
			m.markCompleted()
			return
		}
		var destinationQueue *Queue
		if m.DestinationQueue != nil {
			destinationQueue = m.DestinationQueue
		} else {
			destinationQueue = msg.OriginalSourceQueue
		}
		if destinationQueue.IsDeleted() {
			m.markFailedByDestinationDeleted()
			return
		}
		if destinationQueue.RedriveAllowPolicy.IsSet {
			if !destinationQueue.RedriveAllowPolicy.Value.AllowSource(m.SourceQueue.ARN) {
				m.markFailedByDestinationDisallowed()
				return
			}
		}
		destinationQueue.Push(msg)
		atomic.AddUint64(&m.stats.ApproximateNumberOfMessagesMoved, 1)
		atomic.StoreInt64(&m.stats.ApproximateNumberOfMessagesToMove, m.SourceQueue.Stats().NumMessagesReady)
	}
}

func (m *MessageMoveTask) markFailedByRateLimiterError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status != uint32(MessageMoveStatusRunning) {
		return
	}
	atomic.StoreUint32(&m.status, uint32(MessageMoveStatusFailed))
	m.FailureReason = fmt.Sprintf("rate limiter error: %v", err)
	m.finishUnsafe()
}

func (m *MessageMoveTask) markFailedByDestinationDeleted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status != uint32(MessageMoveStatusRunning) {
		return
	}
	atomic.StoreUint32(&m.status, uint32(MessageMoveStatusFailed))
	m.FailureReason = fmt.Sprintf("destination queue %q has been deleted", m.DestinationQueue.ARN)
	m.finishUnsafe()
}

func (m *MessageMoveTask) markFailedByDestinationDisallowed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status != uint32(MessageMoveStatusRunning) {
		return
	}
	atomic.StoreUint32(&m.status, uint32(MessageMoveStatusFailed))
	m.FailureReason = fmt.Sprintf("destination queue %q disallows the source by redriveAllowPolicy", m.DestinationQueue.ARN)
	m.finishUnsafe()
}

func (m *MessageMoveTask) markCompleted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status != uint32(MessageMoveStatusRunning) {
		return
	}
	atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCompleted))
	m.finishUnsafe()
}

func (m *MessageMoveTask) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCanceling))
		m.finishUnsafe()
	}
}

func (m *MessageMoveTask) finishUnsafe() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
}
