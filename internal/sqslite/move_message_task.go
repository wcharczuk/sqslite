package sqslite

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/wcharczuk/sqslite/internal/uuid"
	"golang.org/x/time/rate"
)

// NewMessagesMoveTask returns a new move message task.
func NewMessagesMoveTask(clock clockwork.Clock, source, destination *Queue, maxNumberOfMessagesPerSecond int) *MessageMoveTask {
	mmt := &MessageMoveTask{
		AccountID:                    source.AccountID,
		TaskHandle:                   uuid.V4().String(),
		SourceQueue:                  source,
		DestinationQueue:             destination,
		MaxNumberOfMessagesPerSecond: maxNumberOfMessagesPerSecond,
		started:                      clock.Now(),
	}
	if maxNumberOfMessagesPerSecond > 0 {
		mmt.limiter = rate.NewLimiter(rate.Limit(maxNumberOfMessagesPerSecond), 0 /*burstBalance*/)
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
		return "CANCELING"
	case MessageMoveStatusCanceled:
		return "CANCELED"
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
				atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCanceled))
				return
			}
		}
		msg, ok := m.SourceQueue.PopMessageForMove()
		if !ok {
			atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCompleted))
			return
		}
		m.DestinationQueue.Push(msg)
		atomic.AddUint64(&m.stats.ApproximateNumberOfMessagesMoved, 1)
		atomic.StoreInt64(&m.stats.ApproximateNumberOfMessagesToMove, m.SourceQueue.Stats().NumMessagesReady)
	}
}

func (m *MessageMoveTask) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		atomic.StoreUint32(&m.status, uint32(MessageMoveStatusCanceling))
		m.cancel()
		m.cancel = nil
	}
}
