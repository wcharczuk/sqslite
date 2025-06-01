package sqslite

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

type deletedQueueWorker struct {
	queues       *Queues
	tickInterval time.Duration
	clock        clockwork.Clock
}

func (q *deletedQueueWorker) TickInterval() time.Duration {
	if q.tickInterval > 0 {
		return q.tickInterval
	}
	return time.Second
}

func (q *deletedQueueWorker) Start(ctx context.Context) error {
	tick := q.clock.NewTicker(q.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.Chan():
			q.queues.PurgeDeletedQueues()
			continue
		}
	}
}
