package sqslite

import (
	"context"
	"time"
)

type deletedQueueWorker struct {
	queues       *Queues
	tickInterval time.Duration
}

func (q *deletedQueueWorker) TickInterval() time.Duration {
	if q.tickInterval > 0 {
		return q.tickInterval
	}
	return time.Second
}

func (q *deletedQueueWorker) Start(ctx context.Context) error {
	tick := time.NewTicker(q.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			q.queues.PurgeDeletedQueues()
			continue
		}
	}
}
