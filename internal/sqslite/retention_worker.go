package sqslite

import (
	"context"
	"time"
)

type retentionWorker struct {
	queue        *Queue
	tickInterval time.Duration
}

func (r *retentionWorker) TickInterval() time.Duration {
	if r.tickInterval > 0 {
		return r.tickInterval
	}
	return time.Second
}

func (r *retentionWorker) Start(ctx context.Context) error {
	tick := time.NewTicker(r.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			r.queue.PurgeExpired()
			continue
		}
	}
}
