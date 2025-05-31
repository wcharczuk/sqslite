package sqslite

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

type retentionWorker struct {
	queue        *Queue
	tickInterval time.Duration
	clock        clockwork.Clock
}

func (r *retentionWorker) TickInterval() time.Duration {
	if r.tickInterval > 0 {
		return r.tickInterval
	}
	return time.Second
}

func (r *retentionWorker) Start(ctx context.Context) error {
	tick := r.clock.NewTicker(r.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.Chan():
			r.queue.PurgeExpired()
			continue
		}
	}
}
