package sqslite

import (
	"context"
	"time"
)

type delayWorker struct {
	queue        *Queue
	tickInterval time.Duration
}

func (d *delayWorker) TickInterval() time.Duration {
	if d.tickInterval > 0 {
		return d.tickInterval
	}
	return time.Second
}

func (d *delayWorker) Start(ctx context.Context) error {
	tick := time.NewTicker(d.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			d.queue.UpdateDelayedToReady()
			continue
		}
	}
}
