package sqslite

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

type delayWorker struct {
	queue        *Queue
	tickInterval time.Duration
	clock        clockwork.Clock
}

func (d *delayWorker) TickInterval() time.Duration {
	if d.tickInterval > 0 {
		return d.tickInterval
	}
	return time.Second
}

func (d *delayWorker) Start(ctx context.Context) error {
	tick := d.clock.NewTicker(d.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.Chan():
			d.queue.UpdateDelayedToReady()
			continue
		}
	}
}
