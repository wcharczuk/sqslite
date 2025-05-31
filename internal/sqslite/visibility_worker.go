package sqslite

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

type visibilityWorker struct {
	queue        *Queue
	tickInterval time.Duration
	clock        clockwork.Clock
}

func (v *visibilityWorker) TickInterval() time.Duration {
	if v.tickInterval > 0 {
		return v.tickInterval
	}
	return time.Second
}

func (v *visibilityWorker) Start(ctx context.Context) error {
	tick := v.clock.NewTicker(v.TickInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.Chan():
			v.queue.UpdateInflightToReady()
			continue
		}
	}
}
