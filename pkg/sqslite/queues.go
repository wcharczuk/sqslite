package sqslite

import (
	"context"
	"sync"
)

type Queues struct {
	queuesMu sync.Mutex
	queues   map[string]*Queue
}

func (q *Queues) CreateQueue(ctx context.Context, _ string) error {
	return nil
}

func (q *Queues) ListQueues(ctx context.Context, _ string) ([]*Queue, error) {
	return nil, nil
}

func (q *Queues) GetQueueURL(ctx context.Context, _ string) (string, error) {
	return "", nil
}

func (q *Queues) DeleteQueue(ctx context.Context, _ string) error {
	return nil
}
