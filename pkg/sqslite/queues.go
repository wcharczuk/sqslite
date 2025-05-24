package sqslite

import (
	"context"
	"fmt"
	"sync"
)

// NewQueues returns a new queues storage.
func NewQueues() *Queues {
	return &Queues{
		queueURLs: make(map[string]string),
		queues:    make(map[string]*Queue),
	}
}

// Queues holds all the queue
type Queues struct {
	queuesMu  sync.Mutex
	queueURLs map[string]string
	queues    map[string]*Queue
}

func (q *Queues) Close() {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	for _, q := range q.queues {
		q.Close()
	}
}

func (q *Queues) CreateQueue(ctx context.Context, queue *Queue) (err *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	if _, ok := q.queueURLs[queue.Name]; ok {
		err = ErrorInvalidParameterValue(fmt.Sprintf("create queue; queue already exists with name: %s", queue.Name))
		return
	}
	q.queueURLs[queue.Name] = queue.URL
	q.queues[queue.URL] = queue
	return
}

func (q *Queues) PurgeQueue(ctx context.Context, queueURL string) (ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queue, ok := q.queues[queueURL]
	if !ok {
		return
	}
	queue.Purge()
	return
}

func (q *Queues) ListQueues(ctx context.Context, _ string) ([]*Queue, error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	var output []*Queue
	for _, queue := range q.queues {
		output = append(output, queue)
	}
	return output, nil
}

func (q *Queues) GetQueueURL(ctx context.Context, queueName string) (queueURL string, ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queueURL, ok = q.queueURLs[queueName]
	return
}

func (q *Queues) GetQueue(ctx context.Context, queueURL string) (queue *Queue, ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queue, ok = q.queues[queueURL]
	return
}

func (q *Queues) DeleteQueue(ctx context.Context, queueURL string) (ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	var queue *Queue
	queue, ok = q.queues[queueURL]
	if !ok {
		return
	}
	delete(q.queueURLs, queue.Name)
	delete(q.queues, queueURL)
	return
}
