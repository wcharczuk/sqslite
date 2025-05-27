package sqslite

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// NewQueues returns a new queues storage.
func NewQueues() *Queues {
	return &Queues{
		queueURLs: make(map[QueueName]string),
		queueARNs: make(map[string]string),
		queues:    make(map[string]*Queue),
	}
}

// Queues holds all the queue
type Queues struct {
	queuesMu  sync.Mutex
	queueURLs map[QueueName]string
	queueARNs map[string]string
	queues    map[string]*Queue
}

// QueueName is a pair of AccountID and QueueName
// because queue names are only unique within an account.
type QueueName struct {
	AccountID string
	QueueName string
}

func (q *Queues) Close() {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	for _, q := range q.queues {
		q.Close()
	}
}

func (q *Queues) AddQueue(ctx context.Context, queue *Queue) (err *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()

	// note(wc): we do a lot of checks after the lock acquisition
	// to prevent race conditions with concurrent creates
	if _, ok := q.queueURLs[QueueName{AccountID: queue.AccountID, QueueName: queue.Name}]; ok {
		err = ErrorInvalidParameterValue(fmt.Sprintf("QueueName: queue already exists with name: %s", queue.Name))
		return
	}
	if queue.RedrivePolicy.IsSet {
		dlqURL, ok := q.queueARNs[queue.RedrivePolicy.Value.DeadLetterTargetArn]
		if !ok {
			err = ErrorInvalidParameterValue(fmt.Sprintf("DeadLetterTargetArn: queue with arn not found: %s", queue.RedrivePolicy.Value.DeadLetterTargetArn))
			return
		}
		dlq, ok := q.queues[dlqURL]
		if !ok {
			err = ErrorInternalServer(fmt.Sprintf("dlq not found with URL: %s", dlqURL))
			return
		}
		queue.dlqTarget = dlq
	}
	q.queueURLs[QueueName{AccountID: queue.AccountID, QueueName: queue.Name}] = queue.URL
	q.queueARNs[queue.ARN] = queue.URL
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

func (q *Queues) ListQueues(ctx context.Context) ([]*Queue, error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	authz, ok := GetContextAuthorization(ctx)
	if !ok {
		return nil, ErrorUnauthorized()
	}
	var output []*Queue
	for _, queue := range q.queues {
		if queue.AccountID == authz.AccountID {
			output = append(output, queue)
		}
	}
	return output, nil
}

func (q *Queues) GetQueueURL(ctx context.Context, queueName string) (queueURL string, ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	var authz Authorization
	authz, ok = GetContextAuthorization(ctx)
	if !ok {
		slog.Debug("queues; get queue url; context authorization not found")
		return
	}
	queueURL, ok = q.queueURLs[QueueName{AccountID: authz.AccountID, QueueName: queueName}]
	return
}

func (q *Queues) GetQueue(ctx context.Context, queueURL string) (queue *Queue, err *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()

	authz, ok := GetContextAuthorization(ctx)
	if !ok {
		err = ErrorUnauthorized()
		return
	}
	queue, ok = q.queues[queueURL]
	if !ok {
		err = ErrorInvalidParameterValue("QueueURL")
		return
	}
	if queue.AccountID != authz.AccountID {
		queue = nil
		err = ErrorInvalidParameterValue("QueueURL")
	}
	return
}

func (q *Queues) DeleteQueue(ctx context.Context, queueURL string) (ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()

	var authz Authorization
	authz, ok = GetContextAuthorization(ctx)
	if !ok {
		return
	}
	var queue *Queue
	queue, ok = q.queues[queueURL]
	if !ok {
		return
	}
	if queue.AccountID != authz.AccountID {
		ok = false
		return
	}
	queue.Close()
	delete(q.queueURLs, QueueName{AccountID: queue.AccountID, QueueName: queue.Name})
	delete(q.queues, queueURL)
	return
}
