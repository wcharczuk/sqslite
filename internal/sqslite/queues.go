package sqslite

import (
	"context"
	"iter"
	"reflect"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// NewQueues returns a new queues storage.
func NewQueues(clock clockwork.Clock) *Queues {
	return &Queues{
		queueURLs:                   make(map[string]string),
		queueARNs:                   make(map[string]string),
		queues:                      make(map[string]*Queue),
		moveMessageTasks:            make(map[string]*MessageMoveTask),
		moveMessageTasksBySourceArn: make(map[string]*OrderedSet[string]),
		clock:                       clock,
	}
}

// Queues holds all the queue
type Queues struct {
	lifecycleMu                 sync.Mutex
	queuesMu                    sync.Mutex
	queueURLs                   map[string]string
	queueARNs                   map[string]string
	queues                      map[string]*Queue
	moveMessageTasks            map[string]*MessageMoveTask
	moveMessageTasksBySourceArn map[string]*OrderedSet[string]

	deletedQueueWorker       *deletedQueueWorker
	deletedQueueWorkerCancel func()

	clock clockwork.Clock
}

func (q *Queues) Start() {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	var deletedQueueWorkerCtx context.Context
	deletedQueueWorkerCtx, q.deletedQueueWorkerCancel = context.WithCancel(context.Background())
	q.deletedQueueWorker = &deletedQueueWorker{queues: q, clock: q.clock}
	go q.deletedQueueWorker.Start(deletedQueueWorkerCtx)
}

func (q *Queues) Close() {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()
	for _, q := range q.queues {
		q.Close()
	}
	if q.deletedQueueWorkerCancel != nil {
		q.deletedQueueWorkerCancel()
		q.deletedQueueWorkerCancel = nil
		q.deletedQueueWorker = nil
	}
}

func (q *Queues) AddQueue(queue *Queue) (err *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	if existingQueueURL, ok := q.queueURLs[queue.Name]; ok {
		existingQueue, ok := q.queues[existingQueueURL]
		if !ok {
			err = ErrorInternalServer().WithMessage("queue url mapping exists for queue name, but queue itself is missing by queue url")
			return
		}
		if reflect.DeepEqual(queue.Attributes, existingQueue.Attributes) {
			// this is ok!
			return
		}
		err = ErrorQueueNameAlreadyExists()
		return
	}
	if queue.RedrivePolicy.IsSet {
		dlqURL, ok := q.queueARNs[queue.RedrivePolicy.Value.DeadLetterTargetArn]
		if !ok {
			err = ErrorInvalidAttributeValue().WithMessagef("DeadLetterTargetArn is invalid; queue with arn not found: %s", queue.RedrivePolicy.Value.DeadLetterTargetArn)
			return
		}
		dlq, ok := q.queues[dlqURL]
		if !ok {
			err = ErrorInternalServer().WithMessagef("Queue not not found with URL: %s", dlqURL)
			return
		}
		queue.dlqTarget = dlq
		dlq.AddDLQSources(queue)
	}
	q.queueURLs[queue.Name] = queue.URL
	q.queueARNs[queue.ARN] = queue.URL
	q.queues[queue.URL] = queue
	return
}

func (q *Queues) PurgeQueue(queueURL string) (ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queue, ok := q.getQueueUnsafe(queueURL)
	if !ok {
		return
	}
	queue.Purge()
	return
}

func (q *Queues) EachQueue() iter.Seq[*Queue] {
	return func(yield func(*Queue) bool) {
		q.queuesMu.Lock()
		defer q.queuesMu.Unlock()
		for _, queue := range q.queues {
			if queue.deleted.IsZero() {
				continue
			}
			if !yield(queue) {
				return
			}
		}
	}
}

func (q *Queues) StartMoveMessageTask(clock clockwork.Clock, sourceArn, destinationArn string, rateLimit int32) (*MessageMoveTask, *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	sourceQueueURL, ok := q.queueARNs[sourceArn]
	if !ok {
		return nil, ErrorResourceNotFoundException().WithMessage("SourceArn")
	}
	sourceQueue, ok := q.getQueueUnsafe(sourceQueueURL)
	if !ok {
		return nil, ErrorResourceNotFoundException().WithMessage("SourceArn")
	}
	destinationQueueURL, ok := q.queueARNs[destinationArn]
	if !ok {
		return nil, ErrorResourceNotFoundException().WithMessage("DestinationArn")
	}
	destinationQueue, ok := q.getQueueUnsafe(destinationQueueURL)
	if !ok {
		return nil, ErrorResourceNotFoundException().WithMessage("DestinationArn")
	}
	mmt := NewMoveMessageTask(clock, sourceQueue, destinationQueue, int(rateLimit))
	mmt.Start(context.Background())
	q.moveMessageTasks[mmt.TaskHandle] = mmt
	if _, ok := q.moveMessageTasksBySourceArn[sourceArn]; !ok {
		q.moveMessageTasksBySourceArn[sourceArn] = NewOrderedSet[string]()
	}
	q.moveMessageTasksBySourceArn[sourceArn].Add(mmt.TaskHandle)
	return mmt, nil
}

func (q *Queues) CancelMoveMessageTask(taskHandle string) (*MessageMoveTask, *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()

	task, ok := q.moveMessageTasks[taskHandle]
	if !ok {
		return nil, ErrorResourceNotFoundException()
	}
	if task.Status() != MessageMoveStatusRunning {
		return nil, ErrorResourceNotFoundException()
	}
	task.Close()
	return task, nil
}

func (q *Queues) EachMoveMessageTasks(sourceArn string) iter.Seq[*MessageMoveTask] {
	return func(yield func(*MessageMoveTask) bool) {
		q.queuesMu.Lock()
		defer q.queuesMu.Unlock()
		orderedTasks, ok := q.moveMessageTasksBySourceArn[sourceArn]
		if !ok {
			return
		}
		for taskHandle := range orderedTasks.InOrder() {
			mmt, ok := q.moveMessageTasks[taskHandle]
			if !ok {
				continue
			}
			if !yield(mmt) {
				return
			}
		}
	}
}

func (q *Queues) GetQueueURL(queueName string) (queueURL string, ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queueURL, ok = q.queueURLs[queueName]
	if !ok {
		return
	}
	_, ok = q.getQueueUnsafe(queueURL)
	if !ok {
		queueURL = ""
	}
	return
}

func (q *Queues) GetQueue(queueURL string) (queue *Queue, ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queue, ok = q.getQueueUnsafe(queueURL)
	return
}

func (q *Queues) DeleteQueue(queueURL string) (ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()

	var queue *Queue
	queue, ok = q.getQueueUnsafe(queueURL)
	if !ok {
		return
	}
	queue.deleted = q.clock.Now()
	return
}

func (q *Queues) PurgeDeletedQueues() {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()

	now := q.clock.Now()
	var toDelete []string
	for _, queue := range q.queues {
		if !queue.deleted.IsZero() && now.Sub(queue.deleted) > 60*time.Second {
			toDelete = append(toDelete, queue.URL)
		}
	}
	for _, queueURL := range toDelete {
		q.deleteQueueUnsafe(queueURL)
	}
}

func (q *Queues) getQueueUnsafe(queueURL string) (*Queue, bool) {
	var queue *Queue
	queue, ok := q.queues[queueURL]
	if !ok {
		return nil, false
	}
	if queue.IsDeleted() {
		return nil, false
	}
	return queue, true
}

func (q *Queues) deleteQueueUnsafe(queueURL string) {
	var queue *Queue
	queue, ok := q.queues[queueURL]
	if !ok {
		return
	}
	queue.Close()
	if queue.dlqTarget != nil {
		queue.dlqTarget.RemoveDLQSource(queueURL)
	}
	for taskHandle := range q.moveMessageTasksBySourceArn[queue.ARN].InOrder() {
		q.moveMessageTasks[taskHandle].Close()
		delete(q.moveMessageTasks, taskHandle)
	}
	delete(q.moveMessageTasksBySourceArn, queue.ARN)
	delete(q.queueURLs, queue.Name)
	delete(q.queues, queueURL)
}
