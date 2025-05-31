package sqslite

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/jonboulle/clockwork"
)

// NewQueues returns a new queues storage.
func NewQueues() *Queues {
	return &Queues{
		queueURLs:                   make(map[string]string),
		queueARNs:                   make(map[string]string),
		queues:                      make(map[string]*Queue),
		moveMessageTasks:            make(map[string]*MessageMoveTask),
		moveMessageTasksBySourceArn: make(map[string]*OrderedSet[string]),
	}
}

// Queues holds all the queue
type Queues struct {
	queuesMu                    sync.Mutex
	queueURLs                   map[string]string
	queueARNs                   map[string]string
	queues                      map[string]*Queue
	moveMessageTasks            map[string]*MessageMoveTask
	moveMessageTasksBySourceArn map[string]*OrderedSet[string]
}

func (q *Queues) Close() {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	for _, q := range q.queues {
		q.Close()
	}
}

func (q *Queues) AddQueue(queue *Queue) (err *Error) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	if _, ok := q.queueURLs[queue.Name]; ok {
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
	queue, ok := q.queues[queueURL]
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
		return nil, ErrorInvalidParameterValue("SourceArn: queueURL for arn not found")
	}
	sourceQueue, ok := q.queues[sourceQueueURL]
	if !ok {
		return nil, ErrorInvalidParameterValue("SourceArn: queue not found for queueURL")
	}
	destinationQueueURL, ok := q.queueARNs[destinationArn]
	if !ok {
		return nil, ErrorInvalidParameterValue("DestinationArn: queueURL for arn not found")
	}
	destinationQueue, ok := q.queues[destinationQueueURL]
	if !ok {
		return nil, ErrorInvalidParameterValue("DestinationArn: queue not found for queueURL")
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
		return nil, ErrorInvalidParameterValue("TaskHandle: not found")
	}
	if task.Status() != MessageMoveStatusRunning {
		return nil, ErrorInvalidParameterValue("TaskHandle: task status is not RUNNING")
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
	return
}

func (q *Queues) GetQueue(queueURL string) (queue *Queue, ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	queue, ok = q.queues[queueURL]
	return
}

func (q *Queues) DeleteQueue(queueURL string) (ok bool) {
	q.queuesMu.Lock()
	defer q.queuesMu.Unlock()
	var queue *Queue
	queue, ok = q.queues[queueURL]
	if !ok {
		return
	}
	queue.Close()

	if queue.dlqTarget != nil {
		queue.dlqTarget.RemoveDLQSource(queueURL)
	}

	// cancel move message tasks ...
	delete(q.queueURLs, queue.Name)
	delete(q.queues, queueURL)
	return
}
