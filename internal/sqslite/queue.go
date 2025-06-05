package sqslite

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

const DefaultQueueShardCount = 32

// NewQueueFromCreateQueueInput returns a new queue for a given [sqs.CreateQueueInput].
func NewQueueFromCreateQueueInput(clock clockwork.Clock, authz Authorization, input *sqs.CreateQueueInput) (*Queue, *Error) {
	if err := validateQueueName(*input.QueueName); err != nil {
		return nil, err
	}
	queue := &Queue{
		Name:                            safeDeref(input.QueueName),
		AccountID:                       authz.AccountID,
		URL:                             FormatQueueURL(authz, *input.QueueName),
		ARN:                             FormatQueueARN(authz, *input.QueueName),
		created:                         clock.Now(),
		lastModified:                    clock.Now(),
		messagesReadyOrdered:            NewShardedLinkedList[*MessageState](DefaultQueueShardCount),
		messagesReady:                   make(map[uuid.UUID]*ShardedLinkedListNode[*MessageState]),
		messagesDelayed:                 make(map[uuid.UUID]*MessageState),
		messagesInflight:                make(map[uuid.UUID]*MessageState),
		messagesInflightByReceiptHandle: make(map[string]uuid.UUID),
		dlqSources:                      make(map[string]*Queue),
		MaximumMessagesInflight:         120000,
		Attributes:                      input.Attributes,
		Tags:                            input.Tags,
		clock:                           clock,
	}
	if err := queue.applyQueueAttributesUnsafe(input.Attributes, true /*applyDefaults*/); err != nil {
		return nil, err
	}
	return queue, nil
}

// FormatQueueURL creates a queue url from required inputs.
func FormatQueueURL(authz Authorization, queueName string) string {
	return fmt.Sprintf("http://%s/%s/%s", authz.HostOrDefault(), authz.AccountID, queueName)
}

// FormatQueueARN creates a queue arn from required inputs.
func FormatQueueARN(authz Authorization, queueName string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", authz.RegionOrDefault(), authz.AccountID, queueName)
}

// RedrivePolicy is the json data in the [types.QueueAttributeNameRedrivePolicy] attribute field.
type RedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

type RedrivePermission string

const (
	RedrivePermissionAllowAll RedrivePermission = "allowAll"
	RedrivePermissionDenyAll  RedrivePermission = "denyAll "
	RedrivePermissionByQueue  RedrivePermission = "byQueue"
)

// RedriveAllowPolicy is the json data in the [types.QueueAttributeNameRedrivePolicy] attribute field.
type RedriveAllowPolicy struct {
	RedrivePermission RedrivePermission `json:"redrivePermission"`
	SourceQueueARNs   []string          `json:"sourceQueueArns"`
}

// AllowSource returns if a given destination redrive allow policy
// allows a given source arn.
func (r RedriveAllowPolicy) AllowSource(sourceARN string) bool {
	if r.RedrivePermission == RedrivePermissionAllowAll {
		return true
	}
	if r.RedrivePermission == RedrivePermissionDenyAll {
		return false
	}
	if r.RedrivePermission == RedrivePermissionByQueue {
		return slices.ContainsFunc(r.SourceQueueARNs, func(arn string) bool {
			return sourceARN == arn
		})
	}
	return false /*fail closed*/
}

// Queue is an individual queue.
type Queue struct {
	Name      string
	AccountID string
	URL       string
	ARN       string

	RedrivePolicy      Optional[RedrivePolicy]
	RedriveAllowPolicy Optional[RedriveAllowPolicy]

	VisibilityTimeout       time.Duration
	ReceiveMessageWaitTime  time.Duration
	MaximumMessageSizeBytes int
	MessageRetentionPeriod  time.Duration
	Delay                   Optional[time.Duration]
	MaximumMessagesInflight int

	Policy Optional[any]

	Attributes map[string]string
	Tags       map[string]string

	created      time.Time
	lastModified time.Time
	deleted      time.Time

	lifecycleMu sync.Mutex
	mu          sync.Mutex

	clock clockwork.Clock

	isDLQ      uint32
	dlqTarget  *Queue
	dlqSources map[string]*Queue

	messagesReadyOrdered            *ShardedLinkedList[*MessageState]
	messagesReady                   map[uuid.UUID]*ShardedLinkedListNode[*MessageState]
	messagesDelayed                 map[uuid.UUID]*MessageState
	messagesInflight                map[uuid.UUID]*MessageState
	messagesInflightByReceiptHandle map[string]uuid.UUID

	retentionWorker        *retentionWorker
	retentionWorkerCancel  func()
	visibilityWorker       *visibilityWorker
	visibilityWorkerCancel func()
	delayWorker            *delayWorker
	delayWorkerCancel      func()

	stats QueueStats
}

// Stats are basic statistics about the queue.
type QueueStats struct {
	NumMessages         int64
	NumMessagesReady    int64
	NumMessagesDelayed  int64
	NumMessagesInflight int64

	TotalMessagesSent              uint64
	TotalMessagesReceived          uint64
	TotalMessagesMoved             uint64
	TotalMessagesDeleted           uint64
	TotalMessagesChangedVisibility uint64
	TotalMessagesPurged            uint64
	TotalMessagesInflightToReady   uint64
	TotalMessagesDelayedToReady    uint64
	TotalMessagesInflightToDLQ     uint64
}

func (q *Queue) Stats() (output QueueStats) {
	output.NumMessages = atomic.LoadInt64(&q.stats.NumMessages)
	output.NumMessagesReady = atomic.LoadInt64(&q.stats.NumMessagesReady)
	output.NumMessagesDelayed = atomic.LoadInt64(&q.stats.NumMessagesDelayed)
	output.NumMessagesInflight = atomic.LoadInt64(&q.stats.NumMessagesInflight)

	output.TotalMessagesSent = atomic.LoadUint64(&q.stats.TotalMessagesSent)
	output.TotalMessagesReceived = atomic.LoadUint64(&q.stats.TotalMessagesReceived)
	output.TotalMessagesMoved = atomic.LoadUint64(&q.stats.TotalMessagesMoved)
	output.TotalMessagesDeleted = atomic.LoadUint64(&q.stats.TotalMessagesDeleted)
	output.TotalMessagesChangedVisibility = atomic.LoadUint64(&q.stats.TotalMessagesChangedVisibility)
	output.TotalMessagesPurged = atomic.LoadUint64(&q.stats.TotalMessagesPurged)
	output.TotalMessagesInflightToReady = atomic.LoadUint64(&q.stats.TotalMessagesInflightToReady)
	output.TotalMessagesDelayedToReady = atomic.LoadUint64(&q.stats.TotalMessagesDelayedToReady)
	output.TotalMessagesInflightToDLQ = atomic.LoadUint64(&q.stats.TotalMessagesInflightToDLQ)
	return
}

// Created returns the timestamp when the queue was instantiated with [NewQueueFromCreateQueueInput].
func (q *Queue) Created() time.Time {
	return q.created
}

// LastModified returns the timestamp when the queue was created or last updated with [Queue.SetQueueAttributes].
func (q *Queue) LastModified() time.Time {
	return q.lastModified
}

// Deleted returns the deleted timestamp.
func (q *Queue) Deleted() time.Time {
	return q.deleted
}

// IsDeleted returns if the queue has been deleted and is waiting to be purged.
func (q *Queue) IsDeleted() bool {
	return !q.deleted.IsZero()
}

func (q *Queue) Start(ctx context.Context) {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	var retentionCtx context.Context
	retentionCtx, q.retentionWorkerCancel = context.WithCancel(ctx)
	q.retentionWorker = &retentionWorker{queue: q, clock: q.clock}
	go q.retentionWorker.Start(retentionCtx)

	var visibilityCtx context.Context
	visibilityCtx, q.visibilityWorkerCancel = context.WithCancel(ctx)
	q.visibilityWorker = &visibilityWorker{queue: q, clock: q.clock}
	go q.visibilityWorker.Start(visibilityCtx)

	var delayCtx context.Context
	delayCtx, q.delayWorkerCancel = context.WithCancel(ctx)
	q.delayWorker = &delayWorker{queue: q, clock: q.clock}
	go q.delayWorker.Start(delayCtx)
}

func (q *Queue) Close() {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()
	if q.retentionWorkerCancel != nil {
		q.retentionWorkerCancel()
		q.retentionWorkerCancel = nil
		q.retentionWorker = nil
	}
	if q.delayWorkerCancel != nil {
		q.delayWorkerCancel()
		q.delayWorkerCancel = nil
		q.delayWorker = nil
	}
	if q.visibilityWorkerCancel != nil {
		q.visibilityWorkerCancel()
		q.visibilityWorkerCancel = nil
		q.visibilityWorker = nil
	}
}

func (q *Queue) AddDLQSources(sources ...*Queue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, queue := range sources {
		atomic.StoreUint32(&q.isDLQ, 1)
		q.dlqSources[queue.URL] = queue
	}
}

func (q *Queue) RemoveDLQSource(queueURL string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.dlqSources, queueURL)
	if len(q.dlqSources) == 0 {
		atomic.StoreUint32(&q.isDLQ, 0)
	}
}

// IsDLQ indicates if a queue is a dlq.
func (q *Queue) IsDLQ() bool {
	return atomic.LoadUint32(&q.isDLQ) == 1
}

// Clock returns the timesource for the queue.
func (q *Queue) Clock() clockwork.Clock {
	return q.clock
}

func (q *Queue) Tag(tags map[string]string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.Tags == nil {
		q.Tags = make(map[string]string)
	}
	maps.Copy(q.Tags, tags)
}

func (q *Queue) Untag(tags []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, key := range tags {
		delete(q.Tags, key)
	}
}

func (q *Queue) SetQueueAttributes(attributes map[string]string) *Error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.applyQueueAttributesUnsafe(attributes, false /*applyDefaults*/)
}

func (q *Queue) Push(msgs ...*MessageState) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()
	for _, m := range msgs {
		// only apply the queue level default if
		// - it's set
		// - the message does not specify a delay
		if q.Delay.IsSet && m.Delay.IsZero() {
			m.Delay = q.Delay
		}
		atomic.AddUint64(&q.stats.TotalMessagesSent, 1)
		atomic.AddInt64(&q.stats.NumMessages, 1)
		if m.IsDelayed(now) {
			atomic.AddInt64(&q.stats.NumMessagesDelayed, 1)
			q.messagesDelayed[m.MessageID] = m
			continue
		}
		atomic.AddInt64(&q.stats.NumMessagesReady, 1)
		node := q.messagesReadyOrdered.Push(m)
		q.messagesReady[m.MessageID] = node
	}
}

func (q *Queue) Receive(input *sqs.ReceiveMessageInput) (output []types.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messagesInflight) >= q.MaximumMessagesInflight {
		return
	}
	var effectiveVisibilityTimeout time.Duration
	if input.VisibilityTimeout > 0 {
		effectiveVisibilityTimeout = time.Duration(input.VisibilityTimeout) * time.Second
	} else {
		effectiveVisibilityTimeout = q.VisibilityTimeout
	}

	var (
		maxNumberOfMessages  = coalesceZero(int(input.MaxNumberOfMessages), 1)
		effectiveMaxMessages = rand.IntN(maxNumberOfMessages) + 1 /* done on purpose! */
	)

	var messagesNoLongerReady []uuid.UUID
	for {
		msg, ok := q.messagesReadyOrdered.Pop()
		if !ok {
			break
		}
		atomic.AddUint64(&q.stats.TotalMessagesReceived, 1)
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
		atomic.AddInt64(&q.stats.NumMessagesInflight, 1)

		now := q.clock.Now()
		msg.MaybeSetFirstReceived(now)
		msg.IncrementApproximateReceiveCount()
		msg.UpdateVisibilityTimeout(effectiveVisibilityTimeout, now)
		msg.SetLastReceived(now)

		receiptHandle := ReceiptHandle{
			ID:           uuid.V4(),
			QueueARN:     q.ARN,
			MessageID:    msg.MessageID.String(),
			LastReceived: now,
		}
		msg.ReceiptHandles.Add(receiptHandle.String())
		q.messagesInflightByReceiptHandle[receiptHandle.String()] = msg.MessageID
		q.messagesInflight[msg.MessageID] = msg

		output = append(output, msg.ForReceiveMessageOutput(input, receiptHandle))
		messagesNoLongerReady = append(messagesNoLongerReady, msg.MessageID)
		if len(output) == effectiveMaxMessages {
			break
		}
		if len(q.messagesInflight) >= q.MaximumMessagesInflight {
			break
		}
	}
	for _, messageID := range messagesNoLongerReady {
		delete(q.messagesReady, messageID)
	}
	return
}

func (q *Queue) PopMessageForMove() (msg *MessageState, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg, ok = q.messagesReadyOrdered.Pop()
	if !ok {
		return
	}
	atomic.AddUint64(&q.stats.TotalMessagesMoved, 1)
	atomic.AddInt64(&q.stats.NumMessagesReady, -1)
	delete(q.messagesReady, msg.MessageID)
	return
}

func (q *Queue) ChangeMessageVisibility(initiatingReceiptHandle string, visibilityTimeout time.Duration) (ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var messageID uuid.UUID
	messageID, ok = q.messagesInflightByReceiptHandle[initiatingReceiptHandle]
	if !ok {
		return
	}
	var msg *MessageState
	msg, ok = q.messagesInflight[messageID]
	if !ok {
		return
	}
	now := q.clock.Now()
	atomic.AddUint64(&q.stats.TotalMessagesChangedVisibility, 1)
	msg.UpdateVisibilityTimeout(visibilityTimeout, now)
	if visibilityTimeout == 0 {
		q.moveMessageFromInflightUnsafe(msg)
	}
	return
}

func (q *Queue) ChangeMessageVisibilityBatch(entries []types.ChangeMessageVisibilityBatchRequestEntry) (successful []types.ChangeMessageVisibilityBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var messageID uuid.UUID
	var ok bool

	var readyMessages []*MessageState
	now := q.clock.Now()
	for _, entry := range entries {
		messageID, ok = q.messagesInflightByReceiptHandle[safeDeref(entry.ReceiptHandle)]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("ReceiptHandleIsInvalid"),
				Id:          entry.Id,
				SenderFault: true,
			})
			return
		}
		var msgNode *MessageState
		msgNode, ok = q.messagesInflight[messageID]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InternalServerError"),
				Id:          entry.Id,
				SenderFault: false,
			})
			return
		}
		atomic.AddUint64(&q.stats.TotalMessagesChangedVisibility, 1)
		msgNode.UpdateVisibilityTimeout(time.Duration(entry.VisibilityTimeout)*time.Second, now)
		successful = append(successful, types.ChangeMessageVisibilityBatchResultEntry{
			Id: entry.Id,
		})
		if entry.VisibilityTimeout == 0 {
			readyMessages = append(readyMessages, msgNode)
		}
	}
	if len(readyMessages) > 0 {
		for _, msg := range readyMessages {
			q.moveMessageFromInflightUnsafe(msg)
		}
	}
	return
}

func (q *Queue) Delete(initiatingReceiptHandle string) (ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	messageID, ok := q.messagesInflightByReceiptHandle[initiatingReceiptHandle]
	if !ok {
		return
	}
	var msg *MessageState
	msg, ok = q.messagesInflight[messageID]
	if !ok {
		return
	}
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(q.messagesInflightByReceiptHandle, receiptHandle)
	}
	atomic.AddUint64(&q.stats.TotalMessagesDeleted, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	atomic.AddInt64(&q.stats.NumMessages, -1)
	delete(q.messagesInflightByReceiptHandle, initiatingReceiptHandle)
	delete(q.messagesInflight, messageID)
	return
}

func (q *Queue) DeleteBatch(handles []types.DeleteMessageBatchRequestEntry) (successful []types.DeleteMessageBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var messageID uuid.UUID
	var ok bool
	for _, handle := range handles {
		messageID, ok = q.messagesInflightByReceiptHandle[safeDeref(handle.ReceiptHandle)]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InvalidParameterValue"),
				Id:          handle.Id,
				SenderFault: true,
				Message:     aws.String("ReceiptHandle not found"),
			})
			continue
		}
		msg, ok := q.messagesInflight[messageID]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InconsistentState"),
				Id:          handle.Id,
				SenderFault: false,
				Message:     aws.String("Message not found for receipt handle"),
			})
			continue
		}
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			delete(q.messagesInflightByReceiptHandle, receiptHandle)
		}
		atomic.AddUint64(&q.stats.TotalMessagesDeleted, 1)
		atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
		atomic.AddInt64(&q.stats.NumMessages, -1)
		delete(q.messagesInflightByReceiptHandle, safeDeref(handle.ReceiptHandle))
		delete(q.messagesInflight, messageID)
		successful = append(successful, types.DeleteMessageBatchResultEntry{
			Id: handle.Id,
		})
	}
	return
}

func (q *Queue) Purge() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.messagesReadyOrdered = NewShardedLinkedList[*MessageState](DefaultQueueShardCount)
	clear(q.messagesReady)
	clear(q.messagesDelayed)
	clear(q.messagesInflight)
	clear(q.messagesInflightByReceiptHandle)

	atomic.AddUint64(&q.stats.TotalMessagesPurged, uint64(atomic.LoadInt64(&q.stats.NumMessages)))
	atomic.StoreInt64(&q.stats.NumMessages, 0)
	atomic.StoreInt64(&q.stats.NumMessagesDelayed, 0)
	atomic.StoreInt64(&q.stats.NumMessagesInflight, 0)
	atomic.StoreInt64(&q.stats.NumMessagesReady, 0)
}

func (q *Queue) PurgeExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()

	deleted := make(map[uuid.UUID]struct{})
	var toDeleteDelayed []uuid.UUID
	for _, msg := range q.messagesDelayed {
		if msg.IsExpired(now) {
			toDeleteDelayed = append(toDeleteDelayed, msg.MessageID)
		}
	}
	for _, id := range toDeleteDelayed {
		atomic.AddInt64(&q.stats.NumMessagesDelayed, -1)
		delete(q.messagesDelayed, id)
		deleted[id] = struct{}{}
	}
	var toDeleteOustanding []*MessageState
	for _, msg := range q.messagesInflight {
		if msg.IsExpired(now) {
			toDeleteOustanding = append(toDeleteOustanding, msg)
		}
	}
	for _, msg := range toDeleteOustanding {
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			delete(q.messagesInflightByReceiptHandle, receiptHandle)
		}
		deleted[msg.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
		delete(q.messagesInflight, msg.MessageID)
	}
	var toDeleteNodes []*ShardedLinkedListNode[*MessageState]
	for _, msg := range q.messagesReady {
		if msg.Value.IsExpired(now) {
			toDeleteNodes = append(toDeleteNodes, msg)
		}
	}
	for _, node := range toDeleteNodes {
		q.messagesReadyOrdered.Remove(node)
		deleted[node.Value.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
		delete(q.messagesReady, node.Value.MessageID)
	}
	atomic.AddUint64(&q.stats.TotalMessagesPurged, uint64(len(deleted)))
	atomic.AddInt64(&q.stats.NumMessages, -int64(len(deleted)))
}

// UpdateInflightVisibility returns messages that are currently
// in flight to the ready queue.
func (q *Queue) UpdateInflightVisibility() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()

	var ready []*MessageState
	for _, msg := range q.messagesInflight {
		if msg.IsVisible(now) {
			ready = append(ready, msg)
		}
	}
	for _, msg := range ready {
		q.moveMessageFromInflightUnsafe(msg)
	}
}

// UpdateDelayedToReady moves messages that were delayed to the ready queue.
func (q *Queue) UpdateDelayedToReady() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()

	var ready []*MessageState
	for _, msg := range q.messagesDelayed {
		if !msg.IsDelayed(now) {
			ready = append(ready, msg)
		}
	}
	for _, msg := range ready {
		q.moveMessageFromDelayedToReadyUnsafe(msg)
	}
}

// GetQueueAttributes gets queue attribute values for a given list of queue attribute names.
func (q *Queue) GetQueueAttributes(attributeNames ...types.QueueAttributeName) map[string]string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.getQueueAttributesUnsafe(attributeNames...)
}

//
// internal methods
//

func (q *Queue) moveMessageFromInflightUnsafe(msg *MessageState) {
	if q.RedrivePolicy.IsSet {
		if msg.ReceiveCount >= uint32(q.RedrivePolicy.Value.MaxReceiveCount) {
			q.moveMessageFromInflightToDLQUnsafe(msg)
			return
		}
	}
	q.moveMessageFromInflightToReadyUnsafe(msg)
}

func (q *Queue) moveMessageFromInflightToDLQUnsafe(msg *MessageState) {
	delete(q.messagesInflight, msg.MessageID)
	atomic.AddUint64(&q.stats.TotalMessagesInflightToDLQ, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	q.moveMessageToDLQUnsafe(msg)
}

func (q *Queue) moveMessageFromInflightToReadyUnsafe(msg *MessageState) {
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(q.messagesInflightByReceiptHandle, receiptHandle)
	}
	delete(q.messagesInflight, msg.MessageID)
	atomic.AddUint64(&q.stats.TotalMessagesInflightToReady, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	q.moveMessageToReadyUnsafe(msg)
}

func (q *Queue) moveMessageFromDelayedToReadyUnsafe(msg *MessageState) {
	delete(q.messagesDelayed, msg.MessageID)
	atomic.AddUint64(&q.stats.TotalMessagesDelayedToReady, 1)
	atomic.AddInt64(&q.stats.NumMessagesDelayed, -1)
	q.moveMessageToReadyUnsafe(msg)
}

func (q *Queue) moveMessageToDLQUnsafe(msg *MessageState) {
	q.dlqTarget.Push(msg)
}

func (q *Queue) moveMessageToReadyUnsafe(msg *MessageState) {
	atomic.AddInt64(&q.stats.NumMessagesReady, 1)
	node := q.messagesReadyOrdered.Push(msg)
	q.messagesReady[msg.MessageID] = node
}

func (q *Queue) getQueueAttributesUnsafe(attributes ...types.QueueAttributeName) map[string]string {
	distinctAttributes := distinct(flatten(apply(attributes, func(v types.QueueAttributeName) []types.QueueAttributeName {
		switch v {
		case types.QueueAttributeNameAll:
			return v.Values()
		default:
			return []types.QueueAttributeName{v}
		}
	})))
	output := make(map[string]string)
	for _, attribute := range distinctAttributes {
		value := q.getQueueAttributeUnsafe(attribute)
		if value != "" {
			output[string(attribute)] = value
		}
	}
	return output
}

func (q *Queue) getQueueAttributeUnsafe(attributeName types.QueueAttributeName) string {
	switch attributeName {
	case types.QueueAttributeNameApproximateNumberOfMessages:
		return fmt.Sprint(atomic.LoadInt64(&q.stats.NumMessages))
	case types.QueueAttributeNameApproximateNumberOfMessagesNotVisible:
		return fmt.Sprint(atomic.LoadInt64(&q.stats.NumMessagesInflight))
	case types.QueueAttributeNameApproximateNumberOfMessagesDelayed:
		return fmt.Sprint(atomic.LoadInt64(&q.stats.NumMessagesDelayed))
	case types.QueueAttributeNameCreatedTimestamp:
		return fmt.Sprint(q.created.Unix())
	case types.QueueAttributeNameLastModifiedTimestamp:
		return fmt.Sprint(q.lastModified.Unix())
	case types.QueueAttributeNameMaximumMessageSize:
		return fmt.Sprint(q.MaximumMessageSizeBytes)
	case types.QueueAttributeNameMessageRetentionPeriod:
		return fmt.Sprint(int(q.MessageRetentionPeriod / time.Second))
	case types.QueueAttributeNamePolicy:
		if q.Policy.IsSet {
			return marshalJSON(q.Policy)
		}
		return ""
	case types.QueueAttributeNameQueueArn:
		return fmt.Sprint(q.ARN)
	case types.QueueAttributeNameReceiveMessageWaitTimeSeconds:
		return fmt.Sprint(int(q.ReceiveMessageWaitTime / time.Second))
	case types.QueueAttributeNameVisibilityTimeout:
		return fmt.Sprint(int(q.VisibilityTimeout / time.Second))
	case types.QueueAttributeNameDelaySeconds:
		if q.Delay.IsSet {
			return fmt.Sprint(int(q.Delay.Value / time.Second))
		}
		return ""
	case types.QueueAttributeNameRedrivePolicy:
		if q.RedrivePolicy.IsSet {
			return marshalJSON(q.RedrivePolicy.Value)
		}
		return ""
	default:
		return ""
	}
}

func (q *Queue) applyQueueAttributesUnsafe(messageAttributes map[string]string, applyDefaults bool) *Error {
	q.lastModified = q.clock.Now()

	delay, err := readAttributeDurationSeconds(messageAttributes, types.QueueAttributeNameDelaySeconds)
	if err != nil {
		return err
	}
	if delay.IsSet {
		if err = validateDelay(q.Delay.Value); err != nil {
			return err
		}
		q.Delay = delay
	}

	maximumMessageSizeBytes, err := readAttributeInt(messageAttributes, types.QueueAttributeNameMaximumMessageSize)
	if err != nil {
		return err
	}
	if maximumMessageSizeBytes.IsSet {
		if err = validateMaximumMessageSizeBytes(maximumMessageSizeBytes.Value); err != nil {
			return err
		}
		q.MaximumMessageSizeBytes = maximumMessageSizeBytes.Value
	} else if applyDefaults {
		q.MaximumMessageSizeBytes = DefaultQueueMaximumMessageSizeBytes // 256KiB
	}

	messageRetentionPeriod, err := readAttributeDurationSeconds(messageAttributes, types.QueueAttributeNameMessageRetentionPeriod)
	if err != nil {
		return err
	}
	if messageRetentionPeriod.IsSet {
		if err = validateMessageRetentionPeriod(messageRetentionPeriod.Value); err != nil {
			return err
		}
		q.MessageRetentionPeriod = messageRetentionPeriod.Value
	} else if applyDefaults {
		q.MessageRetentionPeriod = DefaultQueueMessageRetentionPeriod
	}

	receiveMessageWaitTime, err := readAttributeDurationSeconds(messageAttributes, types.QueueAttributeNameReceiveMessageWaitTimeSeconds)
	if err != nil {
		return err
	}
	if receiveMessageWaitTime.IsSet {
		if err = validateReceiveMessageWaitTime(receiveMessageWaitTime.Value); err != nil {
			return err
		}
		q.ReceiveMessageWaitTime = receiveMessageWaitTime.Value
	} else if applyDefaults {
		q.ReceiveMessageWaitTime = DefaultQueueReceiveMessageWaitTime
	}

	visibilityTimeout, err := readAttributeDurationSeconds(messageAttributes, types.QueueAttributeNameVisibilityTimeout)
	if err != nil {
		return err
	}
	if visibilityTimeout.IsSet {
		if err = validateVisibilityTimeout(visibilityTimeout.Value); err != nil {
			return err
		}
		q.VisibilityTimeout = visibilityTimeout.Value
	} else if applyDefaults {
		q.VisibilityTimeout = DefaultQueueVisibilityTimeout
	}

	redrivePolicy, err := readAttributeRedrivePolicy(messageAttributes)
	if err != nil {
		return err
	}
	if redrivePolicy.IsSet {
		if err = validateRedrivePolicy(redrivePolicy.Value); err != nil {
			return err
		}
		q.RedrivePolicy = redrivePolicy
	}

	redriveAllowPolicy, err := readAttributeRedriveAllowPolicy(messageAttributes)
	if err != nil {
		return err
	}
	if redriveAllowPolicy.IsSet {
		if err = validateRedriveAllowPolicy(redriveAllowPolicy.Value); err != nil {
			return err
		}
		q.RedriveAllowPolicy = redriveAllowPolicy
	}

	policy, err := readAttributePolicy(messageAttributes)
	if err != nil {
		return err
	}
	if policy.IsSet {
		// validate policy ... later
		q.Policy = policy
	}
	return nil
}
