package sqslite

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

const DefaultQueueShardCount = 32

// NewQueueFromCreateQueueInput returns a new queue for a given [sqs.CreateQueueInput].
func NewQueueFromCreateQueueInput(authz Authorization, input *sqs.CreateQueueInput) (*Queue, *Error) {
	if err := validateQueueName(*input.QueueName); err != nil {
		return nil, err
	}
	queue := &Queue{
		Name:                            safeDeref(input.QueueName),
		AccountID:                       authz.AccountID,
		URL:                             FormatQueueURL(authz, *input.QueueName),
		ARN:                             FormatQueueARN(authz, *input.QueueName),
		created:                         time.Now(),
		lastModified:                    time.Now(),
		messagesReadyOrdered:            NewGroupedShardedLinkedList[string, *MessageState](DefaultQueueShardCount),
		messagesDelayed:                 make(map[uuid.UUID]*MessageState),
		messagesInflight:                newGroupedInflightMessages(),
		dlqSources:                      make(map[string]*Queue),
		MaximumMessagesInflightPerGroup: 120000,
		Attributes:                      input.Attributes,
		Tags:                            input.Tags,
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

// Queue is an individual queue.
type Queue struct {
	Name      string
	AccountID string
	URL       string
	ARN       string

	RedrivePolicy      Optional[RedrivePolicy]
	RedriveAllowPolicy Optional[RedriveAllowPolicy]

	VisibilityTimeout               time.Duration
	ReceiveMessageWaitTime          time.Duration
	MaximumMessageSizeBytes         int
	MessageRetentionPeriod          time.Duration
	Delay                           Optional[time.Duration]
	MaximumMessagesInflightPerGroup int

	Policy Optional[any]

	Attributes map[string]string
	Tags       map[string]string

	created      time.Time
	lastModified time.Time
	deleted      time.Time

	lifecycleMu sync.Mutex
	mu          sync.Mutex

	isDLQ      uint32
	dlqTarget  *Queue
	dlqSources map[string]*Queue

	messagesReadyOrdered *GroupedShardedLinkedList[string, *MessageState]
	messagesDelayed      map[uuid.UUID]*MessageState
	messagesInflight     *groupedInflightMessages

	retentionWorker        *retentionWorker
	retentionWorkerCancel  func()
	visibilityWorker       *visibilityWorker
	visibilityWorkerCancel func()
	delayWorker            *delayWorker
	delayWorkerCancel      func()

	stats QueueStats
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
	q.retentionWorker = &retentionWorker{queue: q}
	go q.retentionWorker.Start(retentionCtx)

	var visibilityCtx context.Context
	visibilityCtx, q.visibilityWorkerCancel = context.WithCancel(ctx)
	q.visibilityWorker = &visibilityWorker{queue: q}
	go q.visibilityWorker.Start(visibilityCtx)

	var delayCtx context.Context
	delayCtx, q.delayWorkerCancel = context.WithCancel(ctx)
	q.delayWorker = &delayWorker{queue: q}
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

	now := time.Now()
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
		_ = q.messagesReadyOrdered.Push(m.MessageGroupID, m)
	}
}

func (q *Queue) Receive(input *sqs.ReceiveMessageInput) (output []types.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	validMessageGroups := q.messagesInflight.ValidGroups(q.MaximumMessagesInflightPerGroup)
	if len(validMessageGroups) == 0 && q.messagesInflight.Len() > 0 {
		return
	}

	var visibilityTimeout time.Duration
	if input.VisibilityTimeout > 0 {
		visibilityTimeout = time.Duration(input.VisibilityTimeout) * time.Second
	} else {
		visibilityTimeout = q.VisibilityTimeout
	}
	var (
		maxNumberOfMessages  = coalesceZero(int(input.MaxNumberOfMessages), 1)
		effectiveMaxMessages = rand.IntN(maxNumberOfMessages) + 1 /* done on purpose! */
	)
	for {
		// pop removes the message from the "readiness" state
		// but only if it belongs to a valid group (a group that has
		// fewer than ~120k outstanding messages)
		_, msg, ok := q.messagesReadyOrdered.Pop(validMessageGroups...)
		if !ok {
			break
		}

		atomic.AddUint64(&q.stats.TotalMessagesReceived, 1)
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
		atomic.AddInt64(&q.stats.NumMessagesInflight, 1)

		now := time.Now()
		msg.MaybeSetFirstReceived(now)
		msg.IncrementApproximateReceiveCount()
		msg.UpdateVisibilityTimeout(visibilityTimeout, now)
		msg.SetLastReceived(now)

		receiptHandle := ReceiptHandle{
			ID:           uuid.V4(),
			QueueARN:     q.ARN,
			MessageID:    msg.MessageID.String(),
			LastReceived: now,
		}
		msg.ReceiptHandles.Add(receiptHandle.String())
		q.messagesInflight.Push(receiptHandle.String(), msg)
		output = append(output, msg.ForReceiveMessageOutput(input, receiptHandle))
		if len(output) == effectiveMaxMessages {
			break
		}
		validMessageGroups = q.messagesInflight.ValidGroups(q.MaximumMessagesInflightPerGroup)
		if len(validMessageGroups) == 0 {
			return
		}
	}
	return
}

func (q *Queue) PopMessageForMove() (msg *MessageState, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, msg, ok = q.messagesReadyOrdered.Pop()
	if !ok {
		return
	}
	atomic.AddUint64(&q.stats.TotalMessagesMoved, 1)
	atomic.AddInt64(&q.stats.NumMessagesReady, -1)
	return
}

func (q *Queue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout time.Duration) (ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var msg *MessageState
	msg, ok = q.messagesInflight.GetByReceiptHandle(receiptHandle)
	if !ok {
		return
	}
	now := time.Now()
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
	var readyMessages []*MessageState
	now := time.Now()
	for _, entry := range entries {
		msg, ok := q.messagesInflight.GetByReceiptHandle(safeDeref(entry.ReceiptHandle))
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("ReceiptHandleIsInvalid"),
				Id:          entry.Id,
				SenderFault: true,
			})
			return
		}
		atomic.AddUint64(&q.stats.TotalMessagesChangedVisibility, 1)
		msg.UpdateVisibilityTimeout(time.Duration(entry.VisibilityTimeout)*time.Second, now)
		successful = append(successful, types.ChangeMessageVisibilityBatchResultEntry{
			Id: entry.Id,
		})
		if entry.VisibilityTimeout == 0 {
			readyMessages = append(readyMessages, msg)
		}
	}
	if len(readyMessages) > 0 {
		for _, msg := range readyMessages {
			q.moveMessageFromInflightUnsafe(msg)
		}
	}
	return
}

func (q *Queue) Delete(receiptHandle string) (ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	ok = q.messagesInflight.RemoveByReceiptHandle(receiptHandle)
	if !ok {
		return
	}
	atomic.AddUint64(&q.stats.TotalMessagesDeleted, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	atomic.AddInt64(&q.stats.NumMessages, -1)
	return
}

func (q *Queue) DeleteBatch(entries []types.DeleteMessageBatchRequestEntry) (successful []types.DeleteMessageBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var ok bool
	for _, entry := range entries {
		ok = q.messagesInflight.RemoveByReceiptHandle(safeDeref(entry.ReceiptHandle))
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InvalidParameterValue"),
				Id:          entry.Id,
				SenderFault: true,
				Message:     aws.String("ReceiptHandle not found"),
			})
			continue
		}
		atomic.AddUint64(&q.stats.TotalMessagesDeleted, 1)
		atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
		atomic.AddInt64(&q.stats.NumMessages, -1)
		successful = append(successful, types.DeleteMessageBatchResultEntry{
			Id: entry.Id,
		})
	}
	return
}

func (q *Queue) Purge() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.messagesReadyOrdered = NewGroupedShardedLinkedList[string, *MessageState](DefaultQueueShardCount)
	q.messagesInflight = newGroupedInflightMessages()
	clear(q.messagesDelayed)

	atomic.AddUint64(&q.stats.TotalMessagesPurged, uint64(atomic.LoadInt64(&q.stats.NumMessages)))
	atomic.StoreInt64(&q.stats.NumMessages, 0)
	atomic.StoreInt64(&q.stats.NumMessagesDelayed, 0)
	atomic.StoreInt64(&q.stats.NumMessagesInflight, 0)
	atomic.StoreInt64(&q.stats.NumMessagesReady, 0)
}

func (q *Queue) PurgeExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
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
	for msg := range q.messagesInflight.Each() {
		if msg.IsExpired(now) {
			toDeleteOustanding = append(toDeleteOustanding, msg)
		}
	}
	for _, msg := range toDeleteOustanding {
		q.messagesInflight.Remove(msg)
		deleted[msg.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	}
	var toDeleteNodes []*GroupedShardedLinkedListNode[string, *MessageState]
	for msg := range q.messagesReadyOrdered.Each() {
		if msg.Value.IsExpired(now) {
			toDeleteNodes = append(toDeleteNodes, msg)
		}
	}
	for _, node := range toDeleteNodes {
		q.messagesReadyOrdered.Remove(node)
		deleted[node.Value.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
	}
	atomic.AddUint64(&q.stats.TotalMessagesPurged, uint64(len(deleted)))
	atomic.AddInt64(&q.stats.NumMessages, -int64(len(deleted)))
}

// UpdateInflightVisibility returns messages that are currently
// in flight to the ready queue.
func (q *Queue) UpdateInflightVisibility() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	var ready []*MessageState
	for msg := range q.messagesInflight.Each() {
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
	now := time.Now()
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
	q.messagesInflight.Remove(msg)
	atomic.AddUint64(&q.stats.TotalMessagesInflightToDLQ, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	q.moveMessageToDLQUnsafe(msg)
}

func (q *Queue) moveMessageFromInflightToReadyUnsafe(msg *MessageState) {
	q.messagesInflight.Remove(msg)
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
	_ = q.messagesReadyOrdered.Push(msg.MessageGroupID, msg)
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
	q.lastModified = time.Now()

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
