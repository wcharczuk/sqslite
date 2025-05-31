package sqslite

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

// NewQueueFromCreateQueueInput returns a new queue for a given [sqs.CreateQueueInput].
func NewQueueFromCreateQueueInput(clock clockwork.Clock, authz Authorization, input *sqs.CreateQueueInput) (*Queue, *Error) {
	if err := validateQueueName(*input.QueueName); err != nil {
		return nil, err
	}
	queue := &Queue{
		Name:                            *input.QueueName,
		AccountID:                       authz.AccountIDOrDefault(),
		URL:                             QueueURL(authz, *input.QueueName),
		ARN:                             QueueARN(authz, *input.QueueName),
		created:                         clock.Now(),
		lastModified:                    clock.Now(),
		messagesReadyOrdered:            new(LinkedList[*MessageState]),
		messagesReady:                   make(map[uuid.UUID]*LinkedListNode[*MessageState]),
		messagesDelayed:                 make(map[uuid.UUID]*MessageState),
		messagesInflight:                make(map[uuid.UUID]*MessageState),
		messagesInflightByReceiptHandle: make(map[string]uuid.UUID),
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

const (
	DefaultQueueName    = "default"
	DefaultDLQQueueName = "default-dlq"

	DefaultQueueMaximumMessageSizeBytes = 256 * 1024         // 256KiB
	DefaultQueueMessageRetentionPeriod  = 4 * 24 * time.Hour // 4 days
	DefaultQueueReceiveMessageWaitTime  = 20 * time.Second
	DefaultQueueVisibilityTimeout       = 30 * time.Second
)

// QueueURL creates a queue url from required inputs.
func QueueURL(authz Authorization, queueName string) string {
	return fmt.Sprintf("http://%s/%s/%s", authz.HostOrDefault(), authz.AccountIDOrDefault(), queueName)
}

// QueueARN creates a queue arn from required inputs.
func QueueARN(authz Authorization, queueName string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", authz.RegionOrDefault(), authz.AccountIDOrDefault(), queueName)
}

// RedrivePolicy is the json data in the [types.QueueAttributeNameRedrivePolicy] attribute field.
type RedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

// Queue is an individual queue.
type Queue struct {
	Name      string
	AccountID string
	URL       string
	ARN       string

	RedrivePolicy Optional[RedrivePolicy]

	VisibilityTimeout       time.Duration
	ReceiveMessageWaitTime  time.Duration
	MaximumMessageSizeBytes int
	MessageRetentionPeriod  time.Duration
	Delay                   Optional[time.Duration]
	MaximumMessagesInflight int
	Policy                  Optional[string]

	Attributes map[string]string
	Tags       map[string]string

	created      time.Time
	lastModified time.Time

	sequenceNumber uint64
	lifecycleMu    sync.Mutex
	mu             sync.Mutex

	clock clockwork.Clock

	dlqTarget                       *Queue
	messagesReadyOrdered            *LinkedList[*MessageState]
	messagesReady                   map[uuid.UUID]*LinkedListNode[*MessageState]
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

func (q *Queue) Start() {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	var retentionCtx context.Context
	retentionCtx, q.retentionWorkerCancel = context.WithCancel(context.Background())
	q.retentionWorker = &retentionWorker{queue: q, clock: q.clock}
	go q.retentionWorker.Start(retentionCtx)

	var visibilityCtx context.Context
	visibilityCtx, q.visibilityWorkerCancel = context.WithCancel(context.Background())
	q.visibilityWorker = &visibilityWorker{queue: q, clock: q.clock}
	go q.visibilityWorker.Start(visibilityCtx)

	var delayCtx context.Context
	delayCtx, q.delayWorkerCancel = context.WithCancel(context.Background())
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
			q.messagesDelayed[m.Message.MessageID] = m
			continue
		}
		atomic.AddInt64(&q.stats.NumMessagesReady, 1)
		node := q.messagesReadyOrdered.Push(m)
		q.messagesReady[m.Message.MessageID] = node
	}
}

func (q *Queue) Receive(maxNumberOfMessages int, visibilityTimeout time.Duration) (output []Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messagesInflight) >= q.MaximumMessagesInflight {
		return
	}
	var effectiveVisibilityTimeout time.Duration
	if visibilityTimeout > 0 {
		effectiveVisibilityTimeout = visibilityTimeout
	} else {
		effectiveVisibilityTimeout = q.VisibilityTimeout
	}

	for msg := range q.messagesReadyOrdered.Consume() {
		atomic.AddUint64(&q.stats.TotalMessagesReceived, 1)
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
		atomic.AddInt64(&q.stats.NumMessagesInflight, 1)

		now := q.clock.Now()
		msg.MaybeSetFirstReceived(now)
		msg.IncrementApproximateReceiveCount()
		msg.UpdateVisibilityTimeout(effectiveVisibilityTimeout, now)
		msg.SetLastReceived(now)

		messageCopy := msg.Message
		messageCopy.ReceiptHandle = Some(ReceiptHandle{
			ID:           uuid.V4(),
			QueueARN:     q.ARN,
			MessageID:    msg.Message.MessageID.String(),
			LastReceived: now,
		}.String())
		msg.ReceiptHandles.Add(messageCopy.ReceiptHandle.Value)
		q.messagesInflightByReceiptHandle[messageCopy.ReceiptHandle.Value] = messageCopy.MessageID
		q.messagesInflight[msg.Message.MessageID] = msg

		if messageCopy.Attributes == nil {
			messageCopy.Attributes = make(map[string]string)
		}
		messageCopy.Attributes[MessageAttributeApproximateReceiveCount] = fmt.Sprint(msg.ReceiveCount)

		output = append(output, messageCopy)
		if len(output) == int(maxNumberOfMessages) {
			break
		}
		if len(q.messagesInflight) >= q.MaximumMessagesInflight {
			break
		}
	}
	for _, m := range output {
		delete(q.messagesReady, m.MessageID)
	}
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
		q.moveMessageFromInflightToReadyUnsafe(msg)
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
				Code:        aws.String("InvalidParameterValue"),
				Id:          entry.Id,
				SenderFault: true,
				Message:     aws.String("ReceiptHandle not found"),
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
				Message:     aws.String("Message not found"),
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
			q.moveMessageFromInflightToReadyUnsafe(msg)
		}
	}
	return
}

func (q *Queue) Delete(initiatingReceiptHandle string) (err *Error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	messageID, ok := q.messagesInflightByReceiptHandle[initiatingReceiptHandle]
	if !ok {
		err = ErrorInvalidParameterValue("ReceiptHandle")
		return
	}
	var msg *MessageState
	msg, ok = q.messagesInflight[messageID]
	if !ok {
		err = ErrorInvalidParameterValue("ReceiptHandle")
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

	q.messagesReadyOrdered = new(LinkedList[*MessageState])
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
			toDeleteDelayed = append(toDeleteDelayed, msg.Message.MessageID)
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
		deleted[msg.Message.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
		delete(q.messagesInflight, msg.Message.MessageID)
	}
	var toDeleteNodes []*LinkedListNode[*MessageState]
	for _, msg := range q.messagesReady {
		if msg.Value.IsExpired(now) {
			toDeleteNodes = append(toDeleteNodes, msg)
		}
	}
	for _, node := range toDeleteNodes {
		q.messagesReadyOrdered.Remove(node)
		deleted[node.Value.Message.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
		delete(q.messagesReady, node.Value.Message.MessageID)
	}
	atomic.AddUint64(&q.stats.TotalMessagesPurged, uint64(len(deleted)))
	atomic.AddInt64(&q.stats.NumMessages, -int64(len(deleted)))
}

// UpdateInflightToReady returns messages that are currently
// in flight to the ready queue.
func (q *Queue) UpdateInflightToReady() {
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
		if q.RedrivePolicy.IsSet {
			if msg.ReceiveCount >= uint32(q.RedrivePolicy.Value.MaxReceiveCount) {
				q.moveMessageFromInflightToDLQUnsafe(msg)
				continue
			}
		}
		q.moveMessageFromInflightToReadyUnsafe(msg)
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

// NewMessageStateFromInput returns a new [MessageState] from a given send message input
func (q *Queue) NewMessageState(m Message, created time.Time, delaySeconds int) (*MessageState, *Error) {
	sqsm := &MessageState{
		Message:                m,
		Created:                created,
		MessageRetentionPeriod: q.MessageRetentionPeriod,
		ReceiptHandles:         &SafeSet[string]{storage: make(map[string]struct{})},
		SequenceNumber:         atomic.AddUint64(&q.sequenceNumber, 1),
	}
	if delaySeconds > 0 {
		sqsm.Delay = Some(time.Duration(delaySeconds) * time.Second)
	}
	return sqsm, nil
}

//
// internal methods
//

func (q *Queue) moveMessageFromInflightToDLQUnsafe(msg *MessageState) {
	delete(q.messagesInflight, msg.Message.MessageID)
	atomic.AddUint64(&q.stats.TotalMessagesInflightToDLQ, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	q.moveMessageToDLQUnsafe(msg)
}

func (q *Queue) moveMessageFromInflightToReadyUnsafe(msg *MessageState) {
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(q.messagesInflightByReceiptHandle, receiptHandle)
	}
	delete(q.messagesInflight, msg.Message.MessageID)
	atomic.AddUint64(&q.stats.TotalMessagesInflightToReady, 1)
	atomic.AddInt64(&q.stats.NumMessagesInflight, -1)
	q.moveMessageToReadyUnsafe(msg)
}

func (q *Queue) moveMessageFromDelayedToReadyUnsafe(msg *MessageState) {
	delete(q.messagesDelayed, msg.Message.MessageID)
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
	q.messagesReady[msg.Message.MessageID] = node
}

func (q *Queue) GetQueueAttributes(attributeNames ...types.QueueAttributeName) map[string]string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.getQueueAttributesUnsafe(attributeNames...)
}

func (q *Queue) getQueueAttributesUnsafe(attributes ...types.QueueAttributeName) map[string]string {
	distinctAttributes := distinct(flatten(apply(attributes, func(v types.QueueAttributeName) []types.QueueAttributeName {
		return v.Values()
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
		return fmt.Sprint(q.MessageRetentionPeriod)
	case types.QueueAttributeNamePolicy:
		return fmt.Sprint(q.Policy)
	case types.QueueAttributeNameQueueArn:
		return fmt.Sprint(q.ARN)
	case types.QueueAttributeNameReceiveMessageWaitTimeSeconds:
		return fmt.Sprint(q.ReceiveMessageWaitTime / time.Second)
	case types.QueueAttributeNameVisibilityTimeout:
		return fmt.Sprint(q.VisibilityTimeout / time.Second)
	case types.QueueAttributeNameRedrivePolicy:
		return marshalJSON(q.RedrivePolicy)
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

	maximumMessageSizeBytes, err := readAttributeDurationInt(messageAttributes, types.QueueAttributeNameMaximumMessageSize)
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
		q.RedrivePolicy = redrivePolicy
	}
	policy, ok := messageAttributes[string(types.QueueAttributeNamePolicy)]
	if ok {
		q.Policy = Some(policy)
	}
	return nil
}

func keysAndValues[K comparable, V any](m map[K]V) (output []string) {
	output = make([]string, 0, len(m)<<1)
	for k, v := range m {
		output = append(output, fmt.Sprint(k), fmt.Sprint(v))
	}
	return
}

func safeDeref[T any](valuePtr *T) (output T) {
	if valuePtr != nil {
		output = *valuePtr
	}
	return
}

var validQueueNameRegexp = regexp.MustCompile("^[0-9,a-z,A-Z,_,-]+$")

func validateQueueName(queueName string) *Error {
	if queueName == "" {
		return ErrorInvalidAttributeValue("Invalid QueueName; must not be empty")
	}
	if len(queueName) > 80 {
		return ErrorInvalidAttributeValue("Invalid QueueName; must be less than or equal to 80 characters long")
	}
	if !validQueueNameRegexp.MatchString(queueName) {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid QueueName; invalid characters, regexp used: %s", validQueueNameRegexp.String()))
	}
	return nil
}

func validateDelay(delay time.Duration) *Error {
	if delay < 0 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid DelaySeconds; must be greater than or equal to 0, you put: %v", delay))
	}
	if delay > 90*time.Second {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid DelaySeconds; must be less than or equal to 90 seconds, you put: %v", delay))
	}
	return nil
}

func validateMaximumMessageSizeBytes(maximumMessageSizeBytes int) *Error {
	if maximumMessageSizeBytes < 1024 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid MaximumMessageSizeBytes; must be greater than or equal to 1024, you put: %v", maximumMessageSizeBytes))
	}
	if maximumMessageSizeBytes > 256*1024 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid MaximumMessageSizeBytes; must be less than or equal to 256KiB, you put: %v", maximumMessageSizeBytes))
	}
	return nil
}

func validateMessageRetentionPeriod(messageRetentionPeriod time.Duration) *Error {
	if messageRetentionPeriod < 60*time.Second {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid MessageRetentionPeriod; must be greater than or equal to 60 seconds, you put: %v", messageRetentionPeriod))
	}
	if messageRetentionPeriod > 14*24*time.Hour {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid MessageRetentionPeriod; must be less than or equal to 14 days, you put: %v", messageRetentionPeriod))
	}
	return nil
}

func validateReceiveMessageWaitTime(receiveMessageWaitTime time.Duration) *Error {
	if receiveMessageWaitTime < 0 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid ReceiveMessageWaitTime; must be greater than or equal to 0, you put: %v", receiveMessageWaitTime))
	}
	if receiveMessageWaitTime > 20*time.Second {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid ReceiveMessageWaitTime; must be less than or equal to 20 seconds, you put: %v", receiveMessageWaitTime))
	}
	return nil
}

func validateWaitTimeSeconds(waitTime time.Duration) *Error {
	if waitTime < 0 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid WaitTimeSeconds; must be greater than or equal to 0, you put: %v", waitTime))
	}
	if waitTime > 20*time.Second {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid WaitTimeSeconds; must be less than or equal to 20 seconds, you put: %v", waitTime))
	}
	return nil
}

func validateVisibilityTimeout(visibilityTimeout time.Duration) *Error {
	if visibilityTimeout < 0 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid VisibilityTimeout; must be greater than or equal to 0, you put: %v", visibilityTimeout))
	}
	if visibilityTimeout > 12*time.Hour {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid VisibilityTimeout; must be less than or equal to 12 hours, you put: %v", visibilityTimeout))
	}
	return nil
}

func validateMessageBodySize(body *string, maximumMessageSizeBytes int) *Error {
	if body == nil || *body == "" {
		return nil
	}
	if len(*body) > maximumMessageSizeBytes {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid MessageBody; must be less than %v bytes, you put: %v", maximumMessageSizeBytes, len(*body)))
	}
	return nil
}

func readAttributeDurationSeconds(attributes map[string]string, attributeName types.QueueAttributeName) (output Optional[time.Duration], err *Error) {
	value, ok := attributes[string(attributeName)]
	if !ok {
		return
	}
	parsed, parseErr := strconv.Atoi(value)
	if parseErr != nil {
		err = ErrorInvalidAttributeValue(fmt.Sprintf("Failed to parse %s as duration seconds: %v", attributeName, parseErr))
		return
	}
	output = Some(time.Duration(parsed) * time.Second)
	return
}

func readAttributeDurationInt(attributes map[string]string, attributeName types.QueueAttributeName) (output Optional[int], err *Error) {
	value, ok := attributes[string(attributeName)]
	if !ok {
		return
	}
	parsed, parseErr := strconv.Atoi(value)
	if parseErr != nil {
		err = ErrorInvalidAttributeValue(fmt.Sprintf("Failed to parse %s as integer: %v", attributeName, parseErr))
		return
	}
	output = Some(parsed)
	return
}

func readAttributeRedrivePolicy(attributes map[string]string) (output Optional[RedrivePolicy], err *Error) {
	value, ok := attributes[string(types.QueueAttributeNameRedrivePolicy)]
	if !ok {
		return
	}
	var policy RedrivePolicy
	if jsonErr := json.Unmarshal([]byte(value), &policy); jsonErr != nil {
		err = ErrorInvalidAttributeValue(fmt.Sprintf("Failed to parse redrive policy: %v", jsonErr))
		return
	}
	output = Some(policy)
	return
}
