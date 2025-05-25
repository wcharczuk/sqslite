package sqslite

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"sqslite/pkg/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// NewQueueFromCreateQueueInput returns a new queue for a given [sqs.CreateQueueInput].
func NewQueueFromCreateQueueInput(baseQueueURL string, input *sqs.CreateQueueInput) (*Queue, *Error) {
	if err := validateQueueName(*input.QueueName); err != nil {
		return nil, err
	}
	queue := &Queue{
		Name:                               *input.QueueName,
		URL:                                fmt.Sprintf("%s/%s", baseQueueURL, *input.QueueName),
		messagesReadyOrdered:               new(LinkedList[*MessageState]),
		messagesReady:                      make(map[uuid.UUID]*LinkedListNode[*MessageState]),
		messagesDelayed:                    make(map[uuid.UUID]*MessageState),
		messagesOutstanding:                make(map[uuid.UUID]*MessageState),
		messagesOutstandingByReceiptHandle: make(map[string]uuid.UUID),
		Attributes:                         input.Attributes,
		Tags:                               input.Tags,
	}
	var err *Error
	queue.Delay, err = readAttributeDurationSeconds(input.Attributes, queueAttributeDelaySeconds)
	if err != nil {
		return nil, err
	}
	if queue.Delay.IsSet {
		if err = validateDelay(queue.Delay.Value); err != nil {
			return nil, err
		}
	}

	maximumMessageSizeBytes, err := readAttributeDurationInt(input.Attributes, queueAttributeMaximumMessageSize)
	if err != nil {
		return nil, err
	}
	if maximumMessageSizeBytes.IsSet {
		if err = validateMaximumMessageSizeBytes(maximumMessageSizeBytes.Value); err != nil {
			return nil, err
		}
		queue.MaximumMessageSizeBytes = maximumMessageSizeBytes.Value
	} else {
		queue.MaximumMessageSizeBytes = 256 * 1024 // 256KiB
	}

	messageRetentionPeriod, err := readAttributeDurationSeconds(input.Attributes, queueAttributeMessageRetentionPeriod)
	if err != nil {
		return nil, err
	}
	if messageRetentionPeriod.IsSet {
		if err = validateMessageRetentionPeriod(messageRetentionPeriod.Value); err != nil {
			return nil, err
		}
		queue.MessageRetentionPeriod = messageRetentionPeriod.Value
	} else {
		queue.MessageRetentionPeriod = 4 * 24 * time.Hour // 4 days
	}

	receiveMessageWaitTime, err := readAttributeDurationSeconds(input.Attributes, queueAttributeReceiveMessageWaitTimeSeconds)
	if err != nil {
		return nil, err
	}
	if receiveMessageWaitTime.IsSet {
		if err = validateReceiveMessageWaitTime(receiveMessageWaitTime.Value); err != nil {
			return nil, err
		}
		queue.ReceiveMessageWaitTime = receiveMessageWaitTime.Value
	} else {
		queue.ReceiveMessageWaitTime = 20 * time.Second
	}

	visibilityTimeout, err := readAttributeDurationSeconds(input.Attributes, queueAttributeVisibilityTimeout)
	if err != nil {
		return nil, err
	}
	if visibilityTimeout.IsSet {
		if err = validateVisibilityTimeout(visibilityTimeout.Value); err != nil {
			return nil, err
		}
		queue.VisibilityTimeout = visibilityTimeout.Value
	} else {
		queue.VisibilityTimeout = 30 * time.Second
	}

	//
	// start background workers(s)
	//

	var retentionCtx context.Context
	retentionCtx, queue.retentionWorkerCancel = context.WithCancel(context.Background())
	queue.retentionWorker = &retentionWorker{queue: queue}
	go queue.retentionWorker.Start(retentionCtx)

	var visibilityCtx context.Context
	visibilityCtx, queue.visibilityWorkerCancel = context.WithCancel(context.Background())
	queue.visibilityWorker = &visibilityWorker{queue: queue}
	go queue.visibilityWorker.Start(visibilityCtx)

	var delayCtx context.Context
	delayCtx, queue.delayWorkerCancel = context.WithCancel(context.Background())
	queue.delayWorker = &delayWorker{queue: queue}
	go queue.delayWorker.Start(delayCtx)

	return queue, nil
}

// Queue is an individual queue.
type Queue struct {
	Name string
	URL  string

	VisibilityTimeout       time.Duration
	ReceiveMessageWaitTime  time.Duration
	MaximumMessageSizeBytes int
	MessageRetentionPeriod  time.Duration
	Delay                   Optional[time.Duration]

	Attributes map[string]string
	Tags       map[string]string

	sequenceNumber uint64
	mu             sync.Mutex

	messagesReadyOrdered               *LinkedList[*MessageState]
	messagesReady                      map[uuid.UUID]*LinkedListNode[*MessageState]
	messagesDelayed                    map[uuid.UUID]*MessageState
	messagesOutstanding                map[uuid.UUID]*MessageState
	messagesOutstandingByReceiptHandle map[string]uuid.UUID

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
	NumMessages            int64
	NumMessagesReady       int64
	NumMessagesDelayed     int64
	NumMessagesOutstanding int64

	TotalMessagesSent              uint64
	TotalMessagesReceived          uint64
	TotalMessagesDeleted           uint64
	TotalMessagesChangedVisibility uint64
	TotalMessagesPurged            uint64
}

func (q *Queue) Stats() (output QueueStats) {
	output.NumMessages = atomic.LoadInt64(&q.stats.NumMessages)
	output.NumMessagesReady = atomic.LoadInt64(&q.stats.NumMessagesReady)
	output.NumMessagesDelayed = atomic.LoadInt64(&q.stats.NumMessagesDelayed)
	output.NumMessagesOutstanding = atomic.LoadInt64(&q.stats.NumMessagesOutstanding)

	output.TotalMessagesSent = atomic.LoadUint64(&q.stats.TotalMessagesSent)
	output.TotalMessagesReceived = atomic.LoadUint64(&q.stats.TotalMessagesReceived)
	output.TotalMessagesDeleted = atomic.LoadUint64(&q.stats.TotalMessagesDeleted)
	output.TotalMessagesChangedVisibility = atomic.LoadUint64(&q.stats.TotalMessagesChangedVisibility)
	output.TotalMessagesPurged = atomic.LoadUint64(&q.stats.TotalMessagesPurged)
	return
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.retentionWorkerCancel != nil {
		q.retentionWorkerCancel()
		q.retentionWorkerCancel = nil
	}
	if q.delayWorkerCancel != nil {
		q.delayWorkerCancel()
		q.delayWorkerCancel = nil
	}
	if q.visibilityWorkerCancel != nil {
		q.visibilityWorkerCancel()
		q.visibilityWorkerCancel = nil
	}
}

func (q *Queue) Push(msgs ...*MessageState) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, m := range msgs {
		// only apply the queue level default if
		// - it's set
		// - the message does not specify a delay
		if q.Delay.IsSet && m.Delay.IsZero() {
			m.Delay = q.Delay
		}
		atomic.AddUint64(&q.stats.TotalMessagesSent, 1)
		atomic.AddInt64(&q.stats.NumMessages, 1)
		if m.IsDelayed() {
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

	var effectiveVisibilityTimeout time.Duration
	if visibilityTimeout > 0 {
		effectiveVisibilityTimeout = visibilityTimeout
	} else {
		effectiveVisibilityTimeout = q.VisibilityTimeout
	}
	for msg := range q.messagesReadyOrdered.Consume() {
		atomic.AddUint64(&q.stats.TotalMessagesReceived, 1)
		atomic.AddInt64(&q.stats.NumMessagesReady, -1)
		atomic.AddInt64(&q.stats.NumMessagesOutstanding, 1)
		msg.IncrementApproximateReceiveCount()
		msg.UpdateVisibilityTimeout(effectiveVisibilityTimeout)
		msg.SetLastReceived(time.Now().UTC())
		messageCopy := msg.Message
		messageCopy.ReceiptHandle = Some(uuid.V4().String())
		msg.ReceiptHandles.Add(messageCopy.ReceiptHandle.Value)
		q.messagesOutstandingByReceiptHandle[messageCopy.ReceiptHandle.Value] = messageCopy.MessageID
		q.messagesOutstanding[msg.Message.MessageID] = msg
		output = append(output, messageCopy)
		if len(output) == int(maxNumberOfMessages) {
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
	messageID, ok = q.messagesOutstandingByReceiptHandle[initiatingReceiptHandle]
	if !ok {
		return
	}
	var msg *MessageState
	msg, ok = q.messagesOutstanding[messageID]
	if !ok {
		return
	}
	msg.UpdateVisibilityTimeout(visibilityTimeout)
	if visibilityTimeout == 0 {
		atomic.AddUint64(&q.stats.TotalMessagesChangedVisibility, 1)
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		msg.ReceiptHandles.Del(initiatingReceiptHandle)
		delete(q.messagesOutstandingByReceiptHandle, initiatingReceiptHandle)
		delete(q.messagesOutstanding, msg.Message.MessageID)
		atomic.AddInt64(&q.stats.NumMessagesReady, 1)
		atomic.AddInt64(&q.stats.NumMessagesOutstanding, -1)
		node := q.messagesReadyOrdered.Push(msg)
		q.messagesReady[msg.Message.MessageID] = node
	}
	return
}

func (q *Queue) ChangeMessageVisibilityBatch(entries []types.ChangeMessageVisibilityBatchRequestEntry) (successful []types.ChangeMessageVisibilityBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var messageID uuid.UUID
	var ok bool
	var readyMessages []*MessageState
	for _, entry := range entries {
		messageID, ok = q.messagesOutstandingByReceiptHandle[safeDeref(entry.ReceiptHandle)]
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
		msgNode, ok = q.messagesOutstanding[messageID]
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
		msgNode.UpdateVisibilityTimeout(time.Duration(entry.VisibilityTimeout) * time.Second)
		successful = append(successful, types.ChangeMessageVisibilityBatchResultEntry{
			Id: entry.Id,
		})
		if entry.VisibilityTimeout == 0 {
			for receiptHandle := range msgNode.ReceiptHandles.Consume() {
				delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
			}
			delete(q.messagesOutstandingByReceiptHandle, safeDeref(entry.ReceiptHandle))
			msgNode.ReceiptHandles.Del(safeDeref(entry.ReceiptHandle))
			readyMessages = append(readyMessages, msgNode)
		}
	}
	if len(readyMessages) > 0 {
		for _, msg := range readyMessages {
			atomic.AddInt64(&q.stats.NumMessagesReady, 1)
			atomic.AddInt64(&q.stats.NumMessagesOutstanding, -1)
			delete(q.messagesOutstanding, msg.Message.MessageID)
			node := q.messagesReadyOrdered.Push(msg)
			q.messagesReady[msg.Message.MessageID] = node
		}
	}
	return
}

func (q *Queue) Delete(initiatingReceiptHandle string) (ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var messageID uuid.UUID
	messageID, ok = q.messagesOutstandingByReceiptHandle[initiatingReceiptHandle]
	if !ok {
		return
	}
	var msg *MessageState
	msg, ok = q.messagesOutstanding[messageID]
	if !ok {
		return
	}
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
	}
	atomic.AddUint64(&q.stats.TotalMessagesDeleted, 1)
	atomic.AddInt64(&q.stats.NumMessagesOutstanding, -1)
	atomic.AddInt64(&q.stats.NumMessages, -1)
	delete(q.messagesOutstandingByReceiptHandle, initiatingReceiptHandle)
	delete(q.messagesOutstanding, messageID)
	return
}

func (q *Queue) DeleteBatch(handles []types.DeleteMessageBatchRequestEntry) (successful []types.DeleteMessageBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var messageID uuid.UUID
	var ok bool
	for _, handle := range handles {
		messageID, ok = q.messagesOutstandingByReceiptHandle[safeDeref(handle.ReceiptHandle)]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InvalidParameterValue"),
				Id:          handle.Id,
				SenderFault: true,
				Message:     aws.String("ReceiptHandle not found"),
			})
			continue
		}
		msg, ok := q.messagesOutstanding[messageID]
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
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		atomic.AddUint64(&q.stats.TotalMessagesDeleted, 1)
		atomic.AddInt64(&q.stats.NumMessagesOutstanding, -1)
		atomic.AddInt64(&q.stats.NumMessages, -1)
		delete(q.messagesOutstandingByReceiptHandle, safeDeref(handle.ReceiptHandle))
		delete(q.messagesOutstanding, messageID)
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
	clear(q.messagesOutstanding)
	clear(q.messagesOutstandingByReceiptHandle)

	atomic.AddUint64(&q.stats.TotalMessagesPurged, uint64(atomic.LoadInt64(&q.stats.NumMessages)))
	atomic.StoreInt64(&q.stats.NumMessages, 0)
	atomic.StoreInt64(&q.stats.NumMessagesDelayed, 0)
	atomic.StoreInt64(&q.stats.NumMessagesOutstanding, 0)
	atomic.StoreInt64(&q.stats.NumMessagesReady, 0)
}

func (q *Queue) PurgeExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()

	deleted := make(map[uuid.UUID]struct{})
	var toDeleteDelayed []uuid.UUID
	for _, msg := range q.messagesDelayed {
		if msg.IsExpired() {
			toDeleteDelayed = append(toDeleteDelayed, msg.Message.MessageID)
		}
	}
	for _, id := range toDeleteDelayed {
		atomic.AddInt64(&q.stats.NumMessagesDelayed, -1)
		delete(q.messagesDelayed, id)
		deleted[id] = struct{}{}
	}
	var toDeleteOustanding []*MessageState
	for _, msg := range q.messagesOutstanding {
		if msg.IsExpired() {
			toDeleteOustanding = append(toDeleteOustanding, msg)
		}
	}
	for _, msg := range toDeleteOustanding {
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		deleted[msg.Message.MessageID] = struct{}{}
		atomic.AddInt64(&q.stats.NumMessagesOutstanding, -1)
		delete(q.messagesOutstanding, msg.Message.MessageID)
	}
	var toDeleteNodes []*LinkedListNode[*MessageState]
	for _, msg := range q.messagesReady {
		if msg.Value.IsExpired() {
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

func (q *Queue) UpdateVisible() {
	q.mu.Lock()
	defer q.mu.Unlock()

	var ready []*MessageState
	for _, msg := range q.messagesOutstanding {
		if msg.IsVisible() {
			ready = append(ready, msg)
		}
	}
	for _, msg := range ready {
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		atomic.AddInt64(&q.stats.NumMessagesOutstanding, -1)
		atomic.AddInt64(&q.stats.NumMessagesReady, 1)
		delete(q.messagesOutstanding, msg.Message.MessageID)
		node := q.messagesReadyOrdered.Push(msg)
		q.messagesReady[msg.Message.MessageID] = node
	}
}

func (q *Queue) UpdateDelayed() {
	q.mu.Lock()
	defer q.mu.Unlock()

	var ready []*MessageState
	for _, msg := range q.messagesDelayed {
		if !msg.IsDelayed() {
			ready = append(ready, msg)
		}
	}
	for _, msg := range ready {
		atomic.AddInt64(&q.stats.NumMessagesDelayed, -1)
		atomic.AddInt64(&q.stats.NumMessagesReady, 1)
		delete(q.messagesDelayed, msg.Message.MessageID)
		node := q.messagesReadyOrdered.Push(msg)
		q.messagesReady[msg.Message.MessageID] = node
	}
}

// NewMessageStateFromInput returns a new [MessageState] from a given send message input
func (q *Queue) NewMessageState(m Message, delaySeconds int) (*MessageState, *Error) {
	nowUTC := time.Now().UTC()
	sqsm := &MessageState{
		Message:           m,
		Created:           nowUTC,
		ReceiptHandles:    &SafeSet[string]{storage: make(map[string]struct{})},
		SequenceNumber:    atomic.AddUint64(&q.sequenceNumber, 1),
		RetentionDeadline: nowUTC.Add(q.MessageRetentionPeriod),
	}
	if delaySeconds > 0 {
		sqsm.Delay = Some(time.Duration(delaySeconds) * time.Second)
	}
	return sqsm, nil
}

//
// internal methods
//

const (
	messageAttributeApproximateReceiveCount = "ApproximateReceiveCount"
	messageAttributeMessageGroupID          = "MessageGroupId"
	messageAttributeMessageDeduplicationId  = "MessageDeduplicationId"
)

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

var validQueueNameRegexp = regexp.MustCompile("[0-9,a-z,A-Z,_-]")

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

func readAttributeDurationSeconds(attributes map[string]string, attributeName string) (output Optional[time.Duration], err *Error) {
	value, ok := attributes[attributeName]
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

func readAttributeDurationInt(attributes map[string]string, attributeName string) (output Optional[int], err *Error) {
	value, ok := attributes[attributeName]
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

const (
	queueAttributeDelaySeconds                  = "DelaySeconds"
	queueAttributeMaximumMessageSize            = "MaximumMessageSize"
	queueAttributeMessageRetentionPeriod        = "MessageRetentionPeriod"
	queueAttributeReceiveMessageWaitTimeSeconds = "ReceiveMessageWaitTimeSeconds"
	queueAttributeVisibilityTimeout             = "VisibilityTimeout"
)
