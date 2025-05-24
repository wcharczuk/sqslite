package sqslite

import (
	"context"
	"fmt"
	"log/slog"
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
func NewQueueFromCreateQueueInput(input *sqs.CreateQueueInput) (*Queue, *Error) {
	if err := validateQueueName(*input.QueueName); err != nil {
		return nil, err
	}
	queue := &Queue{
		Name:                               *input.QueueName,
		URL:                                fmt.Sprintf("http://sqslite.%s.localhost/%s", "us-west-2", *input.QueueName),
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
		if m.IsDelayed() {
			q.messagesDelayed[m.Message.MessageID] = m
			continue
		}
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
		msg.IncrementApproximateReceiveCount()
		msg.UpdateVisibilityTimeout(effectiveVisibilityTimeout)
		msg.SetLastReceived(time.Now().UTC())
		messageCopy := msg.Message
		messageCopy.ReceiptHandle = Some(uuid.V4().String())
		slog.Info("receieve message; issuing receipt handle", slog.Duration("visibility_timeout", effectiveVisibilityTimeout), slog.String("receipt_handle", messageCopy.ReceiptHandle.Value))
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
		slog.Error("change message visibility; invalid receipt handle; receipt handle not found", slog.String("receipt_handle", initiatingReceiptHandle))
		return
	}
	var msg *MessageState
	msg, ok = q.messagesOutstanding[messageID]
	if !ok {
		slog.Error("change message visibility; invalid message id; message not found", slog.String("message_id", messageID.String()))
		return
	}
	msg.UpdateVisibilityTimeout(visibilityTimeout)
	if visibilityTimeout == 0 {
		slog.Info("change message visibility batch; marking message ready", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()))
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			slog.Info("change message visibility; marking message ready; deleting message receipt handle", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()), slog.String("receipt_handle", receiptHandle))
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		slog.Info("change message visibility; marking message ready; deleting message receipt handle", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()), slog.String("receipt_handle", initiatingReceiptHandle))
		msg.ReceiptHandles.Del(initiatingReceiptHandle)
		delete(q.messagesOutstandingByReceiptHandle, initiatingReceiptHandle)
		delete(q.messagesOutstanding, msg.Message.MessageID)
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
		msgNode.UpdateVisibilityTimeout(time.Duration(entry.VisibilityTimeout) * time.Second)
		successful = append(successful, types.ChangeMessageVisibilityBatchResultEntry{
			Id: entry.Id,
		})
		if entry.VisibilityTimeout == 0 {
			slog.Info("change message visibility batch; marking message ready", slog.String("initiating_receipt_handle", safeDeref(entry.ReceiptHandle)), slog.String("message_id", messageID.String()))
			for receiptHandle := range msgNode.ReceiptHandles.Consume() {
				slog.Info("change message visibility batch; marking message ready; deleting message receipt handle", slog.String("initiating_receipt_handle", safeDeref(entry.ReceiptHandle)), slog.String("message_id", messageID.String()), slog.String("receipt_handle", receiptHandle))
				delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
			}
			delete(q.messagesOutstandingByReceiptHandle, safeDeref(entry.ReceiptHandle))
			msgNode.ReceiptHandles.Del(safeDeref(entry.ReceiptHandle))
			readyMessages = append(readyMessages, msgNode)
		}
	}
	if len(readyMessages) > 0 {
		for _, msg := range readyMessages {
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
		slog.Error("deleting message; invalid receipt handle; outstanding receipt handle to message id mapping not found", slog.String("initiating_receipt_handle", initiatingReceiptHandle))
		return
	}
	var msg *MessageState
	msg, ok = q.messagesOutstanding[messageID]
	if !ok {
		slog.Error("delete message; invalid message id for receipt handle; outstanding message not found", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()))
		return
	}
	slog.Info("deleting outstanding message", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()))
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		slog.Info("delete message; deleting outstanding message; deleting receipt handle", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()), slog.String("receipt_handle", receiptHandle))
		delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
	}
	slog.Info("delete message; deleting outstanding message; deleting receipt handle", slog.String("initiating_receipt_handle", initiatingReceiptHandle), slog.String("message_id", messageID.String()), slog.String("receipt_handle", initiatingReceiptHandle))
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
			slog.Error("deleting message batch; invalid receipt handle; outstanding receipt handle to message id mapping not found", slog.String("receipt_handle", safeDeref(handle.ReceiptHandle)))
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
			slog.Error("delete message batch; invalid message id for receipt handle; outstanding message not found", slog.String("receipt_handle", safeDeref(handle.ReceiptHandle)), slog.String("message_id", messageID.String()))
			continue
		}
		slog.Info("delete message batch; deleting outstanding message", slog.String("receipt_handle", safeDeref(handle.ReceiptHandle)), slog.String("message_id", messageID.String()))
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			slog.Info("delete message batch; deleting outstanding message; deleting receipt handle", slog.String("initiating_receipt_handle", safeDeref(handle.ReceiptHandle)), slog.String("message_id", messageID.String()), slog.String("receipt_handle", receiptHandle))
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		// largely redundant ...
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
}

func (q *Queue) PurgeExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()

	var toDeleteDelayed []uuid.UUID
	for _, msg := range q.messagesDelayed {
		if msg.IsExpired() {
			toDeleteDelayed = append(toDeleteDelayed, msg.Message.MessageID)
		}
	}
	for _, id := range toDeleteDelayed {
		slog.Info("purging expired; purging delayed message", slog.String("message_id", id.String()))
		delete(q.messagesDelayed, id)
	}
	var toDeleteOustanding []*MessageState
	for _, msg := range q.messagesOutstanding {
		if msg.IsExpired() {
			toDeleteOustanding = append(toDeleteOustanding, msg)
		}
	}
	for _, msg := range toDeleteOustanding {
		slog.Info("purging expired; purging outstanding message", slog.String("message_id", msg.Message.MessageID.String()))
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			slog.Info("purging expired; purging outstanding message; deleting receipt handle", slog.String("id", msg.Message.ID), slog.String("message_id", msg.Message.MessageID.String()), slog.String("receipt_handle", receiptHandle))
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
		delete(q.messagesOutstanding, msg.Message.MessageID)
	}
	var toDeleteNodes []*LinkedListNode[*MessageState]
	for _, msg := range q.messagesReady {
		if msg.Value.IsExpired() {
			toDeleteNodes = append(toDeleteNodes, msg)
		}
	}
	for _, node := range toDeleteNodes {
		slog.Info("purging expired; purging ready message", slog.String("message_id", node.Value.Message.MessageID.String()))
		q.messagesReadyOrdered.Remove(node)
		delete(q.messagesReady, node.Value.Message.MessageID)
	}
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
		slog.Info("update visible; marking message as ready", slog.String("id", msg.Message.ID), slog.String("message_id", msg.Message.MessageID.String()))
		for receiptHandle := range msg.ReceiptHandles.Consume() {
			slog.Info("update visible; marking message as ready; deleting receipt handle", slog.String("id", msg.Message.ID), slog.String("message_id", msg.Message.MessageID.String()), slog.String("receipt_handle", receiptHandle))
			delete(q.messagesOutstandingByReceiptHandle, receiptHandle)
		}
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
		slog.Info("update delayed; marking message as ready", slog.String("id", msg.Message.ID), slog.String("message_id", msg.Message.MessageID.String()))
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
