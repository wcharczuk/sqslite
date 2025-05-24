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
		Name:                    *input.QueueName,
		URL:                     fmt.Sprintf("http://sqs.%s.localhost/%s", "us-west-2", *input.QueueName),
		messagesOrdered:         new(LinkedList[*MessageState]),
		messagesByID:            make(map[uuid.UUID]*LinkedListNode[*MessageState]),
		messagesByReceiptHandle: make(map[string]uuid.UUID),
		Attributes:              input.Attributes,
		Tags:                    input.Tags,
	}
	var err *Error
	//   - DelaySeconds – The length of time, in seconds, for which the delivery of all
	//   messages in the queue is delayed. Valid values: An integer from 0 to 900 seconds
	//   (15 minutes). Default: 0.
	queue.Delay, err = readAttributeDurationSeconds(input.Attributes, queueAttributeDelaySeconds)
	if err != nil {
		return nil, err
	}
	if queue.Delay.IsSet {
		if err = validateDelaySeconds(queue.Delay.Value); err != nil {
			return nil, err
		}
	}

	//   - MaximumMessageSize – The limit of how many bytes a message can contain
	//   before Amazon SQS rejects it. Valid values: An integer from 1,024 bytes (1 KiB)
	//   to 262,144 bytes (256 KiB). Default: 262,144 (256 KiB).
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

	//   - MessageRetentionPeriod – The length of time, in seconds, for which Amazon
	//   SQS retains a message. Valid values: An integer from 60 seconds (1 minute) to
	//   1,209,600 seconds (14 days). Default: 345,600 (4 days).
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

	//   - ReceiveMessageWaitTimeSeconds – The length of time, in seconds, for which a ReceiveMessage
	//   action waits for a message to arrive. Valid values: An integer from 0 to 20
	//   (seconds). Default: 0.
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

	//   - VisibilityTimeout – The visibility timeout for the queue, in seconds. Valid
	//   values: An integer from 0 to 43,200 (12 hours). Default: 30. For more
	//   information about the visibility timeout, see [Visibility Timeout]in the Amazon SQS Developer
	//   Guide.
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

	// do retention things
	var ctx context.Context
	ctx, queue.retentionWorkerCancel = context.WithCancel(context.Background())
	queue.retentionWorker = &retentionWorker{queue: queue}
	go queue.retentionWorker.Start(ctx)
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

	// IsDLQ      bool // we don't support this (yet)
	Attributes map[string]string
	Tags       map[string]string

	sequenceNumber uint64

	messagesMu              sync.Mutex
	messagesOrdered         *LinkedList[*MessageState]
	messagesByID            map[uuid.UUID]*LinkedListNode[*MessageState]
	messagesByReceiptHandle map[string]uuid.UUID

	retentionWorker       *retentionWorker
	retentionWorkerCancel func()
}

func (q *Queue) Close() {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	if q.retentionWorkerCancel != nil {
		q.retentionWorkerCancel()
		q.retentionWorkerCancel = nil
	}
}

func (q *Queue) Push(msgs ...*MessageState) {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	for _, m := range msgs {
		node := q.messagesOrdered.Push(m)
		q.messagesByID[m.Message.MessageID] = node
	}
	return
}

func (q *Queue) Receive(maxNumberOfMessages int, visibilityTimeout time.Duration) (output []Message) {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	for m := range q.messagesOrdered.Each() {
		if m.IsDelayed() {
			continue
		}
		if !m.IsVisible() {
			continue
		}
		m.IncrementApproximateReceiveCount()
		if visibilityTimeout > 0 {
			m.UpdateVisibilityTimeout(visibilityTimeout)
		} else {
			m.UpdateVisibilityTimeout(q.VisibilityTimeout)
		}
		m.SetLastReceived(time.Now().UTC())
		messageCopy := m.Message
		messageCopy.ReceiptHandle = Some(uuid.V4().String())
		m.ReceiptHandles.Add(messageCopy.ReceiptHandle.Value)
		q.messagesByReceiptHandle[messageCopy.ReceiptHandle.Value] = m.Message.MessageID
		output = append(output, messageCopy)
		if len(output) == int(maxNumberOfMessages) {
			break
		}
	}
	return
}

func (q *Queue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout time.Duration) (ok bool) {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	var messageID uuid.UUID
	messageID, ok = q.messagesByReceiptHandle[receiptHandle]
	if !ok {
		slog.Error("change message visibility; invalid receipt handle", slog.String("receipt_handle", receiptHandle))
		return
	}
	var msgNode *LinkedListNode[*MessageState]
	msgNode, ok = q.messagesByID[messageID]
	if !ok {
		slog.Error("change message visibility; invalid message id", slog.String("message_id", messageID.String()))
		return
	}
	msgNode.Value.UpdateVisibilityTimeout(visibilityTimeout)
	return
}

func (q *Queue) ChangeMessageVisibilityBatch(entries []types.ChangeMessageVisibilityBatchRequestEntry) (successful []types.ChangeMessageVisibilityBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	var messageID uuid.UUID
	var ok bool
	for _, entry := range entries {
		messageID, ok = q.messagesByReceiptHandle[*entry.ReceiptHandle]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InvalidParameterValue"),
				Id:          entry.Id,
				SenderFault: true,
				Message:     aws.String("ReceiptHandle not found"),
			})
			return
		}
		var msgNode *LinkedListNode[*MessageState]
		msgNode, ok = q.messagesByID[messageID]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InternalServerError"),
				Id:          entry.Id,
				SenderFault: false,
				Message:     aws.String("Message not found"),
			})
			return
		}
		msgNode.Value.UpdateVisibilityTimeout(time.Duration(entry.VisibilityTimeout) * time.Second)
		successful = append(successful, types.ChangeMessageVisibilityBatchResultEntry{
			Id: entry.Id,
		})
	}
	return
}

func (q *Queue) Delete(receiptHandle string) (ok bool) {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()

	var messageID uuid.UUID
	messageID, ok = q.messagesByReceiptHandle[receiptHandle]
	if !ok {
		slog.Error("deleting message; invalid receipt handle", slog.String("receipt_handle", receiptHandle))
		return
	}
	slog.Error("deleting message", slog.String("messageID", messageID.String()))
	q.deleteUnsafe(messageID)
	return
}

func (q *Queue) DeleteBatch(handles []ReceiptHandleID) (successful []types.DeleteMessageBatchResultEntry, failed []types.BatchResultErrorEntry) {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()

	var messageID uuid.UUID
	var ok bool
	for _, handle := range handles {
		messageID, ok = q.messagesByReceiptHandle[handle.ReceiptHandle]
		if !ok {
			failed = append(failed, types.BatchResultErrorEntry{
				Code:        aws.String("InvalidParameterValue"),
				Id:          aws.String(handle.ID),
				SenderFault: true,
				Message:     aws.String("ReceiptHandle not found"),
			})
			slog.Error("delete message batch; invalid receipt handle", slog.String("receipt_handle", handle.ReceiptHandle))
			continue
		}
		slog.Error("deleting message", slog.String("messageID", messageID.String()))
		q.deleteUnsafe(messageID)
		successful = append(successful, types.DeleteMessageBatchResultEntry{
			Id: aws.String(handle.ID),
		})
	}
	return
}

func (q *Queue) Purge() {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	clear(q.messagesByID)
	clear(q.messagesByReceiptHandle)
	q.messagesOrdered = new(LinkedList[*MessageState])
	return
}

// NewMessageStateFromInput returns a new [MessageState] from a given send message input
func (q *Queue) NewMessageState(m Message, delaySeconds int) (*MessageState, *Error) {
	nowUTC := time.Now().UTC()
	sqsm := &MessageState{
		Message:           m,
		Created:           nowUTC,
		ReceiptHandles:    make(Set[string]),
		SequenceNumber:    atomic.AddUint64(&q.sequenceNumber, 1),
		RetentionDeadline: nowUTC.Add(q.MessageRetentionPeriod),
	}
	if delaySeconds > 0 {
		sqsm.Delay = Some(time.Duration(delaySeconds) * time.Second)
	}
	return sqsm, nil
}

type ReceiptHandleID struct {
	ID            string
	ReceiptHandle string
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

func validateDelaySeconds(delaySeconds time.Duration) *Error {
	if delaySeconds < 0 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid DelaySeconds; must be greater than or equal to 0, you put: %v", delaySeconds))
	}
	if delaySeconds > 90*time.Second {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid DelaySeconds; must be less than or equal to 90 seconds, you put: %v", delaySeconds))
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

func validateWaitMessageSeconds(waitMessageSeconds time.Duration) *Error {
	if waitMessageSeconds < 0 {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid WaitMessageSeconds; must be greater than or equal to 0, you put: %v", waitMessageSeconds))
	}
	if waitMessageSeconds > 20*time.Second {
		return ErrorInvalidAttributeValue(fmt.Sprintf("Invalid WaitMessageSeconds; must be less than or equal to 20 seconds, you put: %v", waitMessageSeconds))
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
	queueAttributeRedrivePolicy                 = "RedrivePolicy" // this will be ignored for now
	queueAttributeFifoQueue                     = "FifoQueue"     // this will be ignored for now
)

func (q *Queue) deleteUnsafe(id uuid.UUID) (ok bool) {
	var node *LinkedListNode[*MessageState]
	node, ok = q.messagesByID[id]
	if !ok {
		return
	}
	q.messagesOrdered.Remove(node)
	for receiptHandle := range node.Value.ReceiptHandles {
		delete(q.messagesByReceiptHandle, receiptHandle)
	}
	return
}

func (q *Queue) PurgeExpired() {
	q.messagesMu.Lock()
	defer q.messagesMu.Unlock()
	var toDelete []uuid.UUID
	for node := range q.messagesOrdered.Each() {
		if node.IsExpired() {
			toDelete = append(toDelete, node.Message.MessageID)
		}
	}
	for _, messageID := range toDelete {
		slog.Error("purging expired message", slog.String("messageID", messageID.String()))
		q.deleteUnsafe(messageID)
	}
}
