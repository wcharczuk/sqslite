package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/wcharczuk/sqslite/internal/sqslite"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

type Run struct {
	id             string
	ctx            context.Context
	outputPath     string
	sqsClient      *sqs.Client
	messageOrdinal uint64
	queueOrdinal   uint64
	after          []func()
}

func (it *Run) Context() context.Context {
	return it.ctx
}

// GetMessageOrdinal gets the messages ordinal (or index) for the run.
func (it *Run) GetMessageOrdinal() (messageOrdinal uint64) {
	messageOrdinal = atomic.LoadUint64(&it.messageOrdinal)
	return
}

func (it *Run) Cleanup() {
	for _, fn := range it.after {
		fn()
	}
}

func (it *Run) Log(message string) {
	slog.Info(message)
}

func (it *Run) Logf(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}

func (it *Run) After(fn func()) {
	it.after = append(it.after, fn)
}

func (it *Run) Sleep(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-it.ctx.Done():
		panic(context.Canceled)
	case <-timer.C:
		return
	}
}

func (it *Run) CreateQueue() (output Queue) {
	it.checkIfCanceled()
	queueName := it.formatQueueName(false)
	queueRes, err := it.sqsClient.CreateQueue(it.ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		panic(err)
	}
	it.After(func() {
		_, err = it.sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{
			QueueUrl: queueRes.QueueUrl,
		})
	})
	queueAttributesRes, err := it.sqsClient.GetQueueAttributes(it.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: queueRes.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		panic(err)
	}
	output.QueueName = queueName
	output.QueueArn = queueAttributesRes.Attributes[string(types.QueueAttributeNameQueueArn)]
	output.QueueURL = safeDeref(queueRes.QueueUrl)
	return
}

func (it *Run) CreateQueueWithDLQ(dlq Queue) (output Queue) {
	queueName := it.formatQueueName(true)
	queueRes, err := it.sqsClient.CreateQueue(it.ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(sqslite.RedrivePolicy{
				DeadLetterTargetArn: dlq.QueueArn,
				MaxReceiveCount:     RedrivePolicyMaxReceiveCount,
			}),
		},
	})
	if err != nil {
		panic(err)
	}
	it.After(func() {
		_, err = it.sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{
			QueueUrl: queueRes.QueueUrl,
		})
	})
	queueAttributesRes, err := it.sqsClient.GetQueueAttributes(it.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: queueRes.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		panic(err)
	}
	output.QueueName = queueName
	output.QueueArn = queueAttributesRes.Attributes[string(types.QueueAttributeNameQueueArn)]
	output.QueueURL = safeDeref(queueRes.QueueUrl)
	return
}

func (it *Run) checkIfCanceled() {
	select {
	case <-it.ctx.Done():
		panic(it.ctx.Err())
	default:
	}
}

func (it *Run) GetQueueURL(queueName string) (queueURL string) {
	it.checkIfCanceled()
	getQueueUrlRes, err := it.sqsClient.GetQueueUrl(it.ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		panic(err)
	}
	queueURL = safeDeref(getQueueUrlRes.QueueUrl)
	return
}

func (it *Run) GetQueueURLByAccountID(queueName, accountID string) (queueURL string) {
	it.checkIfCanceled()
	getQueueUrlRes, err := it.sqsClient.GetQueueUrl(it.ctx, &sqs.GetQueueUrlInput{
		QueueName:              aws.String(queueName),
		QueueOwnerAWSAccountId: aws.String(accountID),
	})
	if err != nil {
		panic(err)
	}
	queueURL = safeDeref(getQueueUrlRes.QueueUrl)
	return
}

func (it *Run) SendMessage(queue Queue) {
	it.checkIfCanceled()
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:    &queue.QueueURL,
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, atomic.AddUint64(&it.messageOrdinal, 1))),
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) SendMessages(queue Queue, count int) {
	it.checkIfCanceled()
	input := &sqs.SendMessageBatchInput{
		QueueUrl: &queue.QueueURL,
	}
	for range count {
		id := atomic.AddUint64(&it.messageOrdinal, 1)
		input.Entries = append(input.Entries,
			types.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprint(id)),
				MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, id)),
			})
	}
	res, err := it.sqsClient.SendMessageBatch(it.ctx, input)
	if err != nil {
		panic(err)
	}
	if len(res.Failed) > 0 {
		panic(fmt.Errorf("failed message: %#v", res.Failed[0]))
	}
}

func (it *Run) SendMessageWithGroup(queue Queue, groupID string) {
	it.checkIfCanceled()
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:       &queue.QueueURL,
		MessageGroupId: aws.String(groupID),
		MessageBody:    aws.String(fmt.Sprintf(`{"message_index":%d}`, atomic.AddUint64(&it.messageOrdinal, 1))),
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) SendMessagesWithGroup(queue Queue, groupID string, count int) {
	it.checkIfCanceled()
	input := &sqs.SendMessageBatchInput{
		QueueUrl: &queue.QueueURL,
	}
	for range count {
		id := atomic.AddUint64(&it.messageOrdinal, 1)
		input.Entries = append(input.Entries,
			types.SendMessageBatchRequestEntry{
				Id:             aws.String(fmt.Sprint(id)),
				MessageGroupId: aws.String(groupID),
				MessageBody:    aws.String(fmt.Sprintf(`{"message_index":%d}`, id)),
			})
	}
	res, err := it.sqsClient.SendMessageBatch(it.ctx, input)
	if err != nil {
		panic(err)
	}
	if len(res.Failed) > 0 {
		panic(fmt.Errorf("failed message: %#v", res.Failed[0]))
	}
}

func (it *Run) SendMessageWithBody(queue Queue, body string) {
	it.checkIfCanceled()
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:    &queue.QueueURL,
		MessageBody: aws.String(body),
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) SendMessageWithAttributes(queue Queue, attributes map[string]types.MessageAttributeValue) {
	it.checkIfCanceled()
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:          &queue.QueueURL,
		MessageBody:       aws.String(fmt.Sprintf(`{"message_index":%d}`, atomic.AddUint64(&it.messageOrdinal, 1))),
		MessageAttributes: attributes,
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) SendMessageWithSystemAttributes(queue Queue, attributes map[string]types.MessageAttributeValue, systemAttributes map[string]types.MessageSystemAttributeValue) {
	it.checkIfCanceled()
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:                &queue.QueueURL,
		MessageBody:             aws.String(fmt.Sprintf(`{"message_index":%d}`, atomic.AddUint64(&it.messageOrdinal, 1))),
		MessageAttributes:       attributes,
		MessageSystemAttributes: systemAttributes,
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) ReceiveMessage(queue Queue) (receiptHandle string, ok bool) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.QueueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   5,
	})
	if err != nil {
		panic(err)
	}
	if len(res.Messages) == 0 {
		return
	}
	receiptHandle = safeDeref(res.Messages[0].ReceiptHandle)
	ok = true
	return
}

func (it *Run) ReceiveMessageWithGroupID(queue Queue) (receiptHandle, groupID string, ok bool) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.QueueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   5,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		panic(err)
	}
	if len(res.Messages) == 0 {
		return
	}
	receiptHandle = safeDeref(res.Messages[0].ReceiptHandle)
	groupID, _ = res.Messages[0].Attributes["MessageGroupId"]
	ok = true
	return
}

func (it *Run) ReceiveMessages(queue Queue) (receiptHandles []string) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.QueueURL,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   5,
	})
	if err != nil {
		panic(err)
	}
	if len(res.Messages) == 0 {
		return
	}
	for _, msg := range res.Messages {
		receiptHandles = append(receiptHandles, safeDeref(msg.ReceiptHandle))
	}
	return
}

func (it *Run) ReceiveMessagesWithGroupIDs(queue Queue) (receiptHandles, messageIDs, groupIDs []string) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.QueueURL,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		panic(err)
	}
	if len(res.Messages) == 0 {
		return
	}
	for _, msg := range res.Messages {
		receiptHandles = append(receiptHandles, safeDeref(msg.ReceiptHandle))
		messageIDs = append(messageIDs, safeDeref(msg.MessageId))
		groupIDs = append(groupIDs, msg.Attributes["MessageGroupId"])
	}
	return
}

func (it *Run) ReceiveMessageWithAttributeNames(queue Queue, attributeNames []string) (receiptHandle string, ok bool) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &queue.QueueURL,
		MaxNumberOfMessages:   1,
		VisibilityTimeout:     5,
		MessageAttributeNames: attributeNames,
	})
	if err != nil {
		panic(err)
	}
	if len(res.Messages) == 0 {
		return
	}
	receiptHandle = safeDeref(res.Messages[0].ReceiptHandle)
	ok = true
	return
}

func (it *Run) ReceiveMessageWithSystemAttributeNames(queue Queue, attributeNames []string, systemAttributeNames []types.MessageSystemAttributeName) (receiptHandle string, ok bool) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    &queue.QueueURL,
		MaxNumberOfMessages:         1,
		VisibilityTimeout:           5,
		MessageAttributeNames:       attributeNames,
		MessageSystemAttributeNames: systemAttributeNames,
	})
	if err != nil {
		panic(err)
	}
	if len(res.Messages) == 0 {
		return
	}
	receiptHandle = safeDeref(res.Messages[0].ReceiptHandle)
	ok = true
	return
}

func (it *Run) GetQueueAttributes(queue Queue, attributeNames ...types.QueueAttributeName) map[string]string {
	it.checkIfCanceled()
	res, err := it.sqsClient.GetQueueAttributes(it.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queue.QueueURL,
		AttributeNames: attributeNames,
	})
	if err != nil {
		panic(err)
	}
	return res.Attributes
}

type QueueStats struct {
	ApproximateNumberOfMessages           int
	ApproximateNumberOfMessagesDelayed    int
	ApproximateNumberOfMessagesNotVisible int
}

func (q QueueStats) TotalNumberOfMessages() int {
	return q.ApproximateNumberOfMessages + q.ApproximateNumberOfMessagesDelayed + q.ApproximateNumberOfMessagesNotVisible
}

func (it *Run) GetQueueStats(queue Queue) (queueStats QueueStats) {
	it.checkIfCanceled()
	res, err := it.sqsClient.GetQueueAttributes(it.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &queue.QueueURL,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		panic(err)
	}
	queueStats = readQueueStatsFromAttributes(res)
	return
}

func readQueueStatsFromAttributes(res *sqs.GetQueueAttributesOutput) (output QueueStats) {
	if res.Attributes == nil {
		return
	}
	output.ApproximateNumberOfMessages, _ = strconv.Atoi(res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	output.ApproximateNumberOfMessagesDelayed, _ = strconv.Atoi(res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)])
	output.ApproximateNumberOfMessagesNotVisible, _ = strconv.Atoi(res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return
}

func (it *Run) DeleteMessage(queue Queue, receiptHandle string) {
	it.checkIfCanceled()
	_, err := it.sqsClient.DeleteMessage(it.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queue.QueueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) ChangeMessageVisibility(queue Queue, receiptHandle string, visibilityTimeout int) {
	it.checkIfCanceled()
	_, err := it.sqsClient.ChangeMessageVisibility(it.ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &queue.QueueURL,
		ReceiptHandle:     &receiptHandle,
		VisibilityTimeout: int32(visibilityTimeout),
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) ExpectFailure(fn func()) {
	defer func() {
		r := recover()
		if r == nil {
			panic("expected panic to be raised by step")
		}
		if r == context.Canceled || r == context.DeadlineExceeded {
			panic("expected panic to be raised by step")
		}
	}()
	fn()
}

func (it *Run) StartMessagesMoveTask(source, destination Queue) (taskHandle string) {
	it.checkIfCanceled()
	res, err := it.sqsClient.StartMessageMoveTask(it.ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:      &source.QueueArn,
		DestinationArn: &destination.QueueArn,
	})
	if err != nil {
		panic(err)
	}
	taskHandle = safeDeref(res.TaskHandle)
	return
}

func (it *Run) ListMessagesMoveTasks(source Queue) (tasks []MoveMessagesTask) {
	it.checkIfCanceled()
	res, err := it.sqsClient.ListMessageMoveTasks(it.ctx, &sqs.ListMessageMoveTasksInput{
		SourceArn: &source.QueueArn,
	})
	if err != nil {
		panic(err)
	}
	for _, t := range res.Results {
		tasks = append(tasks, MoveMessagesTask{
			TaskHandle:     safeDeref(t.TaskHandle),
			DestinationArn: safeDeref(t.DestinationArn),
			Status:         safeDeref(t.Status),
			FailureReason:  safeDeref(t.FailureReason),
		})
	}
	return
}

func (it *Run) formatQueueName(isDLQ bool) string {
	randomID := uuid.V4()
	if isDLQ {
		return fmt.Sprintf("test-%s-%d-%s-dlq", it.id, atomic.AddUint64(&it.queueOrdinal, 1), randomID.ShortString())
	}
	return fmt.Sprintf("test-%s-%d-%s", it.id, atomic.AddUint64(&it.queueOrdinal, 1), randomID.ShortString())
}

type MoveMessagesTask struct {
	TaskHandle     string
	DestinationArn string
	Status         string
	FailureReason  string
}

type Queue struct {
	QueueName string
	QueueURL  string
	QueueArn  string
}

func marshalJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func marshalPrettyJSON(v any) string {
	output := new(bytes.Buffer)
	enc := json.NewEncoder(output)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
	return output.String()
}

func safeDeref[V any](v *V) (out V) {
	if v == nil {
		return
	}
	return *v
}
