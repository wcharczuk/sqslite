package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"
	"github.com/wcharczuk/sqslite/internal/sqslite"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

type Run struct {
	ctx            context.Context
	outputPath     string
	sqsClient      *sqs.Client
	messageOrdinal uint64
	clock          clockwork.Clock
	after          []func()
}

func (it *Run) Cleanup() {
	for _, fn := range it.after {
		fn()
	}
}

func (it *Run) After(fn func()) {
	it.after = append(it.after, fn)
}

func (it *Run) Sleep(d time.Duration) {
	timer := it.clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-it.ctx.Done():
		panic(context.Canceled)
	case <-timer.Chan():
		return
	}
}

func (it *Run) CreateQueue() (output Queue) {
	queueName := fmt.Sprintf("test-queue-%s", uuid.V4())
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
	queueName := fmt.Sprintf("test-queue-%s", uuid.V4())
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

func (it *Run) SendMessage(queue Queue) {
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:    &queue.QueueURL,
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, atomic.AddUint64(&it.messageOrdinal, 1))),
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) ReceiveMessage(queue Queue) (receiptHandle string, ok bool) {
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.QueueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   1,
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
	res, err := it.sqsClient.GetQueueAttributes(it.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queue.QueueURL,
		AttributeNames: attributeNames,
	})
	if err != nil {
		panic(err)
	}
	return res.Attributes
}

func (it *Run) DeleteMessage(queue Queue, receiptHandle string) {
	_, err := it.sqsClient.DeleteMessage(it.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queue.QueueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		panic(err)
	}
}

func (it *Run) ChangeMessageVisibility(queue Queue, receiptHandle string, visibilityTimeout int) {
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
		if r := recover(); r == nil {
			panic("expected panic to be raised by step")
		}
	}()
	fn()
}

func (it *Run) StartMessagesMoveTask(source, destination Queue) (taskHandle string) {
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

func safeDeref[V any](v *V) (out V) {
	if v == nil {
		return
	}
	return *v
}
