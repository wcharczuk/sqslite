package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jonboulle/clockwork"
	"github.com/spf13/pflag"

	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

var (
	flagAWSRegion = pflag.String("region", sqslite.DefaultRegion, "The AWS region")
	flagScenario  = pflag.String("scenario", "messages-move", "The AWS region")
	flagLocal     = pflag.Bool("local", false, "If we should target a local sqslite instance")
)

// assertions
// - ✅ can recreate queues with the same attributes => yields the same queue url
// - ✅ what is the missing required parameter error type
// - ✅ what error is returned by sendMessage if the body is > 256KiB
// - ✅ sendMessageBatch requires the sum of all the bodes to be < 256KiB
// - ✅ startMessageMoveTask what happens if you put a MaxNumberOfMessagesPerSecond > 500

func main() {
	pflag.Parse()
	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	var upstream = "https://sqs.us-west-2.amazonaws.com"
	if *flagLocal {
		upstream = "http://localhost:4566"
	}

	spyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		maybeFatal(err)
	}

	spy := &http.Server{
		Handler: &spy.Handler{
			Do:   spy.WriteOutput(os.Stdout),
			Next: httputil.NewSingleHostReverseProxy(must(url.Parse(upstream))),
		},
	}
	go spy.Serve(spyListener)

	var sess aws.Config
	if *flagLocal {
		sess, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(*flagAWSRegion),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token")),
		)
		if err != nil {
			maybeFatal(err)
		}
	} else {
		sess, err = config.LoadDefaultConfig(ctx)
		if err != nil {
			maybeFatal(err)
		}
	}
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", spyListener.Addr().String()))
	})
	err = (&IntegrationTest{
		ctx:       ctx,
		sqsClient: sqsClient,
		clock:     clockwork.NewRealClock(),
	}).Run(messagesMove)
	maybeFatal(err)
}

func messagesMove(it *IntegrationTest) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)

	for range 5 {
		it.SendMessage(mainQueue)
	}

	// transfer messages to the dlq
	for range 5 {
		messages := it.ReceiveMessages(mainQueue)
		for _, msg := range messages {
			it.ChangeMessageVisibility(mainQueue, msg, 0)
		}
	}

	it.Sleep(time.Second)
	_ = it.StartMessagesMoveTask(dlq, mainQueue)

done:
	for {
		tasks := it.ListMessagesMoveTasks(dlq)
		for _, t := range tasks {
			if t.Status != "RUNNING" {
				break done
			}
		}
		it.Sleep(time.Second)
	}

	messages := it.ReceiveMessages(mainQueue)
	for _, msg := range messages {
		it.DeleteMessage(mainQueue, msg)
	}
	return
}

type IntegrationTest struct {
	ctx            context.Context
	sqsClient      *sqs.Client
	messageOrdinal uint64
	clock          clockwork.Clock
	after          []func()
}

func (it *IntegrationTest) Run(fn func(*IntegrationTest)) (err error) {
	defer func() {
		it.Cleanup()
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			return
		}
	}()
	fn(it)
	return
}

func (it *IntegrationTest) Cleanup() {
	for _, fn := range it.after {
		fn()
	}
}

func (it *IntegrationTest) After(fn func()) {
	it.after = append(it.after, fn)
}

func (it *IntegrationTest) Sleep(d time.Duration) {
	timer := it.clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-it.ctx.Done():
		panic(context.Canceled)
	case <-timer.Chan():
		return
	}
}

func (it *IntegrationTest) CreateQueue() (output Queue) {
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

func (it *IntegrationTest) CreateQueueWithDLQ(dlq Queue) (output Queue) {
	queueName := fmt.Sprintf("test-queue-%s", uuid.V4())
	queueRes, err := it.sqsClient.CreateQueue(it.ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(sqslite.RedrivePolicy{
				DeadLetterTargetArn: dlq.QueueArn,
				MaxReceiveCount:     3,
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

func (it *IntegrationTest) SendMessage(queue Queue) {
	_, err := it.sqsClient.SendMessage(it.ctx, &sqs.SendMessageInput{
		QueueUrl:    &queue.QueueURL,
		MessageBody: aws.String(fmt.Sprintf(`{"message_index":%d}`, atomic.AddUint64(&it.messageOrdinal, 1))),
	})
	if err != nil {
		panic(err)
	}
}

func (it *IntegrationTest) ReceiveMessages(queue Queue) (receiptHandles []string) {
	res, err := it.sqsClient.ReceiveMessage(it.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.QueueURL,
		MaxNumberOfMessages: 10,
	})
	if err != nil {
		panic(err)
	}
	for _, msg := range res.Messages {
		receiptHandles = append(receiptHandles, safeDeref(msg.ReceiptHandle))
	}
	return
}

func (it *IntegrationTest) DeleteMessage(queue Queue, receiptHandle string) {
	_, err := it.sqsClient.DeleteMessage(it.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queue.QueueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		panic(err)
	}
}

func (it *IntegrationTest) ChangeMessageVisibility(queue Queue, receiptHandle string, visibilityTimeout int) {
	_, err := it.sqsClient.ChangeMessageVisibility(it.ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &queue.QueueURL,
		ReceiptHandle:     &receiptHandle,
		VisibilityTimeout: int32(visibilityTimeout),
	})
	if err != nil {
		panic(err)
	}
}

func (it *IntegrationTest) StartMessagesMoveTask(source, destination Queue) (taskHandle string) {
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

func (it *IntegrationTest) ListMessagesMoveTasks(source Queue) (tasks []MoveMessagesTask) {
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
		})
	}
	return
}

type MoveMessagesTask struct {
	TaskHandle     string
	DestinationArn string
	Status         string
}

type Queue struct {
	QueueName string
	QueueURL  string
	QueueArn  string
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
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
