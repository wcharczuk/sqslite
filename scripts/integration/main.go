package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strings"
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
	flagLocal     = pflag.Bool("local", false, "If we should target a local sqslite instance")
	flagScenarios = pflag.StringSlice("scenario", []string{"messages-move"}, fmt.Sprintf(
		"The integration test scenarios to run (%s)",
		strings.Join(slices.Collect(maps.Keys(scenarios)), "|"),
	))
)

// assertions
// - ✅ can recreate queues with the same attributes => yields the same queue url
// - ✅ what is the missing required parameter error type
// - ✅ what error is returned by sendMessage if the body is > 256KiB
// - ✅ sendMessageBatch requires the sum of all the bodes to be < 256KiB
// - ✅ startMessageMoveTask what happens if you put a MaxNumberOfMessagesPerSecond > 500
// - startMessageMoveTask what happens if you put a source that isn't a dlq
// - startMessageMoveTask what happens if you put a destination that disallows a given source with a redriveAllowPolicy
// - startMessageMoveTask what happens if you delete the destination queue with a huge backlog

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
	it := &IntegrationTest{
		ctx:       ctx,
		sqsClient: sqsClient,
		clock:     clockwork.NewRealClock(),
	}
	for _, scenario := range *flagScenarios {
		fn, ok := scenarios[scenario]
		if !ok {
			continue
		}
		slog.Info("running integration test", slog.String("scenario", scenario))
		if err := it.Run(fn); err != nil {
			maybeFatal(err)
		}
	}
	maybeFatal(err)
}

var scenarios = map[string]func(*IntegrationTest){
	"fill-dlq":                     fillDLQ,
	"messages-move":                messagesMove,
	"messages-move-invalid-source": messagesMoveInvalidSource,
}

func messagesMoveInvalidSource(it *IntegrationTest) {
	notDLQ := it.CreateQueue()
	mainQueue := it.CreateQueue()

	it.ExpectFailure(func() {
		_ = it.StartMessagesMoveTask(notDLQ, mainQueue)
	})
}

func fillDLQ(it *IntegrationTest) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)

	for range 5 {
		it.SendMessage(mainQueue)
	}
	for range redrivePolicyMaxReceiveCount {
		messages := it.ReceiveMessages(mainQueue)
		for _, msg := range messages {
			it.ChangeMessageVisibility(mainQueue, msg, 0)
		}
	}
	it.Sleep(time.Second)
}

func messagesMove(it *IntegrationTest) {
	dlq := it.CreateQueue()
	mainQueue := it.CreateQueueWithDLQ(dlq)

	for range 5 {
		it.SendMessage(mainQueue)
	}

	for range redrivePolicyMaxReceiveCount << 1 {
		messages := it.ReceiveMessages(mainQueue)
		if len(messages) != 5 {
			panic(fmt.Errorf("expected receive to have 5 messages, has %d", len(messages)))
		}
		for _, msg := range messages {
			it.ChangeMessageVisibility(mainQueue, msg, 0)
		}
		it.Sleep(time.Second)
	}

	queueAttributes := it.GetQueueAttributes(dlq, types.QueueAttributeNameApproximateNumberOfMessages)
	if value := queueAttributes["ApproximateNumberOfMessages"]; value != "5" {
		panic(fmt.Errorf("expected dlq to have 5 messages, has %s", value))
	}

	taskHandle := it.StartMessagesMoveTask(dlq, mainQueue)

done:
	for {
		tasks := it.ListMessagesMoveTasks(dlq)
		if len(tasks) == 0 {
			panic("expect at least one task")
		}
		if !matchesAny(tasks, func(t MoveMessagesTask) bool {
			return t.Status != "RUNNING" || (t.Status == "RUNNING" && t.TaskHandle == taskHandle)
		}) {
			panic("expect at least one task to have the correct task handle")
		}
		for _, t := range tasks {
			if t.Status == "COMPLETED" {
				break done
			}
			if t.Status == "FAILED" {
				panic(t.FailureReason)
			}
		}
		it.Sleep(time.Second)
	}

	messages := it.ReceiveMessages(mainQueue)
	if len(messages) != 5 {
		panic("expect final moved message count to be 5")
	}
	for _, msg := range messages {
		it.DeleteMessage(mainQueue, msg)
	}
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

var redrivePolicyMaxReceiveCount = 3

func (it *IntegrationTest) CreateQueueWithDLQ(dlq Queue) (output Queue) {
	queueName := fmt.Sprintf("test-queue-%s", uuid.V4())
	queueRes, err := it.sqsClient.CreateQueue(it.ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(sqslite.RedrivePolicy{
				DeadLetterTargetArn: dlq.QueueArn,
				MaxReceiveCount:     redrivePolicyMaxReceiveCount,
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
		VisibilityTimeout:   1,
	})
	if err != nil {
		panic(err)
	}
	for _, msg := range res.Messages {
		receiptHandles = append(receiptHandles, safeDeref(msg.ReceiptHandle))
	}
	return
}

func (it *IntegrationTest) GetQueueAttributes(queue Queue, attributeNames ...types.QueueAttributeName) map[string]string {
	res, err := it.sqsClient.GetQueueAttributes(it.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queue.QueueURL,
		AttributeNames: attributeNames,
	})
	if err != nil {
		panic(err)
	}
	return res.Attributes
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

func (it *IntegrationTest) ExpectFailure(fn func()) {
	defer func() {
		if r := recover(); r == nil {
			panic("expected panic to be raised by step")
		}
	}()
	fn()
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

func matchesAny[V any](values []V, pred func(V) bool) bool {
	return slices.ContainsFunc(values, pred)
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
