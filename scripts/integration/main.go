package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"
	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

var (
	flagAWSRegion   = pflag.String("region", sqslite.DefaultRegion, "The AWS region")
	flagScenario    = pflag.String("scenario", "send-message-batch-large-bodies", "The AWS region")
	flagSpyUpstream = pflag.String("spy-upstream", "https://sqs.us-west-2.amazonaws.com", "The upstrem endpoint for the spy proxy")
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

	spyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		maybeFatal(err)
	}

	spy := &http.Server{
		Handler: &spy.Handler{
			Do:   spy.WriteOutput(os.Stdout),
			Next: httputil.NewSingleHostReverseProxy(must(url.Parse(*flagSpyUpstream))),
		},
	}
	go spy.Serve(spyListener)

	sess, err := config.LoadDefaultConfig(ctx) // config.WithRegion(*flagAWSRegion),
	// config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token")),

	if err != nil {
		maybeFatal(err)
	}
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", spyListener.Addr().String()))
	})
	switch *flagScenario {
	case "recreate-queue":
		if err := recreateQueueWithUpdatedAttributes(ctx, sqsClient); err != nil {
			maybeFatal(err)
		}
	case "create-queue-missing-queue-name":
		if err := createQueueMissingQueueName(ctx, sqsClient); err != nil {
			maybeFatal(err)
		}
	case "create-queue-long-queue-name":
		if err := createQueueLongQueueName(ctx, sqsClient); err != nil {
			maybeFatal(err)
		}
	case "create-queue-queue-name-invalid":
		if err := createQueueQueueNameInvalid(ctx, sqsClient); err != nil {
			maybeFatal(err)
		}
	case "send-message-large-body":
		if err := sendMessageLargeBody(ctx, sqsClient); err != nil {
			maybeFatal(err)
		}
	case "send-message-batch-large-bodies":
		if err := sendMessageBatchLargeBodies(ctx, sqsClient); err != nil {
			maybeFatal(err)
		}
	}
}

func sendMessageBatchLargeBodies(ctx context.Context, sqsClient *sqs.Client) error {
	queueName := fmt.Sprintf("test-queue-%s", uuid.V4())
	queueRes, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return err
	}
	defer func() {
		_, err = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: queueRes.QueueUrl,
		})
	}()
	_, _ = sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: queueRes.QueueUrl,
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          aws.String(uuid.V4().String()),
				MessageBody: aws.String(strings.Repeat("a", 128*1024)),
			},
			{
				Id:          aws.String(uuid.V4().String()),
				MessageBody: aws.String(strings.Repeat("a", 128*1024)),
			},
			{
				Id:          aws.String(uuid.V4().String()),
				MessageBody: aws.String(strings.Repeat("a", 128*1024)),
			},
		},
	})
	return nil
}

func sendMessageLargeBody(ctx context.Context, sqsClient *sqs.Client) error {
	queueName := fmt.Sprintf("test-queue-%s", uuid.V4())
	queueRes, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return err
	}
	defer func() {
		_, err = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: queueRes.QueueUrl,
		})
	}()
	_, _ = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    queueRes.QueueUrl,
		MessageBody: aws.String(strings.Repeat("a", 512*1024)),
	})
	return nil
}

func createQueueMissingQueueName(ctx context.Context, sqsClient *sqs.Client) error {
	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(""),
	})
	if err != nil {
		fmt.Printf("%#v\n", err)
	}
	return nil
}

func createQueueLongQueueName(ctx context.Context, sqsClient *sqs.Client) error {
	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(strings.Repeat("a", 160)),
	})
	if err != nil {
		fmt.Printf("%#v\n", err)
	}
	return nil
}

func createQueueQueueNameInvalid(ctx context.Context, sqsClient *sqs.Client) error {
	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("test!!!queue"),
	})
	if err != nil {
		fmt.Printf("%#v\n", err)
	}
	return nil
}

func recreateQueueWithUpdatedAttributes(ctx context.Context, sqsClient *sqs.Client) error {
	queueName := fmt.Sprintf("duplicate-%s", uuid.V4().String())
	queueRes, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return err
	}
	_, _ = sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			string(types.QueueAttributeNameDelaySeconds): "10",
		},
	})
	_, err = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: queueRes.QueueUrl,
	})
	return err
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
