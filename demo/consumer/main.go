package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/pflag"
)

var (
	flagEndpoint                 = pflag.String("endpoint", "http://localhost:4566", "The endpoint URL")
	flagQueueURL                 = pflag.String("queue-url", "http://sqslite.us-west-2.local/default", "The queue URL")
	flagNumPollers               = pflag.Int("num-pollers", runtime.NumCPU(), "The number of queue pollers")
	flagFailurePct               = pflag.Float64("failure-pct", 0.1, "The fraction of messages to skip deletion for, triggering visibility timeouts")
	flagMaxNumberOfMessages      = pflag.Int32("max-number-of-messages", 10, "The time in seconds to wait for the receive batch [0,10]")
	flagWaitTimeSeconds          = pflag.Int32("wait-time-seconds", 20, "The time in seconds to wait for the receive batch")
	flagVisibilityTimeoutSeconds = pflag.Int32("visibility-timeout-seconds", 30, "The visibility timeout for received messages in seconds")
)

func main() {
	pflag.Parse()

	awsRegion := "us-east-1"

	ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer done()

	sess, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET_KEY", "TOKEN")),
	)
	if err != nil {
		maybeFatal(err)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	poll := func(_ int) func() error {
		return func() error {
			sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
				o.BaseEndpoint = flagEndpoint
				o.AppID = "sqslite-demo-consumer"
			})
			for {
				res, err := sqsClient.ReceiveMessage(groupCtx, &sqs.ReceiveMessageInput{
					QueueUrl:            flagQueueURL,
					MaxNumberOfMessages: *flagMaxNumberOfMessages,
					WaitTimeSeconds:     *flagWaitTimeSeconds,
					VisibilityTimeout:   *flagVisibilityTimeoutSeconds,
				})
				if err != nil {
					return fmt.Errorf("error receiving messages: %w", err)
				}
				for _, m := range res.Messages {
					if *flagFailurePct > 0 {
						if rand.Float64() < *flagFailurePct {
							continue
						}
					}
					_, err = sqsClient.DeleteMessage(groupCtx, &sqs.DeleteMessageInput{
						QueueUrl:      flagQueueURL,
						ReceiptHandle: m.ReceiptHandle,
					})
					if err != nil {
						return fmt.Errorf("error deleting message: %w", err)
					}
				}
			}
		}
	}
	for index := range *flagNumPollers {
		group.Go(poll(index))
	}
	if err := group.Wait(); err != nil {
		maybeFatal(err)
	}
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}
