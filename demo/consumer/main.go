package main

import (
	"context"
	"fmt"
	"log/slog"
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
	flagEndpoint   = pflag.String("endpoint", "http://localhost:4566", "The endpoint URL")
	flagQueueURL   = pflag.String("queue-url", "http://sqslite.us-west-2.localhost/default", "The queue URL")
	flagNumPollers = pflag.Int("num-pollers", runtime.NumCPU(), "The number of queue pollers")
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
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = flagEndpoint
		o.AppID = "sqslite-demo-consumer"
	})

	group, groupCtx := errgroup.WithContext(ctx)
	poll := func(_ int) func() error {
		return func() error {
			for {
				res, err := sqsClient.ReceiveMessage(groupCtx, &sqs.ReceiveMessageInput{
					QueueUrl:            flagQueueURL,
					MaxNumberOfMessages: 10,
					WaitTimeSeconds:     20, // you _really_ should have this be much shorter than the viz timeout
					VisibilityTimeout:   30, // this should be ~30 if you don't want things to break
				})
				if err != nil {
					return fmt.Errorf("error receiving messages: %w", err)
				}
				for _, m := range res.Messages {
					// if rand.Float64() > 0.75 {
					// 	continue
					// }
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
