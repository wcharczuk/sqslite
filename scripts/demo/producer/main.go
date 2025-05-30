package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"

	"github.com/wcharczuk/sqslite/internal/sqslite"
)

var (
	flagAWSRegion    = pflag.String("region", "us-west-2", "The AWS region")
	flagEndpoint     = pflag.String("endpoint", "http://localhost:4567", "The endpoint URL")
	flagQueueURL     = pflag.String("queue-url", sqslite.QueueURL(sqslite.Authorization{}, "default"), "The queue url (optional; uses a default if unset)")
	flagBatchSize    = pflag.Int("batch-size", 10, "The send message batch size")
	flagPause        = pflag.Duration("pause", 0, "The time to pause between send message batches")
	flagDelaySeconds = pflag.Int("delay-seconds", 0, "The delay seconds for each message")
	flagMaxCount     = pflag.Int("max-count", 0, "The max count of messages produced (0 indicates unlimited)")
)

func main() {
	pflag.Parse()

	ctx := context.Background()
	sess, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*flagAWSRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token")),
	)
	if err != nil {
		maybeFatal(err)
	}
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = flagEndpoint
		o.AppID = "sqslite-demo-producer"
	})
	var ordinal uint64
	for {
		var messages []types.SendMessageBatchRequestEntry
		for x := range *flagBatchSize {
			messages = append(messages, types.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprintf("message_%02d", x)),
				MessageBody: aws.String(fmt.Sprintf(`{"messageIndex":%d}`, int(atomic.AddUint64(&ordinal, 1)))),

				DelaySeconds: 10,
			})
		}
		output, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: flagQueueURL,
			Entries:  messages,
		})
		if err != nil {
			slog.Error("error sending message", slog.Any("err", err))
			return
		}
		if len(output.Failed) > 0 {
			slog.Error("sent message batch with failed messages", slog.Int("successful", len(output.Successful)), slog.Int("failed", len(output.Failed)))
		}
		if *flagPause > 0 {
			time.Sleep(*flagPause)
		}
	}
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}
