package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"
)

var (
	flagEndpoint  = pflag.String("endpoint", "http://localhost:4566", "The endpoint URL")
	flagQueueURL  = pflag.String("queue-url", "http://sqslite.us-west-2.local/default", "The queue URL")
	flagBatchSize = pflag.Int("batch-size", 10, "The send message batch size")
	flagPause     = pflag.Duration("pause", 0, "The time to pause between send message batches")
)

func main() {
	pflag.Parse()

	awsRegion := "us-east-1"

	ctx := context.Background()
	sess, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET_KEY", "TOKEN")),
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
		slog.Info("sent message batch", slog.Int("successful", len(output.Successful)), slog.Int("failed", len(output.Failed)))
		// time.Sleep(time.Second)
	}
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}
