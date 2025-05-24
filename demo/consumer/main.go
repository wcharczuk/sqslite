package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/pflag"
)

var (
	flagQueueURL = pflag.String("queue-url", "http://sqs.us-west-2.localhost/default", "The queue URL")
)

func main() {
	pflag.Parse()

	awsEndpoint := "http://localhost:4567"
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
		o.BaseEndpoint = &awsEndpoint
		o.AppID = "sqslite-demo-consumer"
	})
	for {
		res, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            flagQueueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // you _really_ should have this be much shorter than the viz timeout
			VisibilityTimeout:   5,  // this should be ~30 if you don't want things to break
		})
		if err != nil {
			slog.Error("error receiving messagess", slog.Any("err", err))
			return
		}
		for _, m := range res.Messages {
			if rand.Float64() > 0.5 {
				slog.Info("skipping processing message", slog.String("messageID", *m.MessageId))
				continue
			}
			_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      flagQueueURL,
				ReceiptHandle: m.ReceiptHandle,
			})
			if err != nil {
				slog.Error("error deleting message", slog.Any("err", err))
				return
			}
			slog.Info("received message", slog.String("messageID", *m.MessageId))
		}
	}
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}
