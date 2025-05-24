package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/spf13/pflag"
)

var (
	flagQueueURL = pflag.String("queue-url", "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/default", "The queue URL")
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
		o.AppID = "sqslite-demo-producer"
	})
	var ordinal int
	for {
		output, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    flagQueueURL,
			MessageBody: aws.String(fmt.Sprintf(`{"messageIndex":%d}`, ordinal)),
		})
		if err != nil {
			slog.Error("error sending message", slog.Any("err", err))
			return
		}
		slog.Info("sent message", slog.String("messageID", *output.MessageId))
		time.Sleep(100 * time.Millisecond)
	}
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}
