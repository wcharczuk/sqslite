package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/pflag"
	"github.com/wcharczuk/sqslite/pkg/sqslite"
)

var (
	flagAWSRegion = pflag.String("region", "us-west-2", "The AWS region")
	flagEndpoint  = pflag.String("endpoint", "http://localhost:4566", "The endpoint URL")
	flagQueueName = pflag.String("queue-name", "default", "The queue name")
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
	})
	res, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: flagQueueName,
	})
	maybeFatal(err)
	fmt.Println(*res.QueueUrl)
}

func maybeFatal(err error) {
	if err != nil {
		slog.Error("fatal error", slog.Any("err", err))
		os.Exit(1)
	}
}
