package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync/atomic"

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
	flagSpyBindAddr     = pflag.String("spy-bind-addr", ":4567", "The bind address of the spy proxy")
	flagSpyUpstream     = pflag.String("spy-upstream", "https://sqs.us-west-2.amazonaws.com", "The upstream url the spy proxy will forward to")
	flagSpyBaseEndpoint = pflag.String("spy-base-endpoint", "http://localhost:4567", "The endpoint URL (leave blank to use the default)")
)

func main() {
	pflag.Parse()
	slog.SetDefault(
		slog.New(
			slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{}),
		),
	)
	go func() {
		s := &http.Server{
			Addr: *flagSpyBindAddr,
			Handler: &spy.Handler{
				Out:  os.Stdout,
				Next: httputil.NewSingleHostReverseProxy(must(url.Parse(*flagSpyUpstream))),
			},
		}
		slog.Info("spy proxy listening on bind address", slog.String("bind_addr", *flagSpyBindAddr))
		if err := s.ListenAndServe(); err != nil {
			slog.Error("server exited", slog.Any("err", err))
			os.Exit(1)
		}
	}()
	maybeFatal(script())
}

func script() error {
	ctx := context.Background()
	sess, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = flagSpyBaseEndpoint
	})

	baseQueueName := uuid.V4().String()

	queueNameDLQ := fmt.Sprintf("%s-default-dlq", baseQueueName)
	queueName := fmt.Sprintf("%s-default", baseQueueName)
	queueNameSideline := fmt.Sprintf("%s-sideline", baseQueueName)

	resDLQ, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueNameDLQ),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "3600", // 1 hr
		},
	})
	if err != nil {
		return err
	}

	slog.Info("created queue", slog.String("queueName", queueNameDLQ), slog.String("queueURL", *resDLQ.QueueUrl))
	defer func() {
		slog.Info("deleting queue", slog.String("queueName", queueNameDLQ), slog.String("queueURL", *resDLQ.QueueUrl))
		_, _ = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: resDLQ.QueueUrl,
		})
	}()
	queueArnDLQ, err := getQueueARN(ctx, sqsClient, deref(resDLQ.QueueUrl))
	if err != nil {
		return err
	}

	res, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"RedrivePolicy": toJSON(sqslite.RedrivePolicy{
				DeadLetterTargetArn: queueArnDLQ,
				MaxReceiveCount:     10,
			}),
		},
	})
	if err != nil {
		return err
	}
	slog.Info("created queue", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))
	defer func() {
		slog.Info("deleting queue", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))
		_, _ = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: res.QueueUrl,
		})
	}()

	resSideline, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueNameSideline),
		Attributes: map[string]string{
			"DelaySeconds": "15",
		},
	})
	if err != nil {
		return err
	}
	slog.Info("created queue", slog.String("queueName", queueNameSideline), slog.String("queueURL", *resSideline.QueueUrl))
	defer func() {
		slog.Info("deleting queue", slog.String("queueName", queueNameSideline), slog.String("queueURL", *resSideline.QueueUrl))
		_, _ = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: resSideline.QueueUrl,
		})
	}()

	//
	// publish to main queue
	//

	var messageOrdinal uint64
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(fmt.Sprintf(`{"messageIndex":%d}`, atomic.AddUint64(&messageOrdinal, 1))),
		QueueUrl:    res.QueueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"ordinal": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(fmt.Sprint(messageOrdinal)),
			},
			"category": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test"),
			},
		},
	})
	if err != nil {
		return err
	}
	slog.Info("sent message", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(fmt.Sprintf(`{"messageIndex":%d}`, atomic.AddUint64(&messageOrdinal, 1))),
		QueueUrl:    res.QueueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"ordinal": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(fmt.Sprint(messageOrdinal)),
			},
			"category": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test"),
			},
		},
	})
	if err != nil {
		return err
	}
	slog.Info("sent message", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))

	messages, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            res.QueueUrl,
		MaxNumberOfMessages: 2,
		WaitTimeSeconds:     10,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return err
	}
	slog.Info("received messages", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))
	for _, m := range messages.Messages {
		_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      res.QueueUrl,
			ReceiptHandle: m.ReceiptHandle,
		})
		if err != nil {
			return err
		}
		slog.Info("deleted message", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))
	}
	return err
}

func getQueueARN(ctx context.Context, sqsClient *sqs.Client, queueURL string) (string, error) {
	res, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			"QueueArn",
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to get queue attributes: %w", err)
	}
	arn, ok := res.Attributes["QueueArn"]
	if !ok {
		return "", fmt.Errorf("failed to get queue attributes: QueueArn missing")
	}
	return arn, nil
}

func toJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func deref[V any](v *V) (output V) {
	if v == nil {
		return
	}
	output = *v
	return
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
