package scenario

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/wcharczuk/sqslite/internal/sqslite"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

var Basic = Scenario{
	ID:     "basic",
	Script: basic,
}

func basic(ctx context.Context, sqsClient *sqs.Client) error {
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
