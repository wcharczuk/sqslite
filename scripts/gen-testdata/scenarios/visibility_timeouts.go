package scenario

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

var VisibilityTimeouts = Scenario{
	ID:     "visibility_timeouts",
	Script: visibilityTimeouts,
}

func visibilityTimeouts(ctx context.Context, sqsClient *sqs.Client) error {
	baseQueueName := uuid.V4().String()
	queueName := fmt.Sprintf("%s-default", baseQueueName)

	res, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
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

	//
	// start reading from the main queue
	//

	didStartReceiving := make(chan struct{})
	doneReceiving := make(chan struct{})
	receiveErrors := make(chan error, 1)
	go func() {
		close(didStartReceiving)
		defer func() {
			close(doneReceiving)
		}()
		messages, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            res.QueueUrl,
			MaxNumberOfMessages: 2,
			WaitTimeSeconds:     10,
			VisibilityTimeout:   5,
		})
		if err != nil {
			receiveErrors <- err
			return
		}
		slog.Info("received messages", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))
		for _, m := range messages.Messages {
			_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      res.QueueUrl,
				ReceiptHandle: m.ReceiptHandle,
			})
			if err != nil {
				receiveErrors <- err
				return
			}
			slog.Info("deleted message", slog.String("queueName", queueName), slog.String("queueURL", *res.QueueUrl))
		}
	}()
	<-didStartReceiving

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
	if err := sleepOrCancel(ctx, 5*time.Second); err != nil {
		return nil
	}
	for range 5 {
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
	}

	<-doneReceiving
	if len(receiveErrors) > 0 {
		return <-receiveErrors
	}

	return nil
}
