package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

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

func sleepOrCancel(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Canceled
	case <-t.C:
		return nil
	}
}
