package sqslite

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func Test_md5OfMessageAttributes(t *testing.T) {
	messageAttributes := map[string]types.MessageAttributeValue{
		"ordinal": {
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprint(uint64(1))),
		},
		"category": {
			DataType:    aws.String("String"),
			StringValue: aws.String("test"),
		},
	}
	expectedSum := "d21ae90c49061e927f57df5f5d72be1f"
	actualSum := md5OfMessageAttributes(messageAttributes)
	require.EqualValues(t, expectedSum, actualSum)
}
