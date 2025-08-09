package sqslite

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func Test_validateQueueName(t *testing.T) {
	err := validateQueueName("")
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateQueueName(strings.Repeat("a", 81))
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateQueueName("test!!!queue")
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateQueueName("test-queue")
	require.Nil(t, err)
}

func Test_validateDelay(t *testing.T) {
	err := validateDelay(-time.Minute)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateDelay(20 * time.Minute)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateDelay(30 * time.Second)
	require.Nil(t, err)
}

func Test_validateMaximumMessageSizeBytes(t *testing.T) {
	err := validateMaximumMessageSizeBytes(512)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMaximumMessageSizeBytes(2048 * 1024)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMaximumMessageSizeBytes(128 * 1024)
	require.Nil(t, err)
}

func Test_validateMessageRetentionPeriod(t *testing.T) {
	err := validateMessageRetentionPeriod(30 * time.Second)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMessageRetentionPeriod(15 * 25 * time.Hour)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMessageRetentionPeriod(10 * 24 * time.Hour)
	require.Nil(t, err)
}

func Test_validateReceiveMessageWaitTime(t *testing.T) {
	err := validateReceiveMessageWaitTime(-time.Second)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateReceiveMessageWaitTime(30 * time.Second)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateReceiveMessageWaitTime(10 * time.Second)
	require.Nil(t, err)
}

func Test_validateWaitTimeSeconds(t *testing.T) {
	err := validateWaitTimeSeconds(-time.Second)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateWaitTimeSeconds(30 * time.Second)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateWaitTimeSeconds(10 * time.Second)
	require.Nil(t, err)
}

func Test_validateVisibilityTimeout(t *testing.T) {
	err := validateVisibilityTimeout(-time.Second)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateVisibilityTimeout(13 * time.Hour)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateVisibilityTimeout(10 * time.Second)
	require.Nil(t, err)
}

func Test_validateRedrivePolicy(t *testing.T) {
	err := validateRedrivePolicy(RedrivePolicy{
		MaxReceiveCount: -1,
	})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateRedrivePolicy(RedrivePolicy{
		MaxReceiveCount: 1001,
	})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateRedrivePolicy(RedrivePolicy{
		MaxReceiveCount: 500,
	})
	require.Nil(t, err)
}

func Test_validateMessageBody(t *testing.T) {
	/* in practice this is maybe _more_ strict than proper sqs */

	err := validateMessageBody(nil, 128*1024)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMessageBody(aws.String(""), 128*1024)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMessageBody(aws.String(strings.Repeat("a", 64*1024)), 32*1024)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMessageBody(aws.String(string([]byte{0xC0, 0xAF, 0xFE, 0xFF})), 32*1024)
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateMessageBody(aws.String(`{"message_index":0}`), 256*1024)
	require.Nil(t, err)
}

func Test_validateBatchEntryIDs(t *testing.T) {
	err := validateBatchEntryIDs([]string{"dupe", "dupe-01", "dupe"})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#BatchEntryIdsNotDistinct", err.Type)

	err = validateBatchEntryIDs([]string{"dupe-01", "dupe-02", ""})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidBatchEntryId", err.Type)

	err = validateBatchEntryIDs([]string{"dupe-01", "dupe-02", strings.Repeat("a", 81)})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidBatchEntryId", err.Type)

	err = validateBatchEntryIDs([]string{"dupe-01", "dupe-02", "dupe-03!!"})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidBatchEntryId", err.Type)

	err = validateBatchEntryIDs([]string{"dupe-01", "dupe-02", "dupe-03"})
	require.Nil(t, err)
}

func Test_validateRedriveAllowPolicy(t *testing.T) {
	var tooManyIDs []string
	for range 11 {
		tooManyIDs = append(tooManyIDs, "bad")
	}

	err := validateRedriveAllowPolicy(RedriveAllowPolicy{
		SourceQueueARNs: tooManyIDs,
	})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateRedriveAllowPolicy(RedriveAllowPolicy{
		SourceQueueARNs:   []string{"one", "two"},
		RedrivePermission: RedrivePermission("not-real"),
	})
	require.NotNil(t, err)
	require.Equal(t, http.StatusBadRequest, err.StatusCode)
	require.Equal(t, "com.amazonaws.sqs#InvalidParameterValueException", err.Type)

	err = validateRedriveAllowPolicy(RedriveAllowPolicy{
		SourceQueueARNs:   []string{"one", "two"},
		RedrivePermission: RedrivePermissionAllowAll,
	})
	require.Nil(t, err)
}

func Test_readAttributeDurationSeconds(t *testing.T) {
	value, err := readAttributeDurationSeconds(map[string]string{
		string(types.QueueAttributeNameDelaySeconds): "10",
	}, types.QueueAttributeNameDelaySeconds)
	require.Nil(t, err)
	require.True(t, value.IsSet)
	require.EqualValues(t, 10*time.Second, value.Value)

	value, err = readAttributeDurationSeconds(map[string]string{}, types.QueueAttributeNameDelaySeconds)
	require.Nil(t, err)
	require.False(t, value.IsSet)

	value, err = readAttributeDurationSeconds(map[string]string{
		string(types.QueueAttributeNameDelaySeconds): "deadbeef",
	}, types.QueueAttributeNameDelaySeconds)
	require.NotNil(t, err)
	require.False(t, value.IsSet)
}

func Test_readAttributeInt(t *testing.T) {
	value, err := readAttributeInt(map[string]string{
		string(types.QueueAttributeNameMaximumMessageSize): "1024",
	}, types.QueueAttributeNameMaximumMessageSize)
	require.Nil(t, err)
	require.True(t, value.IsSet)
	require.EqualValues(t, 1024, value.Value)

	value, err = readAttributeInt(map[string]string{}, types.QueueAttributeNameMaximumMessageSize)
	require.Nil(t, err)
	require.False(t, value.IsSet)

	value, err = readAttributeInt(map[string]string{
		string(types.QueueAttributeNameMaximumMessageSize): "deadbeef",
	}, types.QueueAttributeNameMaximumMessageSize)
	require.NotNil(t, err)
	require.False(t, value.IsSet)
}

func Test_readAttributeRedrivePolicy(t *testing.T) {
	value, err := readAttributeRedrivePolicy(map[string]string{
		string(types.QueueAttributeNameRedrivePolicy): `{"deadLetterTargetArn":"good","maxReceiveCount":10}`,
	})
	require.Nil(t, err)
	require.True(t, value.IsSet)
	require.EqualValues(t, 10, value.Value.MaxReceiveCount)

	value, err = readAttributeRedrivePolicy(map[string]string{})
	require.Nil(t, err)
	require.False(t, value.IsSet)

	value, err = readAttributeRedrivePolicy(map[string]string{
		string(types.QueueAttributeNameRedrivePolicy): "deadbeef",
	})
	require.NotNil(t, err)
	require.False(t, value.IsSet)
}

func Test_readAttributeRedriveAllowPolicy(t *testing.T) {
	value, err := readAttributeRedriveAllowPolicy(map[string]string{
		string(types.QueueAttributeNameRedriveAllowPolicy): `{"redrivePermission":"AllowAll","sourceQueueArns":["one","two"]}`,
	})
	require.Nil(t, err)
	require.True(t, value.IsSet)
	require.EqualValues(t, "AllowAll", value.Value.RedrivePermission)
	require.EqualValues(t, []string{"one", "two"}, value.Value.SourceQueueARNs)

	value, err = readAttributeRedriveAllowPolicy(map[string]string{})
	require.Nil(t, err)
	require.False(t, value.IsSet)

	value, err = readAttributeRedriveAllowPolicy(map[string]string{
		string(types.QueueAttributeNameRedriveAllowPolicy): "deadbeef",
	})
	require.NotNil(t, err)
	require.False(t, value.IsSet)
}

func Test_readAttributePolicy(t *testing.T) {
	value, err := readAttributePolicy(map[string]string{
		string(types.QueueAttributeNamePolicy): `{}`,
	})
	require.Nil(t, err)
	require.True(t, value.IsSet)

	value, err = readAttributePolicy(map[string]string{})
	require.Nil(t, err)
	require.False(t, value.IsSet)

	value, err = readAttributePolicy(map[string]string{
		string(types.QueueAttributeNamePolicy): "deadbeef",
	})
	require.NotNil(t, err)
	require.False(t, value.IsSet)
}
