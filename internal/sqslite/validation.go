package sqslite

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var validQueueNameRegexp = regexp.MustCompile("^[0-9,a-z,A-Z,_,-]+$")

func validateQueueName(queueName string) *Error {
	if queueName == "" {
		return ErrorInvalidParameterValueException().WithMessage("Queue name cannot be empty")
	}
	if len(queueName) > 80 {
		return ErrorInvalidParameterValueException().WithMessage("Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length")
	}
	if !validQueueNameRegexp.MatchString(queueName) {
		return ErrorInvalidParameterValueException().WithMessage("Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length")
	}
	return nil
}

func validateDelay(delay time.Duration) *Error {
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html
	if delay < 0 {
		return ErrorInvalidParameterValueException().WithMessagef("DelaySeconds must be greater than or equal to 0, you put: %v", delay)
	}
	if delay > 900*time.Second { // 15 minutes
		return ErrorInvalidParameterValueException().WithMessagef("DelaySeconds must be less than or equal to 90 seconds, you put: %v", delay)
	}
	return nil
}

func validateMaximumMessageSizeBytes(maximumMessageSizeBytes int) *Error {
	if maximumMessageSizeBytes < 1024 {
		return ErrorInvalidParameterValueException().WithMessagef("MaximumMessageSizeBytes must be greater than or equal to 1024, you put: %v", maximumMessageSizeBytes)
	}
	if maximumMessageSizeBytes > 256*1024 {
		return ErrorInvalidParameterValueException().WithMessagef("MaximumMessageSizeBytes must be less than or equal to 256KiB, you put: %v", maximumMessageSizeBytes)
	}
	return nil
}

func validateMessageRetentionPeriod(messageRetentionPeriod time.Duration) *Error {
	if messageRetentionPeriod < 60*time.Second {
		return ErrorInvalidParameterValueException().WithMessagef("MessageRetentionPeriod must be greater than or equal to 60 seconds, you put: %v", messageRetentionPeriod)
	}
	if messageRetentionPeriod > 14*24*time.Hour {
		return ErrorInvalidParameterValueException().WithMessagef("MessageRetentionPeriod must be less than or equal to 14 days, you put: %v", messageRetentionPeriod)
	}
	return nil
}

func validateReceiveMessageWaitTime(receiveMessageWaitTime time.Duration) *Error {
	if receiveMessageWaitTime < 0 {
		return ErrorInvalidParameterValueException().WithMessagef("ReceiveMessageWaitTime must be greater than or equal to 0, you put: %v", receiveMessageWaitTime)
	}
	if receiveMessageWaitTime > 20*time.Second {
		return ErrorInvalidParameterValueException().WithMessagef("ReceiveMessageWaitTime must be less than or equal to 20 seconds, you put: %v", receiveMessageWaitTime)
	}
	return nil
}

func validateWaitTimeSeconds(waitTime time.Duration) *Error {
	if waitTime < 0 {
		return ErrorInvalidParameterValueException().WithMessagef("WaitTimeSeconds must be greater than or equal to 0, you put: %v", waitTime)
	}
	if waitTime > 20*time.Second {
		return ErrorInvalidParameterValueException().WithMessagef("WaitTimeSeconds must be less than or equal to 20 seconds, you put: %v", waitTime)
	}
	return nil
}

func validateVisibilityTimeout(visibilityTimeout time.Duration) *Error {
	if visibilityTimeout < 0 {
		return ErrorInvalidParameterValueException().WithMessagef("VisibilityTimeout must be greater than or equal to 0, you put: %v", visibilityTimeout)
	}
	if visibilityTimeout > 12*time.Hour {
		return ErrorInvalidParameterValueException().WithMessagef("VisibilityTimeout must be less than or equal to 12 hours, you put: %v", visibilityTimeout)
	}
	return nil
}

func validateRedrivePolicy(redrivePolicy RedrivePolicy) *Error {
	if redrivePolicy.MaxReceiveCount < 0 || redrivePolicy.MaxReceiveCount > 1000 {
		return ErrorInvalidParameterValueException().WithMessagef("%s for parameter RedrivePolicy is invalid; Reason: Invalid value for maxReceiveCount: %d, valid values are from 1 to 1000 both inclusive.", marshalJSON(redrivePolicy), redrivePolicy.MaxReceiveCount)
	}
	return nil
}

func validateMessageBody(body *string, maximumMessageSizeBytes int) *Error {
	if body == nil || *body == "" {
		return ErrorInvalidParameterValueException().WithMessage("One or more parameters are invalid. Reason: Message must be at least one character.")
	}
	if len([]byte(*body)) > maximumMessageSizeBytes {
		return ErrorInvalidParameterValueException().WithMessage(fmt.Sprintf("One or more parameters are invalid. Reason: Message must be shorter than %d bytes.", maximumMessageSizeBytes))
	}
	if !utf8.ValidString(*body) {
		return ErrorInvalidParameterValueException().WithMessagef("Message body must contain at least one valid character")
	}
	return nil
}

func validateBatchEntryIDs(entryIDs []string) *Error {
	if len(distinct(entryIDs)) != len(entryIDs) {
		return ErrorBatchEntryIdsNotDistinct()
	}
	for _, id := range entryIDs {
		if err := validateBatchEntryID(id); err != nil {
			return err
		}
	}
	return nil
}

var validBatchEntryIDRegexp = regexp.MustCompile("^[0-9,a-z,A-Z,_,-]+$")

func validateBatchEntryID(entryID string) *Error {
	if len(entryID) == 0 {
		return ErrorInvalidBatchEntryID().WithMessage("Id must be non-empty")
	}
	if len(entryID) > 80 {
		return ErrorInvalidBatchEntryID().WithMessagef("Id must be fewer than 80 characters; you put %d", len(entryID))
	}
	if !validBatchEntryIDRegexp.MatchString(entryID) {
		return ErrorInvalidBatchEntryID().WithMessagef("Id must be a valid string; regexp used %q", validBatchEntryIDRegexp.String())
	}
	return nil
}

func validateRedriveAllowPolicy(redriveAllowPolicy RedriveAllowPolicy) *Error {
	if len(redriveAllowPolicy.SourceQueueARNs) > 10 {
		return ErrorInvalidParameterValueException().WithMessagef("sourceQueueARNs must have fewer than 10 elements; you put %d", len(redriveAllowPolicy.SourceQueueARNs))
	}
	switch redriveAllowPolicy.RedrivePermission {
	case RedrivePermissionAllowAll, RedrivePermissionDenyAll, RedrivePermissionByQueue:
		return nil
	default:
		return ErrorInvalidParameterValueException().WithMessagef("redrivePermission invalid")
	}
}

func readAttributeDurationSeconds(attributes map[string]string, attributeName types.QueueAttributeName) (output Optional[time.Duration], err *Error) {
	value, ok := attributes[string(attributeName)]
	if !ok {
		return
	}
	parsed, parseErr := strconv.Atoi(value)
	if parseErr != nil {
		err = ErrorInvalidAttributeValue().WithMessagef("%s failed to parse as duration seconds: %v", attributeName, parseErr)
		return
	}
	output = Some(time.Duration(parsed) * time.Second)
	return
}

func readAttributeInt(attributes map[string]string, attributeName types.QueueAttributeName) (output Optional[int], err *Error) {
	value, ok := attributes[string(attributeName)]
	if !ok {
		return
	}
	parsed, parseErr := strconv.Atoi(value)
	if parseErr != nil {
		err = ErrorInvalidAttributeValue().WithMessagef("%s failed to parse as integer: %v", attributeName, parseErr)
		return
	}
	output = Some(parsed)
	return
}

func readAttributeRedrivePolicy(attributes map[string]string) (output Optional[RedrivePolicy], err *Error) {
	value, ok := attributes[string(types.QueueAttributeNameRedrivePolicy)]
	if !ok {
		return
	}
	var policy RedrivePolicy
	if jsonErr := json.Unmarshal([]byte(value), &policy); jsonErr != nil {
		err = ErrorInvalidAttributeValue().WithMessagef("%s failed to parse redrive policy: %v", string(types.QueueAttributeNameRedrivePolicy), jsonErr)
		return
	}
	output = Some(policy)
	return
}

func readAttributeRedriveAllowPolicy(attributes map[string]string) (output Optional[RedriveAllowPolicy], err *Error) {
	value, ok := attributes[string(types.QueueAttributeNameRedriveAllowPolicy)]
	if !ok {
		return
	}
	var policy RedriveAllowPolicy
	if jsonErr := json.Unmarshal([]byte(value), &policy); jsonErr != nil {
		err = ErrorInvalidAttributeValue().WithMessagef("%s failed to parse redrive allow policy: %v", string(types.QueueAttributeNameRedriveAllowPolicy), jsonErr)
		return
	}
	output = Some(policy)
	return
}

func readAttributePolicy(attributes map[string]string) (output Optional[any], err *Error) {
	value, ok := attributes[string(types.QueueAttributeNamePolicy)]
	if !ok {
		return
	}
	var policy any
	if jsonErr := json.Unmarshal([]byte(value), &policy); jsonErr != nil {
		err = ErrorInvalidAttributeValue().WithMessagef("%s failed to parse policy: %v", string(types.QueueAttributeNamePolicy), jsonErr)
		return
	}
	output = Some(policy)
	return
}
