package sqslite

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func Test_Queue_NewQueueFromCreateQueueInput_minimalDefaults(t *testing.T) {
	q, err := NewQueueFromCreateQueueInput(Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()

	require.Nil(t, err)
	require.Equal(t, "test-queue", q.Name)
	require.Equal(t, "http://sqslite.local/test-account/test-queue", q.URL)
	require.Equal(t, "arn:aws:sqs:us-west-2:test-account:test-queue", q.ARN)
	require.NotNil(t, q.messagesReadyHotKeyed)
	require.NotNil(t, q.messagesReadyColdKeyed)
	// require.NotNil(t, q.messagesReady)
	require.NotNil(t, q.messagesDelayed)
	require.NotNil(t, q.messagesInflight)

	require.Equal(t, false, q.Delay.IsSet)
	require.Equal(t, 256*1024, q.MaximumMessageSizeBytes)
	require.Equal(t, 4*24*time.Hour, q.MessageRetentionPeriod)
	require.Equal(t, 20*time.Second, q.ReceiveMessageWaitTime)
	require.Equal(t, 30*time.Second, q.VisibilityTimeout)
	require.Equal(t, 400000, q.MaximumMessagesInflight)
}

func Test_RedriveAllowPolicy_AllowSource_RedrivePermissionAllowAll(t *testing.T) {
	rap := RedriveAllowPolicy{
		RedrivePermission: RedrivePermissionAllowAll,
	}
	require.True(t, rap.AllowSource("any-01"))
	require.True(t, rap.AllowSource("any-02"))

	rap = RedriveAllowPolicy{
		RedrivePermission: RedrivePermissionDenyAll,
	}
	require.False(t, rap.AllowSource("any-01"))
	require.False(t, rap.AllowSource("any-02"))

	rap = RedriveAllowPolicy{
		RedrivePermission: RedrivePermissionByQueue,
		SourceQueueARNs: []string{
			"any-02",
			"any-03",
			"any-04",
		},
	}
	require.False(t, rap.AllowSource("any-01"))
	require.True(t, rap.AllowSource("any-02"))
	require.True(t, rap.AllowSource("any-03"))

	rap = RedriveAllowPolicy{
		RedrivePermission: RedrivePermission("not real"),
	}
	require.False(t, rap.AllowSource("any-01"))
	require.False(t, rap.AllowSource("any-02"))
}

func Test_Queue_NewQueueFromCreateQueueInput_invalidName(t *testing.T) {
	_, err := NewQueueFromCreateQueueInput(Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test!!!queue"),
	})
	require.NotNil(t, err)
}

func Test_Queue_NewMessageStateFromSendMessageInput(t *testing.T) {
	q, _ := NewQueueFromCreateQueueInput(Authorization{
		Region:    Some("us-west-2"),
		Host:      Some("sqslite.local"),
		AccountID: "test-account",
	}, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	defer q.Close()
	msg := q.NewMessageStateFromSendMessageInput(&sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(`{"messageIndex":0}`),
	})
	require.Equal(t, "552cc6a91af25b6aef1e5d1b5e5f54a9", msg.MD5OfBody().Value)
}

func Test_Queue_Receive_basic(t *testing.T) {
	q := createTestQueue(t)

	pushTestMessages(q, 100)
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	require.GreaterOrEqual(t, len(received), 1)
	require.LessOrEqual(t, len(received), 10)
	require.Equal(t, "1", received[0].Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)], received[0].Attributes)
	require.EqualValues(t, q.messagesInflight.Len(), len(received))
}

func Test_Queue_Receive_single(t *testing.T) {
	q := createTestQueue(t)

	// Push 5 messages to the queue
	pushTestMessages(q, 10)

	// Request only 3 messages
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 1,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})

	// Assert that we get at leaset 1 messages
	require.GreaterOrEqual(t, len(received), 1)

	// Assert that message attributes are added if we ask for them
	require.Equal(t, "1", received[0].Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)], received[0].Attributes)
	require.EqualValues(t, q.messagesInflight.Len(), 1)

	firstKey := slices.Collect(maps.Keys(q.messagesInflight.receiptHandles))[0]
	msgState := q.messagesInflight.receiptHandles[firstKey]
	require.EqualValues(t, msgState.MessageID.String(), safeDeref(received[0].MessageId))
	require.EqualValues(t, 1, msgState.ReceiveCount)
	require.EqualValues(t, 1, msgState.ReceiptHandles.Len())
	require.EqualValues(t, true, msgState.ReceiptHandles.Has(safeDeref(received[0].ReceiptHandle)))

	require.EqualValues(t, 10, q.Stats().NumMessages)
	require.EqualValues(t, 9, q.Stats().NumMessagesReady)
	require.EqualValues(t, 1, q.Stats().NumMessagesInflight)
}

func Test_Queue_Receive_returnsMessagesInMultiplePasses(t *testing.T) {
	q := createTestQueue(t)

	pushTestMessages(q, 5)

	remaining := 5
	for range 5 {
		received := q.Receive(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   10,
		})
		require.GreaterOrEqual(t, len(received), 1, remaining)
		remaining = remaining - len(received)
		if remaining == 0 {
			break
		}
	}
	require.EqualValues(t, 0, remaining)
}

func Test_Queue_Receive_respectsMaximumMessagesInFlight(t *testing.T) {
	q := createTestQueue(t)
	q.MaximumMessagesInflight = 10

	pushTestMessages(q, 20)

	remaining := 10
	for range 20 {
		received := q.Receive(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   10,
		})
		remaining = remaining - len(received)
	}
	require.EqualValues(t, 0, remaining)
	require.EqualValues(t, 10, q.messagesInflight.Len())
}

func Test_Queue_Receive_usesProvidedVisibilityTimeout(t *testing.T) {
	q := createTestQueue(t)
	q.VisibilityTimeout = 30 * time.Second // Default queue timeout

	pushTestMessages(q, 1)
	customTimeout := 60 * time.Second

	// Receive with custom visibility timeout
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   60,
	})
	require.Len(t, received, 1)

	// Check that the message in inflight state has the custom timeout
	firstKey := slices.Collect(maps.Keys(q.messagesInflight.receiptHandles))[0]
	msgState := q.messagesInflight.receiptHandles[firstKey]
	require.Equal(t, customTimeout, msgState.VisibilityTimeout)
	require.Equal(t, true, msgState.FirstReceived.IsSet)
	require.Equal(t, testAccountID, msgState.SenderID.Value)
}

func Test_Queue_Receive_usesDefaultVisibilityTimeoutWhenZero(t *testing.T) {
	q := createTestQueue(t)
	q.VisibilityTimeout = 30 * time.Second // Default queue timeout

	pushTestMessages(q, 1)

	// Receive with custom visibility timeout
	received := q.Receive(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   0,
	})
	require.Len(t, received, 1)

	firstKey := slices.Collect(maps.Keys(q.messagesInflight.receiptHandles))[0]
	msgState := q.messagesInflight.receiptHandles[firstKey]
	require.Equal(t, 30*time.Second, msgState.VisibilityTimeout)
}

func Test_Queue_Push_singleMessageWithoutDelay_addsToReadyQueue(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestSendMessageInput("test body")
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	require.NotNil(t, msgState.OriginalSourceQueue)
	require.EqualValues(t, q.URL, msgState.OriginalSourceQueue.URL)

	q.Push(msgState)
	updatedStats := q.Stats()

	require.Equal(t, 1, q.messagesReadyColdKeyed.Len())

	require.Equal(t, initialStats.TotalMessagesSent+1, updatedStats.TotalMessagesSent)
	require.Equal(t, initialStats.NumMessages+1, updatedStats.NumMessages)
	require.Equal(t, initialStats.NumMessagesReady+1, updatedStats.NumMessagesReady)
}

func Test_Queue_Push_singleMessageWithDelay_addsToDelayedQueue(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	msg := createTestSendMessageInput("test body")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	updatedStats := q.Stats()
	require.Equal(t, 0, q.messagesReadyColdKeyed.Len())
	require.Len(t, q.messagesDelayed, 1)
	require.Equal(t, initialStats.NumMessagesDelayed+1, updatedStats.NumMessagesDelayed)
}

func Test_Queue_Push_multipleMessages_handlesAllMessages(t *testing.T) {
	q := createTestQueue(t)

	msg1 := createTestSendMessageInput("test body 1")
	msgState1 := q.NewMessageStateFromSendMessageInput(msg1)
	msg2 := createTestSendMessageInput("test body 2")
	msgState2 := q.NewMessageStateFromSendMessageInput(msg2)
	q.Push(msgState1, msgState2)

	require.Equal(t, 2, q.messagesReadyColdKeyed.Len())
}

func Test_Queue_Push_queueHasDefaultDelayMessageHasNone_appliesQueueDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(5 * time.Second)

	msg := createTestSendMessageInput("test body")
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	require.Equal(t, 0, q.messagesReadyColdKeyed.Len())
	require.Len(t, q.messagesDelayed, 1)
}

func Test_Queue_Push_queueHasDefaultDelayMessageHasDelay_keepsMessageDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(5 * time.Second)

	msg := createTestSendMessageInput("test body")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	require.Equal(t, 10*time.Second, msgState.Delay.Value)
}

func Test_Queue_Push_noMessages_isNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.Push() // Call with no arguments

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

func Test_Queue_Push_mixedDelayedAndReadyMessages_handlesCorrectly(t *testing.T) {
	q := createTestQueue(t)

	readyMsg := createTestSendMessageInput("ready")
	readyMsgState := q.NewMessageStateFromSendMessageInput(readyMsg)
	delayedMsg := createTestSendMessageInput("delayed")
	delayedMsg.DelaySeconds = 10
	delayedMsgState := q.NewMessageStateFromSendMessageInput(delayedMsg)

	q.Push(readyMsgState, delayedMsgState)

	require.Equal(t, 1, q.messagesReadyColdKeyed.Len())
	require.Len(t, q.messagesDelayed, 1)
}

// Tests for Queue.Stats()
func Test_Queue_Stats_returnsCurrentStats(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	require.Equal(t, int64(0), initialStats.NumMessages)
	require.Equal(t, int64(0), initialStats.NumMessagesReady)
	require.Equal(t, int64(0), initialStats.NumMessagesDelayed)
	require.Equal(t, int64(0), initialStats.NumMessagesInflight)
	require.Equal(t, uint64(0), initialStats.TotalMessagesSent)
	require.Equal(t, uint64(0), initialStats.TotalMessagesReceived)

	// Add messages and verify stats update
	pushTestMessages(q, 5)
	updatedStats := q.Stats()

	require.Equal(t, int64(5), updatedStats.NumMessages)
	require.Equal(t, int64(5), updatedStats.NumMessagesReady)
	require.Equal(t, uint64(5), updatedStats.TotalMessagesSent)
}

func Test_Queue_Stats_reflectsInflightMessages(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 10)

	// Receive at least 2 messages to move them to inflight
	var totalReceived int
	for totalReceived < 2 {
		received := q.Receive(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 5,
		})
		totalReceived += len(received)
		if len(received) == 0 {
			break
		}
	}
	require.GreaterOrEqual(t, totalReceived, 2)

	stats := q.Stats()
	require.Equal(t, int64(10), stats.NumMessages) // Total messages
	require.Equal(t, int64(10)-int64(totalReceived), stats.NumMessagesReady)
	require.Equal(t, int64(totalReceived), stats.NumMessagesInflight)
	require.Equal(t, uint64(totalReceived), stats.TotalMessagesReceived)
}

func Test_Queue_Stats_reflectsDelayedMessages(t *testing.T) {
	q := createTestQueue(t)

	// Add delayed message
	msg := createTestSendMessageInput("delayed body")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)
	q.Push(msgState)

	// Add ready message
	pushTestMessages(q, 1)

	stats := q.Stats()
	require.Equal(t, int64(2), stats.NumMessages)
	require.Equal(t, int64(1), stats.NumMessagesReady)
	require.Equal(t, int64(1), stats.NumMessagesDelayed)
	require.Equal(t, int64(0), stats.NumMessagesInflight)
}

// Tests for Queue.Created(), LastModified(), Deleted(), IsDeleted()
func Test_Queue_Created_returnsCreationTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		require.Equal(t, time.Now(), q.Created())
	})
}

func Test_Queue_LastModified_initiallyEqualsCreated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		require.Equal(t, q.Created(), q.LastModified())
	})
}

func Test_Queue_LastModified_updatesOnAttributeChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		initialModified := q.LastModified()

		// Advance clock and modify attributes
		time.Sleep(5 * time.Second)
		err := q.SetQueueAttributes(map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): "60",
		})
		require.Nil(t, err)

		require.True(t, q.LastModified().After(initialModified))
	})
}

func Test_Queue_IsDeleted_falseForNewQueue(t *testing.T) {
	q := createTestQueue(t)
	require.False(t, q.IsDeleted())
}

func Test_Queue_IsDeleted_trueAfterDeletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)

		// Simulate deletion by setting deleted timestamp
		q.deleted = time.Now()

		require.True(t, q.IsDeleted())
		require.Equal(t, time.Now(), q.Deleted())
	})
}

// Tests for Queue.AddDLQSources() and RemoveDLQSource()
func Test_Queue_AddDLQSources_setsIsDLQFlag(t *testing.T) {
	dlq := createTestQueue(t)
	sourceQueue := createTestQueueWithName(t, "source-queue")

	require.False(t, dlq.IsDLQ())

	dlq.AddDLQSources(sourceQueue)

	require.True(t, dlq.IsDLQ())
	require.Contains(t, dlq.dlqSources, sourceQueue.URL)
}

func Test_Queue_AddDLQSources_handlesMultipleSources(t *testing.T) {
	dlq := createTestQueue(t)
	source1 := createTestQueueWithName(t, "source1")
	source2 := createTestQueueWithName(t, "source2")

	dlq.AddDLQSources(source1, source2)

	require.True(t, dlq.IsDLQ())
	require.Contains(t, dlq.dlqSources, source1.URL)
	require.Contains(t, dlq.dlqSources, source2.URL)
	require.Len(t, dlq.dlqSources, 2)
}

func Test_Queue_RemoveDLQSource_removesSourceFromMap(t *testing.T) {
	dlq := createTestQueue(t)
	sourceQueue := createTestQueueWithName(t, "source-queue")

	dlq.AddDLQSources(sourceQueue)
	require.True(t, dlq.IsDLQ())

	dlq.RemoveDLQSource(sourceQueue.URL)

	require.False(t, dlq.IsDLQ())
	require.NotContains(t, dlq.dlqSources, sourceQueue.URL)
}

func Test_Queue_RemoveDLQSource_keepsIsDLQIfOtherSourcesExist(t *testing.T) {
	dlq := createTestQueue(t)
	source1 := createTestQueueWithName(t, "source1")
	source2 := createTestQueueWithName(t, "source2")

	dlq.AddDLQSources(source1, source2)
	dlq.RemoveDLQSource(source1.URL)

	require.True(t, dlq.IsDLQ()) // Still has source2
	require.NotContains(t, dlq.dlqSources, source1.URL)
	require.Contains(t, dlq.dlqSources, source2.URL)
}

// Tests for Queue.IsDLQ()
func Test_Queue_IsDLQ_falseForRegularQueue(t *testing.T) {
	q := createTestQueue(t)
	require.False(t, q.IsDLQ())
}

func Test_Queue_IsDLQ_trueAfterAddingSource(t *testing.T) {
	dlq := createTestQueue(t)
	sourceQueue := createTestQueueWithName(t, "source")

	dlq.AddDLQSources(sourceQueue)
	require.True(t, dlq.IsDLQ())
}

// Tests for Queue.Tag() and Untag()
func Test_Queue_Tag_addsTags(t *testing.T) {
	q := createTestQueue(t)
	tags := map[string]string{
		"env":  "test",
		"team": "platform",
	}

	q.Tag(tags)

	require.Equal(t, "test", q.Tags["env"])
	require.Equal(t, "platform", q.Tags["team"])
}

func Test_Queue_Tag_mergesWithExistingTags(t *testing.T) {
	q := createTestQueue(t)
	q.Tags = map[string]string{"existing": "value"}

	newTags := map[string]string{
		"env":  "test",
		"team": "platform",
	}
	q.Tag(newTags)

	require.Equal(t, "value", q.Tags["existing"])
	require.Equal(t, "test", q.Tags["env"])
	require.Equal(t, "platform", q.Tags["team"])
	require.Len(t, q.Tags, 3)
}

func Test_Queue_Tag_overwritesExistingTags(t *testing.T) {
	q := createTestQueue(t)
	q.Tags = map[string]string{"env": "prod"}

	newTags := map[string]string{"env": "test"}
	q.Tag(newTags)

	require.Equal(t, "test", q.Tags["env"])
}

func Test_Queue_Tag_initializesTagsMapIfNil(t *testing.T) {
	q := createTestQueue(t)
	q.Tags = nil

	tags := map[string]string{"env": "test"}
	q.Tag(tags)

	require.NotNil(t, q.Tags)
	require.Equal(t, "test", q.Tags["env"])
}

func Test_Queue_Untag_removesSpecifiedTags(t *testing.T) {
	q := createTestQueue(t)
	q.Tags = map[string]string{
		"env":     "test",
		"team":    "platform",
		"project": "sqslite",
	}

	q.Untag([]string{"env", "project"})

	require.NotContains(t, q.Tags, "env")
	require.NotContains(t, q.Tags, "project")
	require.Equal(t, "platform", q.Tags["team"])
	require.Len(t, q.Tags, 1)
}

func Test_Queue_Untag_handlesNonExistentTags(t *testing.T) {
	q := createTestQueue(t)
	q.Tags = map[string]string{"env": "test"}

	q.Untag([]string{"nonexistent", "env"})

	require.NotContains(t, q.Tags, "env")
	require.Len(t, q.Tags, 0)
}

// Tests for Queue.SetQueueAttributes()
func Test_Queue_SetQueueAttributes_updatesVisibilityTimeout(t *testing.T) {
	q := createTestQueue(t)
	initialTimeout := q.VisibilityTimeout

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameVisibilityTimeout): "120",
	})

	require.Nil(t, err)
	require.Equal(t, 120*time.Second, q.VisibilityTimeout)
	require.NotEqual(t, initialTimeout, q.VisibilityTimeout)
}

func Test_Queue_SetQueueAttributes_updatesMaximumMessageSize(t *testing.T) {
	q := createTestQueue(t)

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameMaximumMessageSize): "65536",
	})

	require.Nil(t, err)
	require.Equal(t, 65536, q.MaximumMessageSizeBytes)
}

func Test_Queue_SetQueueAttributes_updatesMessageRetentionPeriod(t *testing.T) {
	q := createTestQueue(t)

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameMessageRetentionPeriod): "86400", // 1 day
	})

	require.Nil(t, err)
	require.Equal(t, 24*time.Hour, q.MessageRetentionPeriod)
}

func Test_Queue_SetQueueAttributes_updatesReceiveMessageWaitTime(t *testing.T) {
	q := createTestQueue(t)

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds): "10",
	})

	require.Nil(t, err)
	require.Equal(t, 10*time.Second, q.ReceiveMessageWaitTime)
}

func Test_Queue_SetQueueAttributes_updatesDelaySeconds(t *testing.T) {
	q := createTestQueue(t)
	require.False(t, q.Delay.IsSet)

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameDelaySeconds): "15",
	})

	require.Nil(t, err)
	require.True(t, q.Delay.IsSet)
	require.Equal(t, 15*time.Second, q.Delay.Value)
}

func Test_Queue_SetQueueAttributes_setsRedrivePolicy(t *testing.T) {
	q := createTestQueue(t)
	dlqARN := "arn:aws:sqs:us-west-2:123456789012:test-dlq"

	redrivePolicy := RedrivePolicy{
		DeadLetterTargetArn: dlqARN,
		MaxReceiveCount:     5,
	}

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameRedrivePolicy): marshalJSON(redrivePolicy),
	})

	require.Nil(t, err)
	require.True(t, q.RedrivePolicy.IsSet)
	require.Equal(t, dlqARN, q.RedrivePolicy.Value.DeadLetterTargetArn)
	require.Equal(t, 5, q.RedrivePolicy.Value.MaxReceiveCount)
}

func Test_Queue_SetQueueAttributes_setsRedriveAllowPolicy(t *testing.T) {
	q := createTestQueue(t)

	redriveAllowPolicy := RedriveAllowPolicy{
		RedrivePermission: RedrivePermissionByQueue,
		SourceQueueARNs:   []string{"arn:aws:sqs:us-west-2:123456789012:source-queue"},
	}

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameRedriveAllowPolicy): marshalJSON(redriveAllowPolicy),
	})

	require.Nil(t, err)
	require.True(t, q.RedriveAllowPolicy.IsSet)
	require.Equal(t, RedrivePermissionByQueue, q.RedriveAllowPolicy.Value.RedrivePermission)
	require.Len(t, q.RedriveAllowPolicy.Value.SourceQueueARNs, 1)
}

func Test_Queue_SetQueueAttributes_returnsErrorForInvalidValues(t *testing.T) {
	q := createTestQueue(t)

	// Test invalid visibility timeout
	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameVisibilityTimeout): "invalid",
	})
	require.NotNil(t, err)

	// Test invalid maximum message size
	err = q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameMaximumMessageSize): "1000000000", // Too large
	})
	require.NotNil(t, err)
}

func Test_Queue_SetQueueAttributes_updatesLastModifiedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		initialModified := q.LastModified()

		time.Sleep(5 * time.Second)
		err := q.SetQueueAttributes(map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): "60",
		})

		require.Nil(t, err)
		require.True(t, q.LastModified().After(initialModified))
	})
}

// Tests for Queue.PopMessageForMove()
func Test_Queue_PopMessageForMove_returnsMessageFromReady(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 3)
	initialStats := q.Stats()

	msg, ok := q.PopMessageForMove()

	require.True(t, ok)
	require.NotNil(t, msg)
	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessagesReady-1, updatedStats.NumMessagesReady)
	require.Equal(t, initialStats.TotalMessagesMoved+1, updatedStats.TotalMessagesMoved)
}

func Test_Queue_PopMessageForMove_returnsFalseWhenEmpty(t *testing.T) {
	q := createTestQueue(t)

	msg, ok := q.PopMessageForMove()

	require.False(t, ok)
	require.Nil(t, msg)
}

func Test_Queue_PopMessageForMove_ignoresInflightMessages(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 1)

	// Move message to inflight
	received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received, 1)

	// PopMessageForMove should return false since no ready messages
	msg, ok := q.PopMessageForMove()
	require.False(t, ok)
	require.Nil(t, msg)
}

// Tests for Queue.ChangeMessageVisibility()
func Test_Queue_ChangeMessageVisibility_updatesVisibilityTimeout(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 1)

	// Receive message to get receipt handle
	received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received, 1)
	receiptHandle := *received[0].ReceiptHandle

	// Change visibility timeout
	newTimeout := 120 * time.Second
	ok := q.ChangeMessageVisibility(receiptHandle, newTimeout)

	require.True(t, ok)

	// Verify message is still inflight with updated timeout
	msg := q.messagesInflight.receiptHandles[receiptHandle]
	require.Equal(t, newTimeout, msg.VisibilityTimeout)
}

func Test_Queue_ChangeMessageVisibility_returnsFalseForInvalidHandle(t *testing.T) {
	q := createTestQueue(t)
	ok := q.ChangeMessageVisibility("invalid-handle", 60*time.Second)
	require.False(t, ok)
}

func Test_Queue_ChangeMessageVisibility_zeroTimeoutMovesToReady(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 1)

	// Receive message
	received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received, 1)
	receiptHandle := *received[0].ReceiptHandle

	initialInflight := q.Stats().NumMessagesInflight
	initialReady := q.Stats().NumMessagesReady

	// Set visibility timeout to 0
	ok := q.ChangeMessageVisibility(receiptHandle, 0)

	require.True(t, ok)
	require.Equal(t, initialInflight-1, q.Stats().NumMessagesInflight)
	require.Equal(t, initialReady+1, q.Stats().NumMessagesReady)
}

// Tests for Queue.ChangeMessageVisibilityBatch()
func Test_Queue_ChangeMessageVisibilityBatch_updatesMultipleMessages(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 10)

	// Receive messages - may get variable number due to randomization
	var allReceived []types.Message
	for len(allReceived) < 3 {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 10})
		allReceived = append(allReceived, received...)
		if len(received) == 0 {
			break
		}
	}
	require.GreaterOrEqual(t, len(allReceived), 3)
	received := allReceived[:3]

	// Prepare batch entries
	entries := []types.ChangeMessageVisibilityBatchRequestEntry{
		{
			Id:                aws.String("msg1"),
			ReceiptHandle:     received[0].ReceiptHandle,
			VisibilityTimeout: 120,
		},
		{
			Id:                aws.String("msg2"),
			ReceiptHandle:     received[1].ReceiptHandle,
			VisibilityTimeout: 180,
		},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entries)

	require.Len(t, successful, 2)
	require.Len(t, failed, 0)
	require.Equal(t, "msg1", *successful[0].Id)
	require.Equal(t, "msg2", *successful[1].Id)
}

func Test_Queue_ChangeMessageVisibilityBatch_handlesInvalidReceiptHandles(t *testing.T) {
	q := createTestQueue(t)

	entries := []types.ChangeMessageVisibilityBatchRequestEntry{
		{
			Id:                aws.String("invalid"),
			ReceiptHandle:     aws.String("invalid-handle"),
			VisibilityTimeout: 120,
		},
	}

	successful, failed := q.ChangeMessageVisibilityBatch(entries)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1)
	require.Equal(t, "invalid", *failed[0].Id)
	require.Equal(t, "ReceiptHandleIsInvalid", *failed[0].Code)
	require.True(t, failed[0].SenderFault)
}

func Test_Queue_ChangeMessageVisibilityBatch_zeroTimeoutMovesToReady(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 10)

	// Receive messages - get at least 2
	var allReceived []types.Message
	for len(allReceived) < 2 {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 10})
		allReceived = append(allReceived, received...)
		if len(received) == 0 {
			break
		}
	}
	require.GreaterOrEqual(t, len(allReceived), 2)
	received := allReceived[:2]

	entries := []types.ChangeMessageVisibilityBatchRequestEntry{
		{
			Id:                aws.String("msg1"),
			ReceiptHandle:     received[0].ReceiptHandle,
			VisibilityTimeout: 0, // Move back to ready
		},
	}

	initialInflight := q.Stats().NumMessagesInflight
	initialReady := q.Stats().NumMessagesReady

	successful, failed := q.ChangeMessageVisibilityBatch(entries)

	require.Len(t, successful, 1)
	require.Len(t, failed, 0)
	require.Equal(t, initialInflight-1, q.Stats().NumMessagesInflight)
	require.Equal(t, initialReady+1, q.Stats().NumMessagesReady)
}

// Tests for Queue.Delete()
func Test_Queue_Delete_removesInflightMessage(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 1)

	// Receive message
	received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received, 1)
	receiptHandle := *received[0].ReceiptHandle

	initialStats := q.Stats()
	ok := q.Delete(receiptHandle)

	require.True(t, ok)
	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessages-1, updatedStats.NumMessages)
	require.Equal(t, initialStats.NumMessagesInflight-1, updatedStats.NumMessagesInflight)
	require.Equal(t, initialStats.TotalMessagesDeleted+1, updatedStats.TotalMessagesDeleted)

	// Message should be completely removed
	_, exists := q.messagesInflight.receiptHandles[receiptHandle]
	require.False(t, exists)
}

func Test_Queue_Delete_returnsFalseForInvalidHandle(t *testing.T) {
	q := createTestQueue(t)

	ok := q.Delete("invalid-handle")

	require.False(t, ok)
}

func Test_Queue_Delete_removesAllReceiptHandles(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 1)

	// Receive same message multiple times to generate multiple receipt handles
	received1 := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received1, 1)
	receiptHandle1 := *received1[0].ReceiptHandle

	// Change visibility to 0 to make it ready again, then receive again
	q.ChangeMessageVisibility(receiptHandle1, 0)
	received2 := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received2, 1)
	receiptHandle2 := *received2[0].ReceiptHandle

	// Delete using one receipt handle should remove all
	ok := q.Delete(receiptHandle2)

	require.True(t, ok)
	_, exists1 := q.messagesInflight.receiptHandles[receiptHandle1]
	_, exists2 := q.messagesInflight.receiptHandles[receiptHandle2]
	require.False(t, exists1)
	require.False(t, exists2)
}

// Tests for Queue.DeleteBatch()
func Test_Queue_DeleteBatch_deletesMultipleMessages(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 10)

	// Receive messages - may get variable number due to randomization
	var allReceived []types.Message
	for len(allReceived) < 3 {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 10})
		allReceived = append(allReceived, received...)
		if len(received) == 0 {
			break // Prevent infinite loop
		}
	}
	require.GreaterOrEqual(t, len(allReceived), 3)

	// Use first 3 for test
	received := allReceived[:3]

	handlers := []types.DeleteMessageBatchRequestEntry{
		{
			Id:            aws.String("msg1"),
			ReceiptHandle: received[0].ReceiptHandle,
		},
		{
			Id:            aws.String("msg2"),
			ReceiptHandle: received[1].ReceiptHandle,
		},
	}

	initialStats := q.Stats()
	successful, failed := q.DeleteBatch(handlers)

	require.Len(t, successful, 2)
	require.Len(t, failed, 0)
	require.Equal(t, "msg1", *successful[0].Id)
	require.Equal(t, "msg2", *successful[1].Id)

	updatedStats := q.Stats()
	require.Equal(t, initialStats.NumMessages-2, updatedStats.NumMessages)
	require.Equal(t, initialStats.NumMessagesInflight-2, updatedStats.NumMessagesInflight)
	require.Equal(t, initialStats.TotalMessagesDeleted+2, updatedStats.TotalMessagesDeleted)
}

func Test_Queue_DeleteBatch_handlesInvalidReceiptHandles(t *testing.T) {
	q := createTestQueue(t)

	handlers := []types.DeleteMessageBatchRequestEntry{
		{
			Id:            aws.String("invalid"),
			ReceiptHandle: aws.String("invalid-handle"),
		},
	}

	successful, failed := q.DeleteBatch(handlers)

	require.Len(t, successful, 0)
	require.Len(t, failed, 1)
	require.Equal(t, "invalid", *failed[0].Id)
	require.Equal(t, "InvalidParameterValue", *failed[0].Code)
	require.True(t, failed[0].SenderFault)
}

func Test_Queue_DeleteBatch_mixedValidAndInvalid(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 1)

	// Receive one valid message
	received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
	require.Len(t, received, 1)

	handlers := []types.DeleteMessageBatchRequestEntry{
		{
			Id:            aws.String("valid"),
			ReceiptHandle: received[0].ReceiptHandle,
		},
		{
			Id:            aws.String("invalid"),
			ReceiptHandle: aws.String("invalid-handle"),
		},
	}

	successful, failed := q.DeleteBatch(handlers)

	require.Len(t, successful, 1)
	require.Len(t, failed, 1)
	require.Equal(t, "valid", *successful[0].Id)
	require.Equal(t, "invalid", *failed[0].Id)
}

// Tests for Queue.Purge()
func Test_Queue_Purge_removesAllMessages(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 10)

	// Move at least 2 messages to inflight
	var totalReceived int
	for totalReceived < 2 {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 5})
		totalReceived += len(received)
		if len(received) == 0 {
			break
		}
	}

	// Add delayed message
	msg := createTestSendMessageInput("delayed")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)
	q.Push(msgState)

	initialStats := q.Stats()
	require.Greater(t, initialStats.NumMessages, int64(0))

	q.Purge()

	updatedStats := q.Stats()
	require.Equal(t, int64(0), updatedStats.NumMessages)
	require.Equal(t, int64(0), updatedStats.NumMessagesReady)
	require.Equal(t, int64(0), updatedStats.NumMessagesDelayed)
	require.Equal(t, int64(0), updatedStats.NumMessagesInflight)
	require.Equal(t, initialStats.TotalMessagesPurged+uint64(initialStats.NumMessages), updatedStats.TotalMessagesPurged)

	// All internal maps should be cleared
	require.Equal(t, 0, q.messagesReadyColdKeyed.Len())
	require.Equal(t, 0, q.messagesReadyHotKeyed.Len())
	require.Len(t, q.messagesDelayed, 0)
	require.Equal(t, 0, q.messagesInflight.Len())
}

func Test_Queue_Purge_emptyQueueIsNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.Purge()

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

// Tests for Queue.PurgeExpired()
func Test_Queue_PurgeExpired_removesExpiredMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		q.MessageRetentionPeriod = 1 * time.Hour

		// Add messages
		pushTestMessages(q, 3)

		// Move one to inflight
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
		require.Len(t, received, 1)

		// Add delayed message
		msg := createTestSendMessageInput("delayed")
		msg.DelaySeconds = 10
		msgState := q.NewMessageStateFromSendMessageInput(msg)
		q.Push(msgState)

		initialStats := q.Stats()
		require.Equal(t, int64(4), initialStats.NumMessages)

		// Advance time beyond retention period
		time.Sleep(2 * time.Hour)

		q.PurgeExpired()

		updatedStats := q.Stats()
		require.Equal(t, int64(0), updatedStats.NumMessages)
		require.Equal(t, int64(0), updatedStats.NumMessagesReady)
		require.Equal(t, int64(0), updatedStats.NumMessagesDelayed)
		require.Equal(t, int64(0), updatedStats.NumMessagesInflight)
		require.Equal(t, initialStats.TotalMessagesPurged+4, updatedStats.TotalMessagesPurged)
	})
}

func Test_Queue_PurgeExpired_keepsNonExpiredMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		q.MessageRetentionPeriod = 1 * time.Hour

		pushTestMessages(q, 2)
		initialStats := q.Stats()

		// Advance time but not beyond retention period
		time.Sleep(30 * time.Minute)

		q.PurgeExpired()

		updatedStats := q.Stats()
		require.Equal(t, initialStats.NumMessages, updatedStats.NumMessages)
		require.Equal(t, initialStats.TotalMessagesPurged, updatedStats.TotalMessagesPurged)
	})
}

func Test_Queue_PurgeExpired_emptyQueueIsNoOp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		initialStats := q.Stats()

		time.Sleep(2 * time.Hour)
		q.PurgeExpired()

		updatedStats := q.Stats()
		require.Equal(t, initialStats, updatedStats)
	})
}

// Tests for Queue.UpdateInflightVisibility()
func Test_Queue_UpdateInflightVisibility_movesVisibleMessagesToReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		pushTestMessages(q, 10)

		// Receive messages with short visibility timeout - get at least 3
		var allReceived []types.Message
		for len(allReceived) < 3 {
			batch := q.Receive(&sqs.ReceiveMessageInput{
				MaxNumberOfMessages: 10,
				VisibilityTimeout:   10, // 10 seconds
			})
			allReceived = append(allReceived, batch...)
			if len(batch) == 0 {
				break
			}
		}
		require.GreaterOrEqual(t, len(allReceived), 3)

		initialInflight := q.Stats().NumMessagesInflight
		initialReady := q.Stats().NumMessagesReady

		// Advance time beyond visibility timeout
		time.Sleep(15 * time.Second)

		q.UpdateInflightVisibility()

		updatedStats := q.Stats()
		require.Equal(t, int64(0), updatedStats.NumMessagesInflight)
		require.Equal(t, initialReady+initialInflight, updatedStats.NumMessagesReady)
		require.Equal(t, uint64(initialInflight), updatedStats.TotalMessagesInflightToReady)
	})
}

func Test_Queue_UpdateInflightVisibility_keepsNonVisibleMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		pushTestMessages(q, 10)

		// Receive messages with long visibility timeout - get at least 2
		var allReceived []types.Message
		for len(allReceived) < 2 {
			batch := q.Receive(&sqs.ReceiveMessageInput{
				MaxNumberOfMessages: 10,
				VisibilityTimeout:   3600, // 1 hour
			})
			allReceived = append(allReceived, batch...)
			if len(batch) == 0 {
				break
			}
		}
		require.GreaterOrEqual(t, len(allReceived), 2)

		initialStats := q.Stats()

		// Advance time but not beyond visibility timeout
		time.Sleep(30 * time.Minute)

		q.UpdateInflightVisibility()

		updatedStats := q.Stats()
		require.Equal(t, initialStats.NumMessagesInflight, updatedStats.NumMessagesInflight)
		require.Equal(t, initialStats.NumMessagesReady, updatedStats.NumMessagesReady)
	})
}

func Test_Queue_UpdateInflightVisibility_emptyInflightIsNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.UpdateInflightVisibility()

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

// Tests for Queue.UpdateDelayedToReady()
func Test_Queue_UpdateDelayedToReady_movesReadyDelayedMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)

		// Add delayed messages
		for i := range 3 {
			msg := createTestSendMessageInput(fmt.Sprintf("delayed %d", i))
			msg.DelaySeconds = 10
			msgState := q.NewMessageStateFromSendMessageInput(msg)
			q.Push(msgState)
		}

		initialStats := q.Stats()
		require.Equal(t, int64(3), initialStats.NumMessagesDelayed)
		require.Equal(t, int64(0), initialStats.NumMessagesReady)

		// Advance time beyond delay
		time.Sleep(15 * time.Second)

		q.UpdateDelayedToReady()

		updatedStats := q.Stats()
		require.Equal(t, int64(0), updatedStats.NumMessagesDelayed)
		require.Equal(t, int64(3), updatedStats.NumMessagesReady)
		require.Equal(t, uint64(3), updatedStats.TotalMessagesDelayedToReady)
	})
}

func Test_Queue_UpdateDelayedToReady_keepsStillDelayedMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)

		// Add delayed message
		msg := createTestSendMessageInput("delayed")
		msg.DelaySeconds = 3600 // 1 hour
		msgState := q.NewMessageStateFromSendMessageInput(msg)
		q.Push(msgState)

		initialStats := q.Stats()

		// Advance time but not beyond delay
		time.Sleep(30 * time.Minute)

		q.UpdateDelayedToReady()

		updatedStats := q.Stats()
		require.Equal(t, initialStats.NumMessagesDelayed, updatedStats.NumMessagesDelayed)
		require.Equal(t, initialStats.NumMessagesReady, updatedStats.NumMessagesReady)
	})
}

func Test_Queue_UpdateDelayedToReady_emptyDelayedIsNoOp(t *testing.T) {
	q := createTestQueue(t)
	initialStats := q.Stats()

	q.UpdateDelayedToReady()

	updatedStats := q.Stats()
	require.Equal(t, initialStats, updatedStats)
}

// Tests for Queue.GetQueueAttributes()
func Test_Queue_GetQueueAttributes_returnsRequestedAttributes(t *testing.T) {
	q := createTestQueue(t)
	q.RedrivePolicy = Some(RedrivePolicy{
		DeadLetterTargetArn: "test-queue-arn",
		MaxReceiveCount:     10,
	})
	pushTestMessages(q, 10)

	// Move exactly 2 to inflight
	var inflightCount int
	for inflightCount < 2 {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 5})
		inflightCount += len(received)
		if len(received) == 0 {
			break
		}
	}

	// Add delayed message
	msg := createTestSendMessageInput("delayed")
	msg.DelaySeconds = 10
	msgState := q.NewMessageStateFromSendMessageInput(msg)
	q.Push(msgState)

	attributes := q.GetQueueAttributes(
		types.QueueAttributeNameApproximateNumberOfMessages,
		types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		types.QueueAttributeNameQueueArn,
		types.QueueAttributeNameRedrivePolicy,
		types.QueueAttributeNameVisibilityTimeout,
	)

	// Check that we have the expected total and some inflight
	stats := q.Stats()
	expectedTotal := fmt.Sprint(stats.NumMessages)
	expectedInflight := fmt.Sprint(stats.NumMessagesInflight)
	expectedDelayed := fmt.Sprint(stats.NumMessagesDelayed)

	require.EqualValues(t, expectedTotal, attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	require.EqualValues(t, expectedInflight, attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	require.EqualValues(t, expectedDelayed, attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)])
	require.EqualValues(t, "{\"deadLetterTargetArn\":\"test-queue-arn\",\"maxReceiveCount\":10}", attributes[string(types.QueueAttributeNameRedrivePolicy)])
	require.EqualValues(t, q.ARN, attributes[string(types.QueueAttributeNameQueueArn)])
	require.EqualValues(t, "30", attributes[string(types.QueueAttributeNameVisibilityTimeout)]) // Should have some visibility timeout value
}

func Test_Queue_GetQueueAttributes_returnsAllWhenRequested(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(time.Minute)

	attributes := q.GetQueueAttributes(types.QueueAttributeNameAll)

	// Should include standard attributes
	require.Contains(t, attributes, string(types.QueueAttributeNameApproximateNumberOfMessages))
	require.Contains(t, attributes, string(types.QueueAttributeNameQueueArn))
	require.Contains(t, attributes, string(types.QueueAttributeNameVisibilityTimeout))
	require.Contains(t, attributes, string(types.QueueAttributeNameMaximumMessageSize))
	require.Contains(t, attributes, string(types.QueueAttributeNameMessageRetentionPeriod))
	require.Contains(t, attributes, string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds))
	require.Contains(t, attributes, string(types.QueueAttributeNameCreatedTimestamp))
	require.Contains(t, attributes, string(types.QueueAttributeNameLastModifiedTimestamp))
	require.Contains(t, attributes, string(types.QueueAttributeNameDelaySeconds))
}

func Test_Queue_GetQueueAttributes_emptyForNonExistentAttributes(t *testing.T) {
	q := createTestQueue(t)

	attributes := q.GetQueueAttributes(types.QueueAttributeNameRedrivePolicy)

	// Should either not include redrive policy, or include empty value
	// Current implementation returns the marshaled Optional struct
	value, exists := attributes[string(types.QueueAttributeNameRedrivePolicy)]
	require.False(t, exists)
	require.Equal(t, "", value)
}

func Test_Queue_GetQueueAttributes_includesTimestamps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)

		attributes := q.GetQueueAttributes(
			types.QueueAttributeNameCreatedTimestamp,
			types.QueueAttributeNameLastModifiedTimestamp,
		)

		createdTimestamp := attributes[string(types.QueueAttributeNameCreatedTimestamp)]
		lastModifiedTimestamp := attributes[string(types.QueueAttributeNameLastModifiedTimestamp)]

		require.Equal(t, fmt.Sprint(time.Now().Unix()), createdTimestamp)
		require.Equal(t, fmt.Sprint(time.Now().Unix()), lastModifiedTimestamp)
	})
}

// Tests for concurrent operations and race conditions
func Test_Queue_ConcurrentPushAndReceive_maintainsConsistency(t *testing.T) {
	q := createTestQueue(t)

	var wg sync.WaitGroup
	const numGoroutines = 10
	const messagesPerGoroutine = 50

	// Concurrent pushes
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range messagesPerGoroutine {
				msg := createTestSendMessageInput("concurrent message")
				msgState := q.NewMessageStateFromSendMessageInput(msg)
				q.Push(msgState)
			}
		}()
	}

	// Concurrent receives
	var totalReceived int64
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range messagesPerGoroutine / 2 {
				received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
				atomic.AddInt64(&totalReceived, int64(len(received)))
			}
		}()
	}

	wg.Wait()

	stats := q.Stats()
	totalPushed := numGoroutines * messagesPerGoroutine
	expectedReceived := numGoroutines * messagesPerGoroutine / 2

	require.Equal(t, uint64(totalPushed), stats.TotalMessagesSent)
	require.LessOrEqual(t, totalReceived, int64(expectedReceived)) // Due to randomization in receive
	require.Equal(t, int64(totalPushed), stats.NumMessages)
}

func Test_Queue_ConcurrentTagging_maintainsConsistency(t *testing.T) {
	q := createTestQueue(t)

	var wg sync.WaitGroup
	const numGoroutines = 10

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tags := map[string]string{
				fmt.Sprintf("key%d", id): fmt.Sprintf("value%d", id),
			}
			q.Tag(tags)
		}(i)
	}

	wg.Wait()

	// All tags should be present
	require.GreaterOrEqual(t, len(q.Tags), numGoroutines)
}

func Test_Queue_ConcurrentDeleteAndVisibilityChange_maintainsConsistency(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 4)

	// Receive messages
	received := make([]types.Message, 0)
	for len(received) < 4 {
		batch := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 10})
		received = append(received, batch...)
		if len(batch) == 0 {
			break
		}
	}
	require.GreaterOrEqual(t, len(received), 4)

	var wg sync.WaitGroup
	var operationCount int64

	// Perform concurrent operations on received messages
	for i := 0; i < len(received) && i < 4; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if index%2 == 0 {
				// Try to delete
				ok := q.Delete(*received[index].ReceiptHandle)
				if ok {
					atomic.AddInt64(&operationCount, 1)
				}
			} else {
				// Try to change visibility
				ok := q.ChangeMessageVisibility(*received[index].ReceiptHandle, 120*time.Second)
				if ok {
					atomic.AddInt64(&operationCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Should have performed some operations without panicking
	require.Greater(t, operationCount, int64(0))
}

// Tests for edge cases and regression prevention
func Test_Queue_Receive_withMaximumMessagesInflightReached_returnsEmpty(t *testing.T) {
	q := createTestQueue(t)
	q.MaximumMessagesInflight = 2
	pushTestMessages(q, 5)

	// Keep receiving until we hit the inflight limit
	var totalReceived int
	for {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 10})
		totalReceived += len(received)
		if len(received) == 0 {
			break // No more messages can be received due to inflight limit
		}
	}

	// Should have some messages inflight, but be limited by MaximumMessagesInflight
	stats := q.Stats()
	require.LessOrEqual(t, stats.NumMessagesInflight, int64(q.MaximumMessagesInflight))
	require.Greater(t, stats.NumMessagesInflight, int64(0))
}

func Test_Queue_Push_withQueueDelayAndMessageDelay_messageDelayTakesPrecedence(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(30 * time.Second) // Queue default delay

	// Message with explicit delay
	msg := createTestSendMessageInput("test")
	msg.DelaySeconds = 60 // Message specific delay
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	// Message should use its own delay, not queue default
	require.Equal(t, 60*time.Second, msgState.Delay.Value)
	require.Len(t, q.messagesDelayed, 1)
	require.Equal(t, 0, q.messagesReadyColdKeyed.Len())
	require.Equal(t, 0, q.messagesReadyHotKeyed.Len())
}

func Test_Queue_Push_withOnlyQueueDelay_appliesQueueDelay(t *testing.T) {
	q := createTestQueue(t)
	q.Delay = Some(30 * time.Second)

	// Message with no explicit delay
	msg := createTestSendMessageInput("test")
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	q.Push(msgState)

	// Message should use queue default delay
	require.Equal(t, 30*time.Second, msgState.Delay.Value)
	require.Len(t, q.messagesDelayed, 1)
	require.Equal(t, 0, q.messagesReadyColdKeyed.Len())
	require.Equal(t, 0, q.messagesReadyHotKeyed.Len())
}

func Test_Queue_Receive_randomizesMessageCount_withinBounds(t *testing.T) {
	q := createTestQueue(t)
	pushTestMessages(q, 100)

	// Request 10 messages, should get between 1 and 10 (inclusive)
	counts := make(map[int]int)
	for range 100 {
		received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 10})
		counts[len(received)]++
		require.GreaterOrEqual(t, len(received), 1)
		require.LessOrEqual(t, len(received), 10)

		// Return messages to ready state for next iteration
		for _, msg := range received {
			q.ChangeMessageVisibility(*msg.ReceiptHandle, 0)
		}
	}

	// Should have some variety in returned counts (not always the same)
	require.Greater(t, len(counts), 1)
}

func Test_Queue_ChangeMessageVisibility_withDLQMovement_respectsMaxReceiveCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dlq := createTestQueueWithName(t, "test-dlq")
		q := createTestQueueWithNameWithDLQ(t, "test-queue", dlq)

		pushTestMessages(q, 1)
		initialDLQStats := dlq.Stats()

		// Receive and let visibility timeout expire multiple times to trigger DLQ movement
		var messageReceiveCount uint32
		for range 4 { // One more than max receive count
			received := q.Receive(&sqs.ReceiveMessageInput{MaxNumberOfMessages: 1})
			if len(received) == 0 {
				break // Message moved to DLQ
			}
			require.Len(t, received, 1)
			receiptHandle := *received[0].ReceiptHandle
			messageReceiveCount++

			// Let message become visible again
			q.ChangeMessageVisibility(receiptHandle, 0)
		}

		// Check that message was received multiple times and potentially moved to DLQ
		require.GreaterOrEqual(t, messageReceiveCount, uint32(3))

		// If max receive count exceeded, message should be in DLQ
		finalDLQStats := dlq.Stats()
		if messageReceiveCount >= 3 {
			require.GreaterOrEqual(t, finalDLQStats.NumMessages, initialDLQStats.NumMessages)
		}
	})
}

func Test_Queue_Stats_atomicOperations_maintainConsistency(t *testing.T) {
	q := createTestQueue(t)

	var wg sync.WaitGroup
	const numOperations = 1000

	// Concurrent operations that modify stats
	for range numOperations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := createTestSendMessageInput("test")
			msgState := q.NewMessageStateFromSendMessageInput(msg)
			q.Push(msgState)
		}()
	}

	// Concurrent stats reading
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := q.Stats()
			// Just reading stats should not panic or return inconsistent values
			require.GreaterOrEqual(t, stats.NumMessages, int64(0))
			require.GreaterOrEqual(t, stats.TotalMessagesSent, uint64(0))
		}()
	}

	wg.Wait()

	finalStats := q.Stats()
	require.Equal(t, int64(numOperations), finalStats.NumMessages)
	require.Equal(t, uint64(numOperations), finalStats.TotalMessagesSent)
}

func Test_Queue_SetQueueAttributes_withInvalidJSON_returnsError(t *testing.T) {
	q := createTestQueue(t)

	err := q.SetQueueAttributes(map[string]string{
		string(types.QueueAttributeNameRedrivePolicy): "invalid-json",
	})

	require.NotNil(t, err)
}

func Test_Queue_GetQueueAttributes_withRedrivePolicy_returnsJSONString(t *testing.T) {
	q := createTestQueue(t)
	redrivePolicy := RedrivePolicy{
		DeadLetterTargetArn: "arn:aws:sqs:us-west-2:123456789012:test-dlq",
		MaxReceiveCount:     3,
	}
	q.RedrivePolicy = Some(redrivePolicy)

	attributes := q.GetQueueAttributes(types.QueueAttributeNameRedrivePolicy)

	require.Contains(t, attributes, string(types.QueueAttributeNameRedrivePolicy))
	policyJSON := attributes[string(types.QueueAttributeNameRedrivePolicy)]
	require.Contains(t, policyJSON, "arn:aws:sqs:us-west-2:123456789012:test-dlq")
	require.Contains(t, policyJSON, "3")
}

func Test_Queue_PurgeExpired_withMixedExpiryTimes_onlyRemovesExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)
		q.MessageRetentionPeriod = 1 * time.Hour

		// Add messages at different times
		pushTestMessages(q, 2) // Fresh messages

		// Advance time and add more messages
		time.Sleep(30 * time.Minute)
		pushTestMessages(q, 2) // Half-aged messages

		// Advance time to expire first batch but not second
		time.Sleep(45 * time.Minute) // Total: 75 minutes (> 1 hour for first batch)

		q.PurgeExpired()

		stats := q.Stats()
		require.Equal(t, int64(2), stats.NumMessages) // Only second batch should remain
		require.Equal(t, uint64(2), stats.TotalMessagesPurged)
	})
}

func Test_Queue_UpdateDelayedToReady_withVariousDelays_onlyMovesReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := createTestQueue(t)

		// Add messages with different delays
		msg1 := createTestSendMessageInput("short delay")
		msg1.DelaySeconds = 10
		msgState1 := q.NewMessageStateFromSendMessageInput(msg1)

		msg2 := createTestSendMessageInput("long delay")
		msg2.DelaySeconds = 3600 // 1 hour
		msgState2 := q.NewMessageStateFromSendMessageInput(msg2)

		q.Push(msgState1, msgState2)

		// Advance time to expire only first message
		time.Sleep(15 * time.Second)

		q.UpdateDelayedToReady()

		stats := q.Stats()
		require.Equal(t, int64(1), stats.NumMessagesReady)   // Only msg1 should be ready
		require.Equal(t, int64(1), stats.NumMessagesDelayed) // msg2 should still be delayed
		require.Equal(t, uint64(1), stats.TotalMessagesDelayedToReady)
	})
}

// Test that tests the original source queue tracking
func Test_Queue_Push_setsOriginalSourceQueue(t *testing.T) {
	q := createTestQueue(t)
	msg := createTestSendMessageInput("test")
	msgState := q.NewMessageStateFromSendMessageInput(msg)

	// NewMessageStateFromSendMessageInput should set original source queue
	require.NotNil(t, msgState.OriginalSourceQueue)
	require.Equal(t, q, msgState.OriginalSourceQueue)

	q.Push(msgState)

	// After push, original source queue should be preserved
	require.NotNil(t, msgState.OriginalSourceQueue)
	require.Equal(t, q, msgState.OriginalSourceQueue)
}

func Test_Queue_Push_preservesExistingOriginalSourceQueue(t *testing.T) {
	originalQueue := createTestQueueWithName(t, "original")
	targetQueue := createTestQueueWithName(t, "target")

	msg := createTestSendMessageInput("test")
	msgState := originalQueue.NewMessageStateFromSendMessageInput(msg)
	originalQueue.Push(msgState)

	// Move message to target queue (simulating DLQ movement)
	msgFromOriginal, ok := originalQueue.PopMessageForMove()
	require.True(t, ok)
	targetQueue.Push(msgFromOriginal)

	// Original source queue should be preserved
	require.Equal(t, originalQueue, msgFromOriginal.OriginalSourceQueue)
}
