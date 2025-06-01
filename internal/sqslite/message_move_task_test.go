package sqslite

import (
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func Test_NewMessageMoveTask(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sourceQueue := createTestQueueWithName(t, clock, "source")
	destinationQueue := createTestQueueWithName(t, clock, "destination")

	mmt := NewMessagesMoveTask(clock, sourceQueue, destinationQueue, 100)
	require.NotNil(t, mmt.SourceQueue)
	require.NotNil(t, mmt.DestinationQueue)
	require.EqualValues(t, mmt.AccountID, sourceQueue.AccountID)
	require.NotEmpty(t, mmt.TaskHandle)
	require.False(t, mmt.started.IsZero())

	require.NotNil(t, mmt.limiter)
	require.EqualValues(t, rate.Limit(100), mmt.limiter.Limit())
}

func Test_MessageMoveStatus_String(t *testing.T) {
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ListMessageMoveTasksResultEntry.html
	require.Equal(t, "UNKNOWN", MessageMoveStatusUnknown.String())
	require.Equal(t, "RUNNING", MessageMoveStatusRunning.String())
	require.Equal(t, "COMPLETED", MessageMoveStatusCompleted.String())
	require.Equal(t, "FAILED", MessageMoveStatusFailed.String())
	//nolint:misspell
	require.Equal(t, "CANCELLING", MessageMoveStatusCanceling.String())
	//nolint:misspell
	require.Equal(t, "CANCELLED", MessageMoveStatusCanceled.String())
	require.Equal(t, "UNKNOWN", MessageMoveStatus(127).String())
}
