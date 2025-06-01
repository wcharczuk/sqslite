package sqslite

import (
	"fmt"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func Test_NewAccounts(t *testing.T) {
	clock := clockwork.NewFakeClock()
	accounts := NewAccounts(clock)
	defer accounts.Close()

	require.Empty(t, accounts.accounts)
	require.NotNil(t, accounts.clock)
}

func Test_Accounts_EnsureQueues(t *testing.T) {
	clock := clockwork.NewFakeClock()
	accounts := NewAccounts(clock)
	defer accounts.Close()

	queues := accounts.EnsureQueues("test-account-id")

	requireHasKey(t, "test-account-id", accounts.accounts)

	require.NotNil(t, queues.deletedQueueWorker)
	require.NotNil(t, queues.deletedQueueWorkerCancel)
}

func Test_Accounts_EnsureQueues_multipleKeys(t *testing.T) {
	clock := clockwork.NewFakeClock()
	accounts := NewAccounts(clock)
	defer accounts.Close()

	queues00 := accounts.EnsureQueues("test-account-id-00")
	queues01 := accounts.EnsureQueues("test-account-id-01")

	requireHasKey(t, "test-account-id-00", accounts.accounts)
	requireHasKey(t, "test-account-id-01", accounts.accounts)

	require.NotNil(t, queues00.deletedQueueWorker)
	require.NotNil(t, queues00.deletedQueueWorkerCancel)

	require.NotNil(t, queues01.deletedQueueWorker)
	require.NotNil(t, queues01.deletedQueueWorkerCancel)
}

func requireHasKey[K comparable, V any](t *testing.T, key K, m map[K]V, msgAndArgs ...any) {
	t.Helper()
	_, ok := m[key]
	if !ok {
		require.Fail(t, fmt.Sprintf("map is missing expected key: %v", key), msgAndArgs...)
	}
}
