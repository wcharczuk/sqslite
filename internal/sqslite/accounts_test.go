package sqslite

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewAccounts(t *testing.T) {
	accounts := NewAccounts()
	defer accounts.Close()

	require.Empty(t, accounts.accounts)
}

func Test_Accounts_EnsureQueues(t *testing.T) {
	accounts := NewAccounts()
	defer accounts.Close()

	queues := accounts.EnsureQueues("test-account-id")

	requireHasKey(t, "test-account-id", accounts.accounts)

	require.NotNil(t, queues.deletedQueueWorker)
	require.NotNil(t, queues.deletedQueueWorkerCancel)
}

func Test_Accounts_EnsureQueues_multipleKeys(t *testing.T) {
	accounts := NewAccounts()
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

func Test_Accounts_EachQueue(t *testing.T) {
	accounts := NewAccounts()
	defer accounts.Close()

	var nameOrdinal uint32
	queues00 := accounts.EnsureQueues("test-account-id-00")
	queues00.AddQueue(createTestQueueWithName(t, fmt.Sprintf("test-queue-%d", atomic.AddUint32(&nameOrdinal, 1))))
	queues01 := accounts.EnsureQueues("test-account-id-01")
	queues01.AddQueue(createTestQueueWithName(t, fmt.Sprintf("test-queue-%d", atomic.AddUint32(&nameOrdinal, 1))))
	queues01.AddQueue(createTestQueueWithName(t, fmt.Sprintf("test-queue-%d", atomic.AddUint32(&nameOrdinal, 1))))
	queues02 := accounts.EnsureQueues("test-account-id-02")
	queues02.AddQueue(createTestQueueWithName(t, fmt.Sprintf("test-queue-%d", atomic.AddUint32(&nameOrdinal, 1))))
	queues02.AddQueue(createTestQueueWithName(t, fmt.Sprintf("test-queue-%d", atomic.AddUint32(&nameOrdinal, 1))))
	queues02.AddQueue(createTestQueueWithName(t, fmt.Sprintf("test-queue-%d", atomic.AddUint32(&nameOrdinal, 1))))

	var queues []string
	for queue := range accounts.EachQueue() {
		queues = append(queues, queue.URL)
	}
	require.Len(t, queues, 6)

	var shortQueues []string
	for queue := range accounts.EachQueue() {
		shortQueues = append(shortQueues, queue.URL)
		if len(shortQueues) == 3 {
			break
		}
	}
	require.Len(t, queues, 6)
}

func requireHasKey[K comparable, V any](t *testing.T, key K, m map[K]V, msgAndArgs ...any) {
	t.Helper()
	_, ok := m[key]
	if !ok {
		require.Fail(t, fmt.Sprintf("map is missing expected key: %v", key), msgAndArgs...)
	}
}
