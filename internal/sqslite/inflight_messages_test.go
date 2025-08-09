package sqslite

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wcharczuk/sqslite/internal/uuid"
)

func Test_inflightMessages_HotKeys_empty(t *testing.T) {
	inflight := newInflightMessages()
	hotKeys := inflight.HotKeys()
	require.Empty(t, hotKeys)
}

func Test_inflightMessages_HotKeys_even(t *testing.T) {
	inflight := newInflightMessages()

	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "foo",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "bar",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "baz",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "buzz",
		})
	}
	hotKeys := inflight.HotKeys()
	require.Empty(t, hotKeys)
}

func Test_inflightMessages_HotKeys_singleHotKey(t *testing.T) {
	inflight := newInflightMessages()

	for range 64 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "foo",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "bar",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "baz",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "buzz",
		})
	}
	hotKeys := inflight.HotKeys()
	require.Len(t, hotKeys, 1)
	require.True(t, hotKeys.Has("foo"))
}

func Test_inflightMessages_HotKeys_manyHotKeys(t *testing.T) {
	inflight := newInflightMessages()

	for range 8 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "foo",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "bar",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "baz",
		})
	}
	for range 4 {
		inflight.Push(uuid.V4().String(), &MessageState{
			MessageID:      uuid.V4(),
			MessageGroupID: "buzz",
		})
	}
	hotKeys := inflight.HotKeys()
	require.Len(t, hotKeys, 1)
	require.True(t, hotKeys.Has("foo"))
}
