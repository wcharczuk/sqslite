package sqslite

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GroupedShardedLinkedList_Push_Pop(t *testing.T) {
	q := newGroupedShardedLinkedList[string, string](32)
	for x := range 100 {
		q.Push("one", fmt.Sprint(x))
	}
	for x := range 50 {
		q.Push("two", fmt.Sprint(x))
	}
	for x := range 25 {
		q.Push("three", fmt.Sprint(x))
	}
	require.Equal(t, 175, q.Len())

	for range 175 {
		_, v, ok := q.Pop()
		require.True(t, ok)
		require.NotEqualValues(t, "", v)
	}
	require.Equal(t, 0, q.Len())

	_, v, ok := q.Pop()
	require.False(t, ok)
	require.EqualValues(t, "", v)

	for x := range 100 {
		q.Push("one", fmt.Sprint(x))
	}
	for x := range 50 {
		q.Push("two", fmt.Sprint(x))
	}
	for x := range 25 {
		q.Push("three", fmt.Sprint(x))
	}
	require.Equal(t, 175, q.Len())
	for range 175 {
		_, v, ok := q.Pop()
		require.True(t, ok)
		require.NotEqualValues(t, "", v)
	}
	require.Equal(t, 0, q.Len())
}

func Test_GroupedShardedLinkedList_RemoveNode(t *testing.T) {
	q := newGroupedShardedLinkedList[string, string](32)
	var nodes []*GroupedShardedLinkedListNode[string, string]
	for x := range 100 {
		node := q.Push("", fmt.Sprint(x))
		require.NotNil(t, node)
		require.EqualValues(t, fmt.Sprint(x), node.ListNode.Value)
		require.True(t, node.ShardIndex < 32)
		nodes = append(nodes, node)
	}
	require.Equal(t, 100, q.Len())
	for _, n := range nodes {
		q.Remove(n)
	}
	require.Equal(t, 0, q.Len())
}
