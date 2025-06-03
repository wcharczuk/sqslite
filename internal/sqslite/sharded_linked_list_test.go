package sqslite

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ShardedLinkedList_Push_Pop(t *testing.T) {
	q := NewShardedLinkedList[string](32)
	for x := range 100 {
		q.Push(fmt.Sprint(x))
	}
	require.Equal(t, 100, q.Len())

	for range 100 {
		v, ok := q.Pop()
		require.True(t, ok)
		require.NotEqualValues(t, "", v)
	}
	require.Equal(t, 0, q.Len())

	v, ok := q.Pop()
	require.False(t, ok)
	require.EqualValues(t, "", v)

	for x := range 100 {
		q.Push(fmt.Sprint(x))
	}
	require.Equal(t, 100, q.Len())
	for range 100 {
		v, ok := q.Pop()
		require.True(t, ok)
		require.NotEqualValues(t, "", v)
	}
	require.Equal(t, 0, q.Len())
}

func Test_ShardedLinkedList_RemoveNode(t *testing.T) {
	q := NewShardedLinkedList[string](32)
	var nodes []*ShardedLinkedListNode[string]
	for x := range 100 {
		node := q.Push(fmt.Sprint(x))
		require.NotNil(t, node)
		require.EqualValues(t, fmt.Sprint(x), node.LinkedListNode.Value)
		require.True(t, node.ShardIndex < 32)
		nodes = append(nodes, node)
	}
	require.Equal(t, 100, q.Len())

	for _, n := range nodes {
		q.Remove(n)
	}
	require.Equal(t, 0, q.Len())
}
