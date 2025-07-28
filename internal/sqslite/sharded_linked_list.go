package sqslite

import "math/rand/v2"

const DefaultQueueShardCount = 32

func NewShardedLinkedList[T any](shardCount int) *ShardedLinkedList[T] {
	if shardCount == 0 {
		panic("sharded linked list cannot have 0 shards")
	}
	return &ShardedLinkedList[T]{
		shards: make([]List[T], shardCount),
	}
}

type ShardedLinkedList[T any] struct {
	shards []List[T]
	len    int
}

// ShardedLinkedListNode is a linked list node with a shard index.
type ShardedLinkedListNode[T any] struct {
	ListNode[T]
	ShardIndex uint32
}

func (sll *ShardedLinkedList[T]) Len() int {
	return sll.len
}

func (sll *ShardedLinkedList[T]) Push(value T) *ShardedLinkedListNode[T] {
	randomShardIndex := rand.IntN(len(sll.shards))
	node := sll.shards[randomShardIndex].Push(value)
	sll.len++
	return &ShardedLinkedListNode[T]{
		ListNode:   *node,
		ShardIndex: uint32(randomShardIndex),
	}
}

func (sll *ShardedLinkedList[T]) Pop() (out T, ok bool) {
	if sll.len == 0 {
		return
	}
	randomStartIndex := rand.IntN(len(sll.shards))
	for x := range sll.shards {
		shardIndex := (randomStartIndex + x) % len(sll.shards)
		if sll.shards[shardIndex].Len() > 0 {
			out, ok = sll.shards[shardIndex].Pop()
			sll.len--
			return
		}
	}
	return
}

func (sll *ShardedLinkedList[T]) Remove(node *ShardedLinkedListNode[T]) {
	sll.len--
	sll.shards[node.ShardIndex].Remove(&node.ListNode)
}

// Clear clears the linked list.
func (sll *ShardedLinkedList[T]) Clear() {
	for _, shard := range sll.shards {
		shard.Clear()
	}
	sll.len = 0
}
