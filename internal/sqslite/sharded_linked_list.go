package sqslite

import (
	"iter"
	"math/rand/v2"
)

const DefaultQueueShardCount = 32

func newShardedLinkedList[T any](shardCount int) *shardedLinkedList[T] {
	if shardCount == 0 {
		panic("sharded linked list cannot have 0 shards")
	}
	return &shardedLinkedList[T]{
		shards: make([]list[T], shardCount),
	}
}

type shardedLinkedList[T any] struct {
	shards []list[T]
	len    int
}

// shardedLinkedListNode is a linked list node with a shard index.
type shardedLinkedListNode[T any] struct {
	listNode[T]
	ShardIndex uint32
}

func (sll *shardedLinkedList[T]) Len() int {
	return sll.len
}

func (sll *shardedLinkedList[T]) Push(value T) *shardedLinkedListNode[T] {
	randomShardIndex := rand.IntN(len(sll.shards))
	node := sll.shards[randomShardIndex].Push(value)
	sll.len++
	return &shardedLinkedListNode[T]{
		listNode:   *node,
		ShardIndex: uint32(randomShardIndex),
	}
}

func (sll *shardedLinkedList[T]) Pop() (out T, ok bool) {
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

func (sll *shardedLinkedList[T]) Remove(node *shardedLinkedListNode[T]) {
	sll.len--
	sll.shards[node.ShardIndex].Remove(&node.listNode)
}

// Clear clears the linked list.
func (sll *shardedLinkedList[T]) Clear() {
	for _, shard := range sll.shards {
		shard.Clear()
	}
	sll.len = 0
}

func (sll *shardedLinkedList[T]) Each() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, s := range sll.shards {
			for n := range s.Each() {
				if !yield(n) {
					return
				}
			}
		}
	}
}

func (sll *shardedLinkedList[T]) EachNode() iter.Seq[*shardedLinkedListNode[T]] {
	return func(yield func(*shardedLinkedListNode[T]) bool) {
		for shardIndex, s := range sll.shards {
			for n := range s.EachNode() {
				if !yield(&shardedLinkedListNode[T]{
					listNode:   *n,
					ShardIndex: uint32(shardIndex),
				}) {
					return
				}
			}
		}
	}
}
