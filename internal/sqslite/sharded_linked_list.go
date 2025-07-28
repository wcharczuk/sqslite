package sqslite

import "math/rand/v2"

func NewShardedLinkedList[K comparable, T any](shardCount int) *ShardedLinkedList[K, T] {
	if shardCount == 0 {
		panic("sharded linked list cannot have 0 shards")
	}
	return &ShardedLinkedList[K, T]{
		shardCount: shardCount,
		shards:     make(map[K][]List[T]),
	}
}

type ShardedLinkedList[K comparable, T any] struct {
	shardCount int
	shards     map[K][]List[T]
	len        int
}

// ShardedLinkedListNode is a linked list node with a shard index.
type ShardedLinkedListNode[K comparable, T any] struct {
	ListNode[T]
	Key        K
	ShardIndex uint32
}

func (sll *ShardedLinkedList[K, T]) ensureShardsForKey(key K) []List[T] {
	if _, ok := sll.shards[key]; !ok {
		sll.shards[key] = make([]List[T], sll.shardCount)
	}
	return sll.shards[key]
}

func (sll *ShardedLinkedList[K, T]) Len() int {
	return sll.len
}

func (sll *ShardedLinkedList[K, T]) Push(key K, value T) *ShardedLinkedListNode[K, T] {
	shards := sll.ensureShardsForKey(key)
	randomShardIndex := rand.IntN(len(shards))
	node := shards[randomShardIndex].Push(value)
	sll.len++
	return &ShardedLinkedListNode[K, T]{
		ListNode:   *node,
		Key:        key,
		ShardIndex: uint32(randomShardIndex),
	}
}

func (sll *ShardedLinkedList[K, T]) Pop() (key K, out T, ok bool) {
	if sll.len == 0 {
		return
	}

	// pick a random key
	for key = range sll.shards {
		break
	}

	shards := sll.ensureShardsForKey(key)
	randomStartIndex := rand.IntN(len(shards))
	for x := range shards {
		shardIndex := (randomStartIndex + x) % len(shards)
		if shards[shardIndex].Len() > 0 {
			out, ok = shards[shardIndex].Pop()
			sll.len--
			return
		}
	}
	return
}

func (sll *ShardedLinkedList[K, T]) Remove(node *ShardedLinkedListNode[K, T]) {
	sll.len--
	sll.shards[node.Key][node.ShardIndex].Remove(&node.ListNode)
}

// Clear clears the linked list.
func (sll *ShardedLinkedList[K, T]) Clear() {
	for _, shardGroup := range sll.shards {
		for _, shard := range shardGroup {
			shard.Clear()
		}
	}
	sll.len = 0
}
