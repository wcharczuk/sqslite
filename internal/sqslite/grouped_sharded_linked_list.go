package sqslite

import (
	"iter"
	"maps"
	"slices"
)

func NewGroupedShardedLinkedList[K comparable, V any](shardCount int) *GroupedShardedLinkedList[K, V] {
	if shardCount == 0 {
		panic("grouped sharded linked list cannot have 0 shards")
	}
	return &GroupedShardedLinkedList[K, V]{
		shardCount: shardCount,
		groups:     make(map[K]*ShardedLinkedList[V]),
	}
}

type GroupedShardedLinkedList[K comparable, V any] struct {
	shardCount int
	groups     map[K]*ShardedLinkedList[V]
	len        int
}

func (gsll *GroupedShardedLinkedList[K, V]) GroupIDs() (output []K) {
	output = slices.Collect(maps.Keys(gsll.groups))
	return
}

func (gsll *GroupedShardedLinkedList[K, V]) Len() int {
	return gsll.len
}

// GroupedShardedLinkedListNode are a [ShardedLinkedListNode]
// with a given group key.
type GroupedShardedLinkedListNode[K comparable, V any] struct {
	ShardedLinkedListNode[V]
	Group K
}

// Push adds a new value with a given key.
func (gsll *GroupedShardedLinkedList[K, V]) Push(group K, value V) *GroupedShardedLinkedListNode[K, V] {
	if _, ok := gsll.groups[group]; !ok {
		gsll.groups[group] = NewShardedLinkedList[V](gsll.shardCount)
	}
	node := gsll.groups[group].Push(value)
	gsll.len++
	return &GroupedShardedLinkedListNode[K, V]{
		*node,
		group,
	}
}

// Pop returns an element for a given list of groups, based on a random iteration
// of the underlying groups and lists.
func (gsll *GroupedShardedLinkedList[K, V]) Pop(groups ...K) (group K, out V, ok bool) {
	if gsll.len == 0 {
		return
	}
	var list *ShardedLinkedList[V]
	if len(groups) == 0 {
		// pick a random group with values
		for group, list = range gsll.groups {
			if list.len > 0 {
				break
			}
		}
	} else {
		var hasGroup bool
		for _, group = range groups {
			list, hasGroup = gsll.groups[group]
			if !hasGroup {
				continue
			}
			if list.len > 0 {
				break
			}
		}
	}
	if list == nil || list.len == 0 {
		return
	}
	gsll.len--
	out, ok = list.Pop()
	return
}

func (gsll *GroupedShardedLinkedList[K, V]) Remove(node *GroupedShardedLinkedListNode[K, V]) {
	gsll.groups[node.Group].Remove(&node.ShardedLinkedListNode)
	gsll.len--
}

// Clear clears the linked list.
func (gsll *GroupedShardedLinkedList[K, V]) Clear() {
	for _, group := range gsll.groups {
		group.Clear()
	}
	gsll.len = 0
}

// Each returns an iterator for each node in the grouped sharded linked list.
func (gsll *GroupedShardedLinkedList[K, V]) Each() iter.Seq[*GroupedShardedLinkedListNode[K, V]] {
	return func(yield func(*GroupedShardedLinkedListNode[K, V]) bool) {
		for groupID, group := range gsll.groups {
			for shardIndex, shard := range group.shards {
				for node := range shard.EachNode() {
					if !yield(&GroupedShardedLinkedListNode[K, V]{
						ShardedLinkedListNode[V]{
							ListNode:   *node,
							ShardIndex: uint32(shardIndex),
						},
						groupID,
					}) {
						return
					}
				}
			}
		}
	}
}
