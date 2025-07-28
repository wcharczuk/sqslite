package sqslite

import (
	"iter"
	"maps"
	"math/rand/v2"
	"slices"
)

func newGroupedShardedLinkedList[K comparable, V any](shardCount int) *groupedShardedLinkedList[K, V] {
	if shardCount == 0 {
		panic("grouped sharded linked list cannot have 0 shards")
	}
	return &groupedShardedLinkedList[K, V]{
		shardCount: shardCount,
		groups:     make(map[K]*ShardedLinkedList[V]),
	}
}

type groupedShardedLinkedList[K comparable, V any] struct {
	shardCount int
	groups     map[K]*ShardedLinkedList[V]
	len        int
}

func (gsll *groupedShardedLinkedList[K, V]) GroupIDs() (output []K) {
	output = slices.Collect(maps.Keys(gsll.groups))
	return
}

func (gsll *groupedShardedLinkedList[K, V]) Len() int {
	return gsll.len
}

// GroupedShardedLinkedListNode are a [ShardedLinkedListNode]
// with a given group key.
type GroupedShardedLinkedListNode[K comparable, V any] struct {
	ShardedLinkedListNode[V]
	Group K
}

// Push adds a new value with a given key.
func (gsll *groupedShardedLinkedList[K, V]) Push(group K, value V) *GroupedShardedLinkedListNode[K, V] {
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
func (gsll *groupedShardedLinkedList[K, V]) Pop(groups ...K) (group K, out V, ok bool) {
	if gsll.len == 0 {
		return
	}
	list, hasList := gsll.getRandomGroupWithMessages(groups...)
	if !hasList || list == nil || list.len == 0 {
		return
	}
	gsll.len--
	out, ok = list.Pop()
	return
}

func (gsll *groupedShardedLinkedList[K, V]) getRandomGroupWithMessages(groups ...K) (output *ShardedLinkedList[V], ok bool) {
	if len(groups) == 0 {
		keys := slices.Collect(maps.Keys(gsll.groups))
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
		for _, groupID := range keys {
			list, hasList := gsll.groups[groupID]
			if !hasList {
				continue
			}
			if list.len > 0 {
				output = list
				ok = true
				return
			}
		}
	} else {
		rand.Shuffle(len(groups), func(i, j int) {
			groups[i], groups[j] = groups[j], groups[i]
		})
		for _, groupID := range groups {
			list, hasList := gsll.groups[groupID]
			if !hasList {
				continue
			}
			if list.len > 0 {
				output = list
				ok = true
				return
			}
		}
	}
	return
}

func (gsll *groupedShardedLinkedList[K, V]) Remove(node *GroupedShardedLinkedListNode[K, V]) {
	gsll.groups[node.Group].Remove(&node.ShardedLinkedListNode)
	gsll.len--
}

// Clear clears the linked list.
func (gsll *groupedShardedLinkedList[K, V]) Clear() {
	for _, group := range gsll.groups {
		group.Clear()
	}
	gsll.len = 0
}

// Each returns an iterator for each node in the grouped sharded linked list.
func (gsll *groupedShardedLinkedList[K, V]) Each() iter.Seq[*GroupedShardedLinkedListNode[K, V]] {
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
