package sqslite

import "iter"

type LinkedList[V any] struct {
	head *LinkedListNode[V]
	tail *LinkedListNode[V]
	len  int
}

// LinkedListNode is a linked list node.
type LinkedListNode[T any] struct {
	// Value holds the value of the node.
	Value T
	// Next points towards the tail.
	Next *LinkedListNode[T]
	// Previous points towards the head.
	Previous *LinkedListNode[T]
}

// Len returns the length of the list in constant time.
func (l *LinkedList[T]) Len() int {
	return l.len
}

// Push appends a node to the end, or tail, of the list.
func (l *LinkedList[T]) Push(value T) *LinkedListNode[T] {
	item := &LinkedListNode[T]{
		Value: value,
	}
	l.len++
	if l.head == nil {
		l.head = item
		l.tail = item
		return item
	}

	l.tail.Next = item
	item.Previous = l.tail
	item.Next = nil
	l.tail = item
	return item
}

// Pop removes the head element from the list.
func (l *LinkedList[T]) Pop() (out T, ok bool) {
	if l.head == nil {
		return
	}

	out = l.head.Value
	ok = true

	l.len--
	if l.head == l.tail {
		l.head = nil
		l.tail = nil
		return
	}

	next := l.head.Next
	next.Previous = nil
	l.head = next
	return
}

// PopNode removes the head node from the list (which contains the element).
func (l *LinkedList[T]) PopNode() (out *LinkedListNode[T], ok bool) {
	if l.head == nil {
		return
	}

	out = l.head
	ok = true

	l.len--
	if l.head == l.tail {
		l.head = nil
		l.tail = nil
		return
	}

	next := l.head.Next
	next.Previous = nil
	l.head = next
	return
}

// Clear clears the linked list.
func (l *LinkedList[T]) Clear() {
	l.tail = nil
	l.head = nil
	l.len = 0
}

// Each returns an iterator that walks the list from head to tail.
func (q *LinkedList[T]) Each() iter.Seq[T] {
	return func(yield func(T) bool) {
		nodePtr := q.head
		for nodePtr != nil {
			if !yield(nodePtr.Value) {
				return
			}
			nodePtr = nodePtr.Next
		}
	}
}

// EachNode returns an iterator that walks the list from head to tail.
func (q *LinkedList[T]) EachNode() iter.Seq[*LinkedListNode[T]] {
	return func(yield func(*LinkedListNode[T]) bool) {
		nodePtr := q.head
		for nodePtr != nil {
			if !yield(nodePtr) {
				return
			}
			nodePtr = nodePtr.Next
		}
	}
}

// Consume returns an iterator that pops and yields elements in the list from head to tail.
func (q *LinkedList[T]) Consume() iter.Seq[T] {
	return func(yield func(T) bool) {
		v, ok := q.Pop()
		if !ok {
			return
		}
		if !yield(v) {
			return
		}
		for ok {
			v, ok = q.Pop()
			if !ok {
				return
			}
			if !yield(v) {
				return
			}

		}
	}
}

// ConsumeNode returns an iterator that pops and yields nodes in the list from head to tail.
func (q *LinkedList[T]) ConsumeNode() iter.Seq[*LinkedListNode[T]] {
	return func(yield func(*LinkedListNode[T]) bool) {
		v, ok := q.PopNode()
		if !ok {
			return
		}
		if !yield(v) {
			return
		}
		for ok {
			v, ok = q.PopNode()
			if !ok {
				return
			}
			if !yield(v) {
				return
			}

		}
	}
}

func (l *LinkedList[T]) Remove(i *LinkedListNode[T]) {
	l.len--

	// three possibilities
	// - i is both the head and the tail
	// 		- nil out both
	// - i is the head
	// 		- set the head to i's next
	// - i is the tail
	//		- set the tail to i's previous
	// - i is neither
	//		- if i has a next, set its previous to i's previous
	//		- if i has a previous, set its previous to i's next

	if l.head == i && l.tail == i {
		l.head = nil
		l.tail = nil
		return
	}
	if l.head == i {
		l.head = i.Next
		return
	}
	if l.tail == i {
		l.tail = i.Previous
		return
	}

	next := i.Next
	if next != nil {
		next.Previous = i.Previous
	}
	previous := i.Previous
	if previous != nil {
		previous.Next = i.Next
	}
}
