package sqslite

import "iter"

type LinkedList[V any] struct {
	head *LinkedListNode[V]
	tail *LinkedListNode[V]
	len  int
}

// Len returns the length of the list in constant time.
func (l *LinkedList[T]) Len() int {
	return l.len
}

// PushBack adds a new value to the front of the list.
func (l *LinkedList[T]) PushFront(value T) *LinkedListNode[T] {
	item := &LinkedListNode[T]{Value: value}
	l.len++
	if l.head == nil {
		l.head = item
		l.tail = item
		return item
	}

	l.head.Previous = item
	item.Next = l.head
	item.Previous = nil
	l.head = item
	return item
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

// PopBack removes the last, or tail, element of the list.
func (l *LinkedList[T]) PopBack() (out T, ok bool) {
	if l.tail == nil {
		return
	}

	out = l.tail.Value
	ok = true

	l.len--
	if l.tail == l.head {
		l.head = nil
		l.tail = nil
		return
	}

	previous := l.tail.Previous
	previous.Next = nil
	l.tail = previous
	return
}

// PopAll removes all the elements, returning a slice.
func (l *LinkedList[T]) PopAll() (output []T) {
	ptr := l.head
	for ptr != nil {
		output = append(output, ptr.Value)
		ptr = ptr.Next
	}
	l.head = nil
	l.tail = nil
	l.len = 0
	return
}

// Peek returns the first element of the list but does not remove it.
func (q *LinkedList[T]) Peek() (out T, ok bool) {
	if q.head == nil {
		return
	}
	out = q.head.Value
	ok = true
	return
}

// PeekBack returns the last element of the list.
func (q *LinkedList[T]) PeekBack() (out T, ok bool) {
	if q.tail == nil {
		return
	}
	out = q.tail.Value
	ok = true
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

// ReverseEach returns an iterator that walks the list from tail to head.
func (q *LinkedList[T]) ReverseEach() iter.Seq[T] {
	return func(yield func(T) bool) {
		nodePtr := q.tail
		for nodePtr != nil {
			if !yield(nodePtr.Value) {
				return
			}
			nodePtr = nodePtr.Previous
		}
	}
}

// ReverseEach returns an iterator that walks the list from tail to head.
func (q *LinkedList[T]) ReverseEachNode() iter.Seq[*LinkedListNode[T]] {
	return func(yield func(*LinkedListNode[T]) bool) {
		nodePtr := q.tail
		for nodePtr != nil {
			if !yield(nodePtr) {
				return
			}
			nodePtr = nodePtr.Previous
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

// LinkedListNode is a linked list node.
type LinkedListNode[T any] struct {
	// Value holds the value of the node.
	Value T
	// Next points towards the tail.
	Next *LinkedListNode[T]
	// Previous points towards the head.
	Previous *LinkedListNode[T]
}
