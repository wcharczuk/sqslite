package sqslite

import "iter"

func NewOrderedSet[T comparable]() *OrderedSet[T] {
	return &OrderedSet[T]{
		ordered: new(LinkedList[T]),
		lookup:  make(map[T]*LinkedListNode[T]),
	}
}

type OrderedSet[T comparable] struct {
	ordered *LinkedList[T]
	lookup  map[T]*LinkedListNode[T]
}

func (os *OrderedSet[T]) Add(v T) {
	n := os.ordered.Push(v)
	os.lookup[v] = n
}

func (os *OrderedSet[T]) Has(v T) (ok bool) {
	_, ok = os.lookup[v]
	return
}

func (os *OrderedSet[T]) Del(v T) (ok bool) {
	var node *LinkedListNode[T]
	node, ok = os.lookup[v]
	if !ok {
		return
	}
	os.ordered.Remove(node)
	delete(os.lookup, v)
	return
}

func (os *OrderedSet[T]) InOrder() iter.Seq[T] {
	return os.ordered.Each()
}
