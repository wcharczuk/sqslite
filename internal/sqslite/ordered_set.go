package sqslite

import "iter"

func NewOrderedSet[T comparable]() *OrderedSet[T] {
	return &OrderedSet[T]{
		ordered: new(List[T]),
		lookup:  make(map[T]*ListNode[T]),
	}
}

type OrderedSet[T comparable] struct {
	ordered *List[T]
	lookup  map[T]*ListNode[T]
}

func (os *OrderedSet[T]) Len() int {
	return len(os.lookup)
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
	var node *ListNode[T]
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
