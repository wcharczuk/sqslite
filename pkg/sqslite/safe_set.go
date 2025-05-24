package sqslite

import (
	"iter"
	"sync"
)

type SafeSet[T comparable] struct {
	storageMu sync.Mutex
	storage   map[T]struct{}
}

func (s *SafeSet[T]) Add(v T) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	s.storage[v] = struct{}{}
}

func (s *SafeSet[T]) Has(v T) (ok bool) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	_, ok = s.storage[v]
	return
}

// Consume returns an iterator that yields element of the set
// and purges the set once the iterator returns.
func (s *SafeSet[T]) Consume() iter.Seq[T] {
	return func(yield func(T) bool) {
		s.storageMu.Lock()
		defer s.storageMu.Unlock()
		for v := range s.storage {
			if !yield(v) {
				return
			}
		}
		s.storage = make(map[T]struct{})
	}

}

func (s *SafeSet[T]) Each() iter.Seq[T] {
	return func(yield func(T) bool) {
		s.storageMu.Lock()
		defer s.storageMu.Unlock()
		for v := range s.storage {
			if !yield(v) {
				return
			}
		}
	}
}

func (s *SafeSet[T]) Del(v T) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	delete(s.storage, v)
}
