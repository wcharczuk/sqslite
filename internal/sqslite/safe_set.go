package sqslite

import (
	"iter"
	"sync"
)

func NewSafeSet[T comparable]() *SafeSet[T] {
	return &SafeSet[T]{storage: make(map[T]struct{})}
}

type SafeSet[T comparable] struct {
	storageMu sync.Mutex
	storage   map[T]struct{}
}

func (s *SafeSet[T]) Len() (output int) {
	s.storageMu.Lock()
	output = len(s.storage)
	s.storageMu.Unlock()
	return
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

func (s *SafeSet[T]) Consume() iter.Seq[T] {
	return func(yield func(T) bool) {
		s.storageMu.Lock()
		defer s.storageMu.Unlock()
		var toDelete []T
		for v := range s.storage {
			if !yield(v) {
				return
			}
			toDelete = append(toDelete, v)
		}
		for _, v := range toDelete {
			delete(s.storage, v)
		}
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
