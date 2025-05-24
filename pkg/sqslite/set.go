package sqslite

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(v T) {
	if s == nil {
		s = make(map[T]struct{})
	}
	s[v] = struct{}{}
	return
}

func (s Set[T]) Has(v T) (ok bool) {
	if s == nil {
		return
	}
	_, ok = s[v]
	return
}

func (s Set[T]) Del(v T) {
	if s == nil {
		return
	}
	delete(s, v)
	return
}
