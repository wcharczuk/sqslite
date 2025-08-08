package sqslite

type Set[K comparable] map[K]struct{}

func (s Set[K]) Add(v K) {
	s[v] = struct{}{}
}

func (s Set[K]) Has(v K) (ok bool) {
	_, ok = s[v]
	return
}

func (s Set[K]) Remove(v K) {
	delete(s, v)
}
