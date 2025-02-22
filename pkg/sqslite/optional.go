package sqslite

func Some[T any](v T) Optional[T] {
	return Optional[T]{v, true}
}

// None returns none.
func None[T any]() Optional[T] {
	return Optional[T]{}
}

type Optional[T any] struct {
	Value T
	IsSet bool
}

func (o Optional[T]) IsZero() bool {
	return !o.IsSet
}
