package sqslite

func Some[T any](v T) Optional[T] {
	return Optional[T]{v, true}
}

func SomePtr[T any](v *T) Optional[T] {
	if v == nil {
		return Optional[T]{}
	}
	return Optional[T]{*v, true}
}

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
