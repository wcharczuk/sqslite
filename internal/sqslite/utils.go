package sqslite

import "fmt"

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}

func coalesceZero[V comparable](values ...V) V {
	var zero V
	for _, v := range values {
		if v != zero {
			return v
		}
	}
	return zero
}

func apply[Input, Output any](values []Input, fn func(Input) Output) (output []Output) {
	output = make([]Output, len(values))
	for index, input := range values {
		output[index] = fn(input)
	}
	return
}

func distinct[V comparable](values []V) (output []V) {
	lookup := map[V]struct{}{}
	output = make([]V, 0, len(values))
	for _, v := range values {
		if _, ok := lookup[v]; ok {
			continue
		}
		lookup[v] = struct{}{}
		output = append(output, v)
	}
	return
}

func flatten[V any](values [][]V) (output []V) {
	for _, list := range values {
		output = append(output, list...)
	}
	return
}

func keysAndValues[K comparable, V any](m map[K]V) (output []string) {
	output = make([]string, 0, len(m)<<1)
	for k, v := range m {
		output = append(output, fmt.Sprint(k), fmt.Sprint(v))
	}
	return
}

func safeDeref[T any](valuePtr *T) (output T) {
	if valuePtr != nil {
		output = *valuePtr
	}
	return
}
