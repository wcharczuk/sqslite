package sqslite

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
