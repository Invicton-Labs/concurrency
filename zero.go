package concurrency

import "reflect"

func zeroDefault[T any](val T, dflt T) T {
	if reflect.ValueOf(val).IsZero() {
		return dflt
	}
	return val
}
