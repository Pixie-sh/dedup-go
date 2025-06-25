package dedup

import (
	"reflect"
)

// nameOf returns the canonical name of the type T
func nameOf[T any]() string {
	var t T
	typ := reflect.TypeOf(t)
	if typ == nil {
		return "nil"
	}

	return typ.String()
}

// IsEmpty checks if the given struct of type T has zero values for all its fields.
func IsEmpty[T any](i T) bool {
	return reflect.DeepEqual(i, reflect.Zero(reflect.TypeOf(i)).Interface())
}
