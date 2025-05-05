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
