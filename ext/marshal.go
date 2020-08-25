package ext

import (
	"reflect"
	"time"
)

// type ParquetMarshaler interface {
//   MarshalParquet() interface{}
// }

func CustomMarshal(v reflect.Value) reflect.Value {
	// Check if struct has a Method called MarshalParquet and returns first output parameter
	if m := v.MethodByName("MarshalParquet"); m.IsValid() {
		ret := m.Call(nil)
		return ret[0]
	}

	switch m := v.Interface().(type) {
	case time.Time:
		return reflect.ValueOf(m.UnixNano() / int64(time.Millisecond))

	default:
		return v
	}
}
