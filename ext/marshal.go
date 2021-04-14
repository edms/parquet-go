package ext

import (
	"database/sql"
	"reflect"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/xitongsys/parquet-go/types"
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
	case sql.NullBool:
		if !m.Valid {
			var val *bool
			return reflect.ValueOf(val)
		}

		return reflect.ValueOf(m.Bool)

	case sql.NullFloat64:
		if !m.Valid {
			var val *float64
			return reflect.ValueOf(val)
		}

		return reflect.ValueOf(m.Float64)

	case sql.NullInt32:
		if !m.Valid {
			var val *int32
			return reflect.ValueOf(val)
		}

		return reflect.ValueOf(m.Int32)

	case sql.NullInt64:
		if !m.Valid {
			var val *int64
			return reflect.ValueOf(val)
		}

		return reflect.ValueOf(m.Int64)

	case sql.NullString:
		if !m.Valid {
			var val *string
			return reflect.ValueOf(val)
		}

		return reflect.ValueOf(m.String)

	case sql.NullTime:
		if !m.Valid {
			var val *int64
			return reflect.ValueOf(val)
		}

		return reflect.ValueOf(m.Time)

	case time.Time:
		return reflect.ValueOf(int64(time.Nanosecond) * m.UnixNano() / int64(time.Microsecond))

	case TimeMillis:
		return reflect.ValueOf(m.Time.UnixNano() / int64(time.Millisecond))

	case decimal.Decimal:
		num := strings.Replace(m.StringFixed(9), ".", "", -1)
		return reflect.ValueOf(types.StrIntToBinary(num, "BigEndian", 16, true))

	default:
		return v
	}
}

// MarshalType returns the custom marshal type for Parquet and if it a required field
func MarshalType(f reflect.StructField) (reflect.Type, bool) {
	if m, ok := f.Type.MethodByName("MarshalParquet"); ok {
		return m.Type.Out(0), true
	}

	pkgPath := f.Type.PkgPath()
	ty := f.Type.String()

	if pkgPath == "database/sql" {
		switch ty {
		case "sql.NullBool":
			return reflect.TypeOf(true), false
		case "sql.NullFloat64":
			return reflect.TypeOf(float64(0.0)), false
		case "sql.NullInt32":
			return reflect.TypeOf(int32(0)), false
		case "sql.NullInt64":
			return reflect.TypeOf(int64(0)), false
		case "sql.NullString":
			return reflect.TypeOf(string("")), false
		case "sql.NullTime":
			return reflect.TypeOf(int64(0)), false // timestamp micros
		}
	} else if pkgPath == "time" && ty == "time.Time" {
		return reflect.TypeOf(int64(0)), true // timestamp micros
	} else if pkgPath == "github.com/edms/parquet-go" && ty == "ext.TimeMillis" {
		return reflect.TypeOf(int64(0)), true // timestamp millis
	} else if pkgPath == "github.com/shopspring/decimal" && ty == "decimal.Decimal" {
		return reflect.TypeOf(string("")), true
	}

	return f.Type, true
}
