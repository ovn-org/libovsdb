package libovsdb

import (
	"fmt"
	"reflect"
)

var (
	intType  = reflect.TypeOf(0)
	realType = reflect.TypeOf(0.0)
	boolType = reflect.TypeOf(true)
	strType  = reflect.TypeOf("")
)

// ErrWrongType describes typing error
type ErrWrongType struct {
	from     string
	expected string
	got      interface{}
}

func (e *ErrWrongType) Error() string {
	return fmt.Sprintf("Wrong Type (%s): expected %s but got %s (%s)",
		e.from, e.expected, e.got, reflect.TypeOf(e.got))
}

// NewErrWrongType creates a new ErrWrongType
func NewErrWrongType(from, expected string, got interface{}) error {
	return &ErrWrongType{
		from:     from,
		expected: expected,
		got:      got,
	}
}

// nativeTypeFromBasic returns the native type that can hold a value of an
// BasicType type
func nativeTypeFromBasic(basicType string) reflect.Type {
	switch basicType {
	case TypeInteger:
		return intType
	case TypeReal:
		return realType
	case TypeBoolean:
		return boolType
	case TypeString:
		return strType
	case TypeUUID:
		return strType
	default:
		panic("Unkown basic type %s basicType")
	}
}

//nativeType returns the reflect.Type that can hold the value of a column
//OVS Type to Native Type convertions:
// OVS sets -> go slices
// OVS uuid -> go strings
// OVS map  -> go map
// OVS enum -> go native type depending on the type of the enum key
func nativeType(column *ColumnSchema) reflect.Type {
	switch column.Type {
	case TypeInteger, TypeReal, TypeBoolean, TypeUUID, TypeString:
		return nativeTypeFromBasic(column.Type)
	case TypeEnum:
		return nativeTypeFromBasic(column.TypeObj.Key.Type)
	case TypeMap:
		kType := nativeTypeFromBasic(column.TypeObj.Key.Type)
		vType := nativeTypeFromBasic(column.TypeObj.Value.Type)
		return reflect.MapOf(kType, vType)
	case TypeSet:
		kType := nativeTypeFromBasic(column.TypeObj.Key.Type)
		return reflect.SliceOf(kType)
	default:
		panic(fmt.Errorf("unknown extended type %s", column.Type))
	}
}

// OvsToNativeBasic returns the native type of the basic ovs type
func OvsToNativeBasic(basicType string, ovsElem interface{}) (interface{}, error) {
	switch basicType {
	case TypeReal, TypeString, TypeBoolean:
		naType := nativeTypeFromBasic(basicType)
		if reflect.TypeOf(ovsElem) != naType {
			return nil, NewErrWrongType("OvsToNativeBasic", naType.String(), ovsElem)
		}
		return ovsElem, nil
	case TypeInteger:
		naType := nativeTypeFromBasic(basicType)
		// Default decoding of numbers is float64, convert them to int
		if !reflect.TypeOf(ovsElem).ConvertibleTo(naType) {
			return nil, NewErrWrongType("OvsToNative", fmt.Sprintf("Convertible to %s", naType), ovsElem)
		}
		return reflect.ValueOf(ovsElem).Convert(naType).Interface(), nil
	case TypeUUID:
		uuid, ok := ovsElem.(UUID)
		if !ok {
			return nil, NewErrWrongType("OvsToNativeBasic", "UUID", ovsElem)
		}
		return uuid.GoUUID, nil
	default:
		panic(fmt.Errorf("unknown atomic type %s", basicType))
	}
}

// OvsToNative transforms an ovs type to native one based on the column type information
func OvsToNative(column *ColumnSchema, ovsElem interface{}) (interface{}, error) {
	switch column.Type {
	case TypeReal, TypeString, TypeBoolean, TypeInteger, TypeUUID:
		return OvsToNativeBasic(column.Type, ovsElem)
	case TypeEnum:
		return OvsToNativeBasic(column.TypeObj.Key.Type, ovsElem)
	case TypeSet:
		naType := nativeType(column)
		// The inner slice is []interface{}
		// We need to convert it to the real type os slice
		var nativeSet reflect.Value

		// RFC says that for a set of exactly one, an atomic type an be sent
		switch ovsSet := ovsElem.(type) {
		case OvsSet:
			nativeSet = reflect.MakeSlice(naType, 0, len(ovsSet.GoSet))
			for _, v := range ovsSet.GoSet {
				nv, err := OvsToNativeBasic(column.TypeObj.Key.Type, v)
				if err != nil {
					return nil, err
				}
				nativeSet = reflect.Append(nativeSet, reflect.ValueOf(nv))
			}

		default:
			nativeSet = reflect.MakeSlice(naType, 0, 1)
			nv, err := OvsToNativeBasic(column.TypeObj.Key.Type, ovsElem)
			if err != nil {
				return nil, err
			}

			nativeSet = reflect.Append(nativeSet, reflect.ValueOf(nv))
		}
		return nativeSet.Interface(), nil

	case TypeMap:
		naType := nativeType(column)
		ovsMap, ok := ovsElem.(OvsMap)
		if !ok {
			return nil, NewErrWrongType("OvsToNative", "OvsMap", ovsElem)
		}
		// The inner slice is map[interface]interface{}
		// We need to convert it to the real type os slice
		nativeMap := reflect.MakeMapWithSize(naType, len(ovsMap.GoMap))
		for k, v := range ovsMap.GoMap {
			nk, err := OvsToNativeBasic(column.TypeObj.Key.Type, k)
			if err != nil {
				return nil, err
			}
			nv, err := OvsToNativeBasic(column.TypeObj.Value.Type, v)
			if err != nil {
				return nil, err
			}
			nativeMap.SetMapIndex(reflect.ValueOf(nk), reflect.ValueOf(nv))
		}
		return nativeMap.Interface(), nil
	default:
		panic(fmt.Sprintf("Unknown Type: %v", column.Type))
	}
}

// NativeToOvs transforms an native type to a ovs type based on the column type information
func NativeToOvs(column *ColumnSchema, rawElem interface{}) (interface{}, error) {
	naType := nativeType(column)

	if t := reflect.TypeOf(rawElem); t != naType {
		return nil, NewErrWrongType("NativeToOvs", naType.String(), rawElem)
	}

	switch column.Type {
	case TypeInteger, TypeReal, TypeString, TypeBoolean, TypeEnum:
		return rawElem, nil
	case TypeUUID:
		return UUID{GoUUID: rawElem.(string)}, nil
	case TypeSet:
		var ovsSet *OvsSet
		if column.TypeObj.Key.Type == TypeUUID {
			var ovsSlice []interface{}
			for _, v := range rawElem.([]string) {
				uuid := UUID{GoUUID: v}
				ovsSlice = append(ovsSlice, uuid)
			}
			ovsSet = &OvsSet{GoSet: ovsSlice}

		} else {
			var err error
			ovsSet, err = NewOvsSet(rawElem)
			if err != nil {
				return nil, err
			}
		}
		return ovsSet, nil
	case TypeMap:
		ovsMap, err := NewOvsMap(rawElem)
		if err != nil {
			return nil, err
		}
		return ovsMap, nil
	default:
		panic(fmt.Sprintf("Unknown Type: %v", column.Type))
	}
}
