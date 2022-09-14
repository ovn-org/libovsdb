package ovsdb

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// OvsSet is an OVSDB style set
// RFC 7047 has a weird (but understandable) notation for set as described as :
// Either an <atom>, representing a set with exactly one element, or
// a 2-element JSON array that represents a database set value.  The
// first element of the array must be the string "set", and the
// second element must be an array of zero or more <atom>s giving the
// values in the set.  All of the <atom>s must have the same type, and all
// values must be unique within the set.
type OvsSet struct {
	goSet []interface{}
}

func getUUID(val interface{}) (UUID, error) {
	uuid, ok := val.(UUID)
	if ok {
		return uuid, nil
	}
	str, ok := val.(string)
	if ok {
		return UUID{GoUUID: str}, nil
	}
	return UUID{}, fmt.Errorf("expected UUID or string but got %T", val)
}

// NewOvsSet creates a new OVSDB style set from a Go interface (object)
func NewOvsSet(keyType string, obj interface{}) (OvsSet, error) {
	ovsSet := make([]interface{}, 0)
	var v reflect.Value
	if reflect.TypeOf(obj).Kind() == reflect.Ptr {
		v = reflect.ValueOf(obj).Elem()
		if v.Kind() == reflect.Invalid {
			// must be a nil pointer, so just return an empty set
			return OvsSet{goSet: ovsSet}, nil
		}
	} else {
		v = reflect.ValueOf(obj)
	}

	switch v.Kind() {
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if keyType == TypeUUID {
				uuid, err := getUUID(v.Index(i).Interface())
				if err != nil {
					return OvsSet{}, err
				}
				ovsSet = append(ovsSet, uuid)
			} else {
				ovsSet = append(ovsSet, v.Index(i).Interface())
			}
		}
	case reflect.String:
		if keyType == TypeUUID {
			uuid, err := getUUID(v.Interface())
			if err != nil {
				return OvsSet{}, err
			}
			ovsSet = append(ovsSet, uuid)
		} else {
			ovsSet = append(ovsSet, v.Interface())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool:
		ovsSet = append(ovsSet, v.Interface())
	case reflect.Struct:
		if v.Type() == reflect.TypeOf(UUID{}) {
			ovsSet = append(ovsSet, v.Interface())
		} else {
			return OvsSet{}, fmt.Errorf("ovsset supports only go slice/string/numbers/uuid or pointers to those types")
		}
	default:
		return OvsSet{}, fmt.Errorf("ovsset supports only go slice/string/numbers/uuid or pointers to those types")
	}
	return OvsSet{goSet: ovsSet}, nil
}

// MarshalJSON wil marshal an OVSDB style Set in to a JSON byte array
func (o OvsSet) MarshalJSON() ([]byte, error) {
	switch l := len(o.goSet); {
	case l == 1:
		return json.Marshal(o.goSet[0])
	case l > 0:
		var oSet []interface{}
		oSet = append(oSet, "set")
		oSet = append(oSet, o.goSet)
		return json.Marshal(oSet)
	}
	return []byte("[\"set\",[]]"), nil
}

// UnmarshalJSON will unmarshal a JSON byte array to an OVSDB style Set
func (o *OvsSet) UnmarshalJSON(b []byte) (err error) {
	o.goSet = make([]interface{}, 0)
	addToSet := func(o *OvsSet, v interface{}) error {
		goVal, err := ovsSliceToGoNotation(v)
		if err == nil {
			o.goSet = append(o.goSet, goVal)
		}
		return err
	}

	var inter interface{}
	if err = json.Unmarshal(b, &inter); err != nil {
		return err
	}
	switch inter.(type) {
	case []interface{}:
		var oSet []interface{}
		oSet = inter.([]interface{})
		// it's a single uuid object
		if len(oSet) == 2 && (oSet[0] == "uuid" || oSet[0] == "named-uuid") {
			return addToSet(o, UUID{GoUUID: oSet[1].(string)})
		}
		if oSet[0] != "set" {
			// it is a slice, but is not a set
			return &json.UnmarshalTypeError{Value: reflect.ValueOf(inter).String(), Type: reflect.TypeOf(*o)}
		}
		innerSet := oSet[1].([]interface{})
		for _, val := range innerSet {
			err := addToSet(o, val)
			if err != nil {
				return err
			}
		}
		return err
	default:
		// it is a single object
		return addToSet(o, inter)
	}
}

func (o *OvsSet) Append(newVal ...interface{}) error {
	o.goSet = append(o.goSet, newVal...)
	return nil
}

func (o *OvsSet) Len() int {
	return len(o.goSet)
}

func (o *OvsSet) Replace(idx int, newVal interface{}) error {
	if idx > len(o.goSet)-1 {
		return fmt.Errorf("attempted to access element %d beyond end of array (length %d)", idx, len(o.goSet))
	}
	o.goSet[idx] = newVal
	return nil
}

// HasElementType matches the given value's type with the set's element type.
// It returns true if the set has at least one element, and that element is
// of the given type, otherwise false.
func (o *OvsSet) HasElementType(checkVal interface{}) bool {
	if len(o.goSet) == 0 {
		return false
	}
	return reflect.ValueOf(checkVal).Type() == reflect.ValueOf(o.goSet[0]).Type()
}

// Range iterates over elements of the set and calls the given function for
// each element. The function should return true if iteration should terminate,
// a value to return to the caller of Range(), and/or an error (which also
// terminates iteration).
func (o *OvsSet) Range(elemFn func(int, interface{}) (bool, error)) error {
	for i, v := range o.goSet {
		done, err := elemFn(i, v)
		if err != nil {
			return err
		} else if done {
			return nil
		}
	}
	return nil
}

func SetDifference(a, b OvsSet) (OvsSet, bool) {
	if a.Len() == 0 && b.Len() == 0 {
		return a, false
	} else if a.Len() == 0 && b.Len() != 0 {
		return b, b.Len() != 0
	} else if b.Len() == 0 && a.Len() != 0 {
		return a, a.Len() != 0
	}

	// From https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/#update2-notification
	// The difference between two sets are all elements that only belong to one
	// of the sets.
	difference := make(map[interface{}]struct{}, b.Len())
	for i := 0; i < b.Len(); i++ {
		// supossedly we are working with comparable atomic types with no
		// pointers so we can use the values as map key
		difference[b.goSet[i]] = struct{}{}
	}
	j := a.Len()
	for i := 0; i < j; {
		vi := a.goSet[i]
		if _, ok := difference[vi]; ok {
			// this value of 'a' is in 'b', so remove it from 'a'; to do that,
			// overwrite it with the last value and re-evaluate
			a.goSet[i] = a.goSet[j-1]
			// decrease where the last 'a' value is at
			j--
			// remove from 'b' values
			delete(difference, vi)
		} else {
			// this value of 'a' is not in 'b', evaluate the next value
			i++
		}
	}
	// trim the slice to the actual values held
	a.goSet = a.goSet[:j]
	for item := range difference {
		a.goSet = append(a.goSet, item)
	}

	if a.Len() == 0 {
		return a, false
	}

	return a, true
}
