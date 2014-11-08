package libovsdb

import (
	"encoding/json"
	"errors"
	"reflect"
)

//  RFC 7047 uses the following notation for map as JSON doesnt support non-string keys for maps.
//  A 2-element JSON array that represents a database map value.  The
//  first element of the array must be the string "map", and the
//  second element must be an array of zero or more <pair>s giving the
//  values in the map.  All of the <pair>s must have the same key and
//  value types.

type OvsMap struct {
	GoMap map[interface{}]interface{}
}

// <map> notation requires special handling
func (o OvsMap) MarshalJSON() ([]byte, error) {
	var ovsMap []interface{}
	ovsMap = append(ovsMap, "map")
	for key, val := range o.GoMap {
		var mapSeg []interface{}
		mapSeg = append(mapSeg, key)
		mapSeg = append(mapSeg, val)
		ovsMap = append(ovsMap, mapSeg)
	}
	return json.Marshal(ovsMap)
}

// <map> notation requires special marshaling
func NewOvsMap(goMap interface{}) (*OvsMap, error) {
	v := reflect.ValueOf(goMap)
	if v.Kind() != reflect.Map {
		return nil, errors.New("OvsMap supports only Go Map types")
	}

	genMap := make(map[interface{}]interface{})
	keys := v.MapKeys()
	for _, key := range keys {
		genMap[key.Interface()] = v.MapIndex(key).Interface()
	}
	return &OvsMap{genMap}, nil
}
