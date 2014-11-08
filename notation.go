package libovsdb

import (
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
)

// Operation represents an operation according to RFC7047 section 5.2
type Operation struct {
	Op        string                   `json:"op"`
	Table     string                   `json:"table"`
	Row       map[string]interface{}   `json:"row,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Columns   []string                 `json:"columns,omitempty"`
	Mutations []string                 `json:"mutations,omitempty"`
	Timeout   int                      `json:"timeout,omitempty"`
	Where     []string                 `json:"where,omitempty"`
	Until     string                   `json:"until,omitempty"`
	UUIDName  string                   `json:"uuid-name,omitempty"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string      `json:"columns,omitempty"`
	Select  MonitorSelect `json:"select,omitempty"`
}

// MonitorSelect represents a monitor select according to RFC7047
type MonitorSelect struct {
	Initial bool `json:"initial,omitempty"`
	Insert  bool `json:"insert,omitempty"`
	Delete  bool `json:"delete,omitempty"`
	Modify  bool `json:"modify,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

type UUID struct {
	uuid string `json:"uuid"`
}

// <set> notation requires special marshaling
func (u UUID) MarshalJSON() ([]byte, error) {
	var uuidSlice []string
	err := validateUUID(u.uuid)
	if err == nil {
		uuidSlice = []string{"uuid", u.uuid}
	} else {
		uuidSlice = []string{"named-uuid", u.uuid}
	}

	return json.Marshal(uuidSlice)
}

// NewCondition creates a new condition as specified in RFC7047
func NewCondition(column string, function string, value interface{}) []interface{} {
	return []interface{}{column, function, value}
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column string, mutator string, value interface{}) []interface{} {
	return []interface{}{column, mutator, value}
}

func validateUUID(uuid string) error {

	if len(uuid) != 36 {
		return errors.New("uuid exceeds 36 characters")
	}

	var validUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

	if !validUUID.MatchString(uuid) {
		return errors.New("uuid does not match regexp")
	}

	return nil
}

//  RFC 7047 has a wierd (but understandable) notation for set as described as :
//  Either an <atom>, representing a set with exactly one element, or
//  a 2-element JSON array that represents a database set value.  The
//  first element of the array must be the string "set", and the
//  second element must be an array of zero or more <atom>s giving the
//  values in the set.  All of the <atom>s must have the same type.

type OvsSet struct {
	ovsSet []interface{}
}

// <set> notation requires special handling
func newOvsSet(goSlice interface{}) (*OvsSet, error) {
	v := reflect.ValueOf(goSlice)
	if v.Kind() != reflect.Slice {
		return nil, errors.New("OvsSet supports only Go Slice types")
	}

	var ovsSet []interface{}
	for i := 0; i < v.Len(); i++ {
		ovsSet = append(ovsSet, v.Index(i).Interface())
	}
	return &OvsSet{ovsSet}, nil
}

// <set> notation requires special marshaling
func (o OvsSet) MarshalJSON() ([]byte, error) {
	var oSet []interface{}
	oSet = append(oSet, "set")
	oSet = append(oSet, o.ovsSet)
	return json.Marshal(oSet)
}

//  RFC 7047 uses the following notation for map as JSON doesnt support non-string keys for maps.
//  A 2-element JSON array that represents a database map value.  The
//  first element of the array must be the string "map", and the
//  second element must be an array of zero or more <pair>s giving the
//  values in the map.  All of the <pair>s must have the same key and
//  value types.

type OvsMap struct {
	goMap map[interface{}]interface{}
}

// <map> notation requires special handling
func (o OvsMap) MarshalJSON() ([]byte, error) {
	var ovsMap []interface{}
	ovsMap = append(ovsMap, "map")
	for key, val := range o.goMap {
		var mapSeg []interface{}
		mapSeg = append(mapSeg, key)
		mapSeg = append(mapSeg, val)
		ovsMap = append(ovsMap, mapSeg)
	}
	return json.Marshal(ovsMap)
}

// <map> notation requires special marshaling
func newOvsMap(goMap interface{}) (*OvsMap, error) {
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

// TODO : add Condition, Function, Mutation and Mutator notations
