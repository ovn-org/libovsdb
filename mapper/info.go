package mapper

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/ovn-org/libovsdb/ovsdb"
)

const (
	ovsdbStructTag  = "ovsdb"
	omitUnsupported = "omitunsupported"
)

// ErrOmitted is returned when an operation has been performed on a
// column that has been omitted due to not being available in the runtime schema
var ErrOmitted = errors.New("column is not available in runtime schema")

// Info is a struct that wraps an object with its metadata
type Info struct {
	// FieldName indexed by column
	Obj      interface{}
	Metadata Metadata
}

// Metadata represents the information needed to know how to map OVSDB columns into an objetss fields
type Metadata struct {
	Fields        map[string]string  // Map of ColumnName -> FieldName
	OmittedFields map[string]string  // Map of ColumnName -> Empty Struct
	TableSchema   *ovsdb.TableSchema // TableSchema associated
	TableName     string             // Table name
}

// FieldByColumn returns the field value that corresponds to a column
func (i *Info) FieldByColumn(column string) (interface{}, error) {
	if _, ok := i.Metadata.OmittedFields[column]; ok {
		return nil, ErrOmitted
	}
	fieldName, ok := i.Metadata.Fields[column]
	if !ok {
		return nil, fmt.Errorf("FieldByColumn: column %s not found in mapper info", column)
	}
	return reflect.ValueOf(i.Obj).Elem().FieldByName(fieldName).Interface(), nil
}

// hasColumn returns whether a column is present
func (i *Info) hasColumn(column string) bool {
	_, ok := i.Metadata.Fields[column]
	return ok
}

// SetField sets the field in the column to the specified value
func (i *Info) SetField(column string, value interface{}) error {
	if _, ok := i.Metadata.OmittedFields[column]; ok {
		return ErrOmitted
	}
	fieldName, ok := i.Metadata.Fields[column]
	if !ok {
		return fmt.Errorf("SetField: column %s not found in orm info", column)
	}
	fieldValue := reflect.ValueOf(i.Obj).Elem().FieldByName(fieldName)

	if !fieldValue.Type().AssignableTo(reflect.TypeOf(value)) {
		return fmt.Errorf("column %s: native value %v (%s) is not assignable to field %s (%s)",
			column, value, reflect.TypeOf(value), fieldName, fieldValue.Type())
	}
	fieldValue.Set(reflect.ValueOf(value))
	return nil
}

// ColumnByPtr returns the column name that corresponds to the field by the field's pointer
func (i *Info) ColumnByPtr(fieldPtr interface{}) (string, error) {
	fieldPtrVal := reflect.ValueOf(fieldPtr)
	if fieldPtrVal.Kind() != reflect.Ptr {
		return "", ovsdb.NewErrWrongType("ColumnByPointer", "pointer to a field in the struct", fieldPtr)
	}
	offset := fieldPtrVal.Pointer() - reflect.ValueOf(i.Obj).Pointer()
	objType := reflect.TypeOf(i.Obj).Elem()
	for j := 0; j < objType.NumField(); j++ {
		if objType.Field(j).Offset == offset {
			field := objType.Field(j)
			column, omit := parseStructTag(field)
			if omit {
				return "", ErrOmitted
			}
			if _, ok := i.Metadata.Fields[column]; !ok {
				return "", fmt.Errorf("field does not have orm column information")
			}
			return column, nil
		}
	}
	return "", fmt.Errorf("field pointer does not correspond to orm struct")
}

// getValidIndexes inspects the object and returns the a list of indexes (set of columns) for witch
// the object has non-default values
func (i *Info) getValidIndexes() ([][]string, error) {
	var validIndexes [][]string
	var possibleIndexes [][]string

	possibleIndexes = append(possibleIndexes, []string{"_uuid"})
	possibleIndexes = append(possibleIndexes, i.Metadata.TableSchema.Indexes...)

	// Iterate through indexes and validate them
OUTER:
	for _, idx := range possibleIndexes {
		for _, col := range idx {
			if !i.hasColumn(col) {
				continue OUTER
			}
			columnSchema := i.Metadata.TableSchema.Column(col)
			if columnSchema == nil {
				continue OUTER
			}
			field, err := i.FieldByColumn(col)
			if err != nil {
				return nil, err
			}
			if !reflect.ValueOf(field).IsValid() || ovsdb.IsDefaultValue(columnSchema, field) {
				continue OUTER
			}
		}
		validIndexes = append(validIndexes, idx)
	}
	return validIndexes, nil
}

// NewInfo creates a MapperInfo structure around an object based on a given table schema
func NewInfo(tableName string, table *ovsdb.TableSchema, obj interface{}) (*Info, error) {
	objPtrVal := reflect.ValueOf(obj)
	if objPtrVal.Type().Kind() != reflect.Ptr {
		return nil, ovsdb.NewErrWrongType("NewMapperInfo", "pointer to a struct", obj)
	}
	objVal := reflect.Indirect(objPtrVal)
	if objVal.Kind() != reflect.Struct {
		return nil, ovsdb.NewErrWrongType("NewMapperInfo", "pointer to a struct", obj)
	}
	objType := objVal.Type()

	fields := make(map[string]string, objType.NumField())
	omittedFields := make(map[string]string)
	for i := 0; i < objType.NumField(); i++ {
		field := objType.Field(i)
		colName, omit := parseStructTag(field)
		if colName == "" {
			// Untagged fields are ignored
			continue
		}
		column := table.Column(colName)
		if column == nil {
			// fields that are marked optional in struct tags are safe to skip
			if omit {
				omittedFields[colName] = field.Name
				continue
			}
			return nil, &ErrMapper{
				objType:   objType.String(),
				field:     field.Name,
				fieldType: field.Type.String(),
				fieldTag:  colName,
				reason:    "Column does not exist in schema",
			}
		}

		// Perform schema-based type checking
		expType := ovsdb.NativeType(column)
		if expType != field.Type {
			return nil, &ErrMapper{
				objType:   objType.String(),
				field:     field.Name,
				fieldType: field.Type.String(),
				fieldTag:  colName,
				reason:    fmt.Sprintf("Wrong type, column expects %s", expType),
			}
		}
		fields[colName] = field.Name
	}

	return &Info{
		Obj: obj,
		Metadata: Metadata{
			Fields:        fields,
			OmittedFields: omittedFields,
			TableSchema:   table,
			TableName:     tableName,
		},
	}, nil
}

// parseStructTag parses the ovsdb struct tag
// it returns the column name and whether it should be omitted if
// unsupported by the runtime schema
func parseStructTag(field reflect.StructField) (string, bool) {
	tagData := field.Tag.Get(ovsdbStructTag)
	parts := strings.Split(tagData, ",")
	if len(parts) == 0 {
		return "", false
	}
	omit := false
	colName := parts[0]
	if len(parts) == 2 && parts[1] == omitUnsupported {
		omit = true
	}
	return colName, omit
}
