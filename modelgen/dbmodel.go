package modelgen

import (
	"sort"
	"text/template"

	"github.com/ovn-org/libovsdb/ovsdb"
)

// BASE_DB_TEMPLATE is the base DBModel template
// It includes the following other templates that can be overriden to customize the generated file
// "header"
// "preDBDefinitions"
// "postDBDefinitions"
// It is design to be used with a map[string] interface and some defined keys (see GetDBTemplateData)
const BASE_DB_TEMPLATE = `{{ template "header" . }}

package {{ index . "PackageName" }}

{{ template "preDBDefinitions" }}

// FullDatabaseModel returns the DatabaseModel object to be used in libovsdb
func FullDatabaseModel() (*model.DBModel, error) {
	return model.NewDBModel("{{ index . "DatabaseName" }}", map[string]model.Model{
    {{ range index . "Tables" }} "{{ .TableName }}" : &{{ .StructName }}{}, 
    {{ end }}
	})
}

{{ template "postDBDefinitions" . }}

`

// DEFAULT_PRE_DB_TEMPLATE is the default template for "preDBDefinitions"
const DEFAULT_PRE_DB_TEMPLATE = `{{ define "preDBDefinitions" }} import (
	"github.com/ovn-org/libovsdb/model"
) {{ end }}
`

// DEFAULT_POST_DB_TEMPLATE is the default template for "postDBDefinitions"
const DEFAULT_POST_DB_TEMPLATE = `{{ define "postDBDefinitions" }}{{ end }}`

//TableInfo represents the information of a table needed by the Model template
type TableInfo struct {
	TableName  string
	StructName string
}

// GetDBTemplateData returns the map needed to execute the DBTemplate. It has the following keys:
// DatabaseName: (string) the database name
// PackageName : (string) the package name
// Tables: []Table list of Tables that form the Model
func GetDBTemplateData(pkg string, schema *ovsdb.DatabaseSchema) map[string]interface{} {
	data := map[string]interface{}{}
	data["DatabaseName"] = schema.Name
	data["PackageName"] = pkg
	tables := []TableInfo{}

	var order sort.StringSlice
	for tableName := range schema.Tables {
		order = append(order, tableName)
	}
	order.Sort()

	for _, tableName := range order {
		tables = append(tables, TableInfo{
			TableName:  tableName,
			StructName: StructName(tableName),
		})
	}
	data["Tables"] = tables
	return data
}

// NewDBTemplate returns a new DBTemplate and the DBTemplate data map
// See BASE_DB_TEMPLATE to a detailed explanation of the possible ways this template can be expanded
func NewDBTemplate(pkg string, db *ovsdb.DatabaseSchema) (*template.Template, map[string]interface{}) {
	main, err := template.New("DB").Parse(BASE_DB_TEMPLATE)
	if err != nil {
		panic(err)
	}
	tmpl, err := main.Parse(DEFAULT_HEADER_TEMPLATE)
	if err != nil {
		panic(err)
	}
	tmpl, err = tmpl.Parse(DEFAULT_PRE_DB_TEMPLATE)
	if err != nil {
		panic(err)
	}
	_, err = tmpl.Parse(DEFAULT_POST_DB_TEMPLATE)
	if err != nil {
		panic(err)
	}

	return main, GetDBTemplateData(pkg, db)
}
