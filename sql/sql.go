package sql

import (
	"encoding/json"
	"fmt"
	"github.com/kimuraz/golang-json-db/table"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

// TODO: Check this https://marianogappa.github.io/software/2019/06/05/lets-build-a-sql-parser-in-go/
// TODO: Also this https://github.com/xwb1989/sqlparser?tab=readme-ov-file

func ColumnsToSchema(columns []*sqlparser.ColumnDefinition) (string, error) {
	schema := make(map[string]interface{})
	schema["type"] = "object"
	schema["properties"] = make(map[string]interface{})

	for _, column := range columns {
		switch column.Type.SQLType() {
		case sqltypes.Int8:
			fallthrough
		case sqltypes.Int16:
			fallthrough
		case sqltypes.Int24:
			fallthrough
		case sqltypes.Int32:
			fallthrough
		case sqltypes.Int64:
			schema["properties"].(map[string]interface{})[column.Name.String()] = map[string]interface{}{"type": "integer"}
		case sqltypes.Text:
			fallthrough
		case sqltypes.VarChar:
			fallthrough
		case sqltypes.Char:
			schema["properties"].(map[string]interface{})[column.Name.String()] = map[string]interface{}{"type": "string"}
		case sqltypes.Decimal:
			fallthrough
		case sqltypes.Float32:
			fallthrough
		case sqltypes.Float64:
			schema["properties"].(map[string]interface{})[column.Name.String()] = map[string]interface{}{"type": "number"}
		default:
			return "", fmt.Errorf("Unsupported type: %s", column.Type.SQLType())
		}
	}
	json, err := json.Marshal(schema)

	return string(json), err
}

func SQLToAction(sql string) (map[string]interface{}, error) {
	response := make(map[string]interface{})
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return nil, err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		_ = stmt
		if stmt.Action == sqlparser.CreateStr {
			if stmt.TableSpec == nil {
				return nil, fmt.Errorf("Cannot parse table specification")
			}
			schema, err := ColumnsToSchema(stmt.TableSpec.Columns)
			if err != nil {
				return nil, err
			}
			response["schema"] = schema
			response["table"] = stmt.NewName.Name.CompliantName()
			_, err = table.NewTable(stmt.NewName.Name.CompliantName(), schema)

			if err != nil {
				response["ok"] = false
				return response, err
			}

		} else {
			return nil, fmt.Errorf("Unsupported action: %s", stmt.Action)
		}
	case *sqlparser.Insert:
		_ = stmt

	}
	response["ok"] = true
	return response, nil
}
