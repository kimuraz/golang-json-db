package sql

import (
	"encoding/json"
	"fmt"
	"github.com/kimuraz/golang-json-db/table"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"strconv"
)

func SQLToAction(sql string) (map[string]interface{}, error) {
	response := make(map[string]interface{})
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return nil, err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		_ = stmt
		response["table"] = stmt.NewName.Name.CompliantName()
		switch stmt.Action {

		case sqlparser.CreateStr:
			if stmt.TableSpec == nil {
				return nil, fmt.Errorf("Cannot parse table specification")
			}
			schema, err := ColumnsToSchema(stmt.TableSpec.Columns)
			if err != nil {
				return nil, err
			}
			response["schema"] = schema
			_, err = table.NewTable(stmt.NewName.Name.CompliantName(), schema)

			if err != nil {
				response["ok"] = false
				return response, err
			}
		default:
			return nil, fmt.Errorf("Unsupported action: %s", stmt.Action)
		}

	case *sqlparser.Insert:
		_ = stmt
		response["table"] = stmt.Table.Name.CompliantName()
		table, err := table.GetTable(stmt.Table.Name.CompliantName())
		if err != nil {
			response["ok"] = false
			return response, err
		}
		defaultColumnNames, err := table.GetColumnNames()
		if err != nil {
			response["ok"] = false
			return response, err
		}
		insertJson := InsertSqlToJSON(stmt, defaultColumnNames)
		for _, row := range insertJson.([]map[string]interface{}) {
			jsonStr, err := json.Marshal(row)
			if err != nil {
				response["ok"] = false
				return response, err
			}
			err = table.Insert(string(jsonStr))
			if err != nil {
				response["ok"] = false
				return response, err
			}
		}
		response["ok"] = true
		return response, nil

	case *sqlparser.Select:
		_ = stmt
		response["table"] = stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.CompliantName()
		t, err := table.GetTable(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.CompliantName())
		if err != nil {
			response["ok"] = false
			return response, err
		}
		if stmt.Where == nil {
			result, err := t.SelectAll()
			if err != nil {
				response["ok"] = false
				return response, err
			}
			resToJson, err := json.Marshal(result)
			if err != nil {
				response["ok"] = false
				return response, err
			}
			response["result"] = string(resToJson)
		} else {
			whereClauses := parseWhereExpr(stmt.Where.Expr)
			result, err := t.SelectWhere(*whereClauses)
			if err != nil {
				response["ok"] = false
				return response, err
			}
			resToJson, err := json.Marshal(result)
			if err != nil {
				response["ok"] = false
				return response, err
			}
			response["result"] = string(resToJson)
		}
	}
	response["ok"] = true
	return response, nil
}

func parseWhereExpr(expr sqlparser.Expr) *table.WhereClause {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return &table.WhereClause{
			Column:   sqlparser.String(expr.Left),
			Operator: expr.Operator,
			Value:    extractValue(expr.Right),
		}
	case *sqlparser.AndExpr:
		left := parseWhereExpr(expr.Left)
		right := parseWhereExpr(expr.Right)
		if left != nil && right != nil {
			andClause := left
			for andClause.And != nil {
				andClause = andClause.And
			}
			andClause.And = right
		}
		return left
	case *sqlparser.OrExpr:
		left := parseWhereExpr(expr.Left)
		right := parseWhereExpr(expr.Right)
		if left != nil && right != nil {
			orClause := left
			for orClause.Or != nil {
				orClause = orClause.Or
			}
			orClause.Or = right
		}
		return left
	default:
		return nil
	}
}

func InsertSqlToJSON(stmt *sqlparser.Insert, defaultColumnNames []string) interface{} {
	values := make([]map[string]interface{}, 0)
	columnsNames := make([]string, 0)
	if len(stmt.Columns) == 0 {
		columnsNames = defaultColumnNames
	} else {
		for _, column := range stmt.Columns {
			columnsNames = append(columnsNames, column.CompliantName())
		}
	}
	for _, row := range stmt.Rows.(sqlparser.Values) {
		jsonObject := make(map[string]interface{})
		for i, val := range row {
			columnName := columnsNames[i]
			jsonObject[columnName] = extractValue(val)
		}
		values = append(values, jsonObject)
	}
	return values
}

func extractValue(val sqlparser.Expr) interface{} {
	switch v := val.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return string(v.Val)
		case sqlparser.IntVal:
			intVal, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return string(v.Val)
			}
			return intVal
		case sqlparser.FloatVal:
			floatVal, err := strconv.ParseFloat(string(v.Val), 64)
			if err != nil {
				return string(v.Val)
			}
			return floatVal
		default:
			return string(v.Val)
		}
	case *sqlparser.NullVal:
		return nil
	default:
		return sqlparser.String(val)
	}
}

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
