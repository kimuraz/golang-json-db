package main

import (
	"fmt"
	"strings"
)

// TODO: Check this https://marianogappa.github.io/software/2019/06/05/lets-build-a-sql-parser-in-go/

type SQLCommand struct {
	SQL          string   `json:"sql"`
	Verb         string   `json:"verb"`
	TableName    string   `json:"table_name"`
	Columns      []string `json:"columns"`
	ColumnsTypes []string `json:"columns_types"`
	Where        string   `json:"where"`
	WhereArgs    []string `json:"where_args"`
}

var validVerbs = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"}

var validColumnTypes = []string{"INT", "VARCHAR", "TEXT", "FLOAT", "BOOLEAN", "UUID"}

func NewSqlCommand(sql string) (*SQLCommand, error) {
	command := SQLCommand{SQL: sql}
	return &command, command.ParseFromString()
}

func (c *SQLCommand) String() string {
	return c.SQL
}

func (c *SQLCommand) ParseCreateTable() error {
	words := strings.Fields(c.SQL)
	if len(words) < 4 {
		return fmt.Errorf("Invalid SQL command")
	}

	c.Verb = "CREATE"
	c.TableName = words[2]

	// Find the columns
	var columns []string
	var columnsTypes []string

	columnsDefinition := strings.Join(words[3:], " ")
	columnsDefinition = strings.TrimLeft(columnsDefinition, "(")
	columnsDefinition = strings.TrimRight(columnsDefinition, ")")

	for _, pair := range strings.Split(columnsDefinition, ",") {
		pairNameType := strings.Split(pair, " ")
		if len(pairNameType) != 2 {
			return fmt.Errorf("Invalid column definition on table creation %s", pair)
		}
		if pairNameType[0] == "" || pairNameType[1] == "" {
			return fmt.Errorf("Column name or type is empty on table creation %s %s", pairNameType[0], pairNameType[1])
		}
		for i, validColumnType := range validColumnTypes {
			if strings.ToUpper(pairNameType[1]) == validColumnType {
				break
			}
			if i == len(validColumnTypes)-1 {
				return fmt.Errorf("Invalid column type %s for column %s on table creation", pairNameType[1], pairNameType[0])
			}
		}

		column := pairNameType[0]
		columnType := pairNameType[1]

		columns = append(columns, column)
		columnsTypes = append(columnsTypes, columnType)
	}
	c.Columns = columns
	c.ColumnsTypes = columnsTypes

	return nil
}

func (c *SQLCommand) ParseFromString() error {
	words := strings.Fields(c.SQL)

	if len(words) == 0 {
		return fmt.Errorf("Empty SQL command")
	}

	var verb string
	for _, validVerb := range validVerbs {
		if strings.ToUpper(words[0]) == validVerb {
			verb = validVerb
			break
		}
	}

	if verb == "" {
		return fmt.Errorf("Unknown SQL command %s", words[0])
	}

	if verb == "CREATE" {
		if len(words) < 3 {
			return fmt.Errorf("Invalid syntax for CREATE command")
		}
		if strings.ToUpper(words[1]) == "TABLE" {
			return c.ParseCreateTable()
		} else {
			return fmt.Errorf("Invalid SQL command")
		}
	}

	return nil
}
