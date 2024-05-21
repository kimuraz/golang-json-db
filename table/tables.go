package table

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"os"
	"strings"
)

type Table struct {
	name          string
	path          string
	schema        string
	ids           map[string][2]uint64
	boolIndexes   map[string]*HashIndex[bool]
	intIndexes    map[string]*HashIndex[int64]
	floatIndexes  map[string]*HashIndex[float64]
	stringIndexes map[string]*BTreeStringIndex
}

type JSONProperty struct {
	Type string `json:"type"`
	Ref  string `json:"$ref"`
}

type JSONSchemaForValidation struct {
	Properties map[string]JSONProperty `json:"properties"`
}

type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
	And      *WhereClause
	Or       *WhereClause
}

// For now I'll use a fixed size (Bytes) for the json objects data
// but soon I'll implement a dynamic size
var MAX_SIZE = 128

func NewTable(name string, schema string) (*Table, error) {
	// Check if name is valid new directory name
	_, err := os.Stat(fmt.Sprintf("./data/%s", name))
	if err == nil {
		return nil, fmt.Errorf("Table with name %s already exists", name)
	}

	// Create table directory
	err = os.Mkdir(fmt.Sprintf("./data/%s", name), 0755)
	if err != nil {
		return nil, fmt.Errorf("Error creating table directory: %s", err)
	}

	// Create indexes directory
	err = os.Mkdir(fmt.Sprintf("./data/%s/indexes", name), 0755)
	if err != nil {
		return nil, fmt.Errorf("Error creating indexes directory: %s", err)
	}

	// Create data file
	_, err = os.Create(fmt.Sprintf("./data/%s/data.bin", name))
	if err != nil {
		return nil, fmt.Errorf("Error creating data file: %s", err)
	}

	// Check if schema string is valid json
	var jsonSchema JSONSchemaForValidation
	var indexFiles []string = []string{"id_idx.bin"}
	err = json.Unmarshal([]byte(schema), &jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling schema: %s", err)
	}
	for propName, prop := range jsonSchema.Properties {
		if prop.Type == "array" || prop.Type == "object" || prop.Ref != "" {
			return nil, fmt.Errorf("Invalid schema, it cannot contain arrays or object references: %s", schema)
		}
		if propName != "id" {
			idxPrefix := ""
			if prop.Type == "boolean" {
				idxPrefix = "b"
			}
			if prop.Type == "integer" {
				idxPrefix = "i"
			}
			if prop.Type == "number" {
				idxPrefix = "f"
			}
			if prop.Type == "string" && propName != "id" {
				idxPrefix = "s"
			}
			indexFiles = append(indexFiles, fmt.Sprintf("%s_%s_idx.bin", idxPrefix, propName))
		}
	}

	// Write json schema to file
	err = os.WriteFile(fmt.Sprintf("./data/%s/schema.json", name), []byte(schema), 0644)
	if err != nil {
		return nil, fmt.Errorf("Error writing schema to file: %s", err)
	}

	// Create index files
	for _, file := range indexFiles {
		_, err = os.Create(fmt.Sprintf("./data/%s/indexes/%s", name, file))
		if err != nil {
			return nil, fmt.Errorf("Error creating index file: %s", err)
		}
	}

	table := &Table{
		name:          name,
		path:          fmt.Sprintf("./data/%s", name),
		schema:        schema,
		ids:           make(map[string][2]uint64),
		boolIndexes:   make(map[string]*HashIndex[bool]),
		intIndexes:    make(map[string]*HashIndex[int64]),
		floatIndexes:  make(map[string]*HashIndex[float64]),
		stringIndexes: make(map[string]*BTreeStringIndex),
	}

	return table, nil
}

func GetTable(name string) (*Table, error) {
	_, err := os.Stat(fmt.Sprintf("./data/%s", name))
	if err != nil {
		return nil, fmt.Errorf("Table with name %s does not exist", name)
	}

	schema, err := os.ReadFile(fmt.Sprintf("./data/%s/schema.json", name))
	if err != nil {
		return nil, fmt.Errorf("Error reading schema file: %s", err)
	}

	table := &Table{
		name:          name,
		path:          fmt.Sprintf("./data/%s", name),
		schema:        string(schema),
		ids:           make(map[string][2]uint64),
		boolIndexes:   make(map[string]*HashIndex[bool]),
		intIndexes:    make(map[string]*HashIndex[int64]),
		floatIndexes:  make(map[string]*HashIndex[float64]),
		stringIndexes: make(map[string]*BTreeStringIndex),
	}

	table.LoadIndexes()

	return table, nil
}

func (t *Table) GetIdType() (string, error) {
	var jsonSchema JSONSchemaForValidation
	err := json.Unmarshal([]byte(t.schema), &jsonSchema)
	if err != nil {
		return "", fmt.Errorf("Error unmarshalling schema: %s", err)
	}
	return jsonSchema.Properties["id"].Type, nil
}

func (t *Table) GetColumnNames() ([]string, error) {
	var jsonSchema JSONSchemaForValidation
	err := json.Unmarshal([]byte(t.schema), &jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling schema: %s", err)
	}
	var columns []string
	for key := range jsonSchema.Properties {
		columns = append(columns, key)
	}
	return columns, nil
}

func (t *Table) updateIds() error {
	f, err := os.OpenFile(fmt.Sprintf("./data/%s/indexes/id_idx.bin", t.name), os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("Error opening id index file: %s", err)
	}
	enc := gob.NewEncoder(f)
	err = enc.Encode(t.ids)
	if err != nil {
		return fmt.Errorf("Error encoding id index: %s", err)
	}

	return nil
}

func (t *Table) loadIds() error {
	file, err := os.OpenFile(fmt.Sprintf("./data/%s/indexes/id_idx.bin", t.name), os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("Error reading id index file: %s", err)
	}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&t.ids)
	if err != nil {
		return fmt.Errorf("Error decoding id index: %s", err)
	}
	return nil
}

func (t *Table) ValidateInsertToSchema(data string) (bool, error) {
	schemaLoader := gojsonschema.NewStringLoader(t.schema)
	dataLoader := gojsonschema.NewStringLoader(data)

	result, err := gojsonschema.Validate(schemaLoader, dataLoader)
	if err != nil {
		return false, fmt.Errorf("Error validating data: %s", err)
	}
	return result.Valid(), nil
}

// Table dir should be something like:
// tableName/data.bin
// tableName/indexes/id_idx.bin
// tableName/indexes/b_[attr]_idx.bin
// tableName/indexes/i_[attr]_idx.bin
// tableName/indexes/f_[attr]_idx.bin
// tableName/indexes/s_[attr]_idx.bin

func (t *Table) LoadIndexes() error {
	files, err := os.ReadDir(fmt.Sprintf("./data/%s/indexes", t.name))
	if err != nil {
		return fmt.Errorf("Error reading indexes directory: %s", err)
	}
	for _, file := range files {
		idxName := strings.TrimSuffix(file.Name(), "_idx.bin")
		if strings.Contains(file.Name(), "id_idx.bin") {
			err = t.loadIds()
			if err != nil {
				return fmt.Errorf("Error loading id index: %s", err)
			}
		}
		if strings.Contains(file.Name(), "b_") {
			idx := NewHashIndex[bool]()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading bool index: %s", err)
			}
			idxName = strings.TrimPrefix(idxName, "b_")
			t.boolIndexes[idxName] = idx
		}
		if strings.Contains(file.Name(), "i_") {
			idx := NewHashIndex[int64]()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading int index: %s", err)
			}
			idxName = strings.TrimPrefix(idxName, "i_")
			t.intIndexes[idxName] = idx
		}
		if strings.Contains(file.Name(), "f_") {
			idx := NewHashIndex[float64]()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading float index: %s", err)
			}
			idxName = strings.TrimPrefix(idxName, "f_")
			t.floatIndexes[idxName] = idx
		}
		if strings.Contains(file.Name(), "s_") {
			idx := NewBTreeStringIndex()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading string index: %s", err)
			}
			idxName = strings.TrimPrefix(idxName, "s_")
			t.stringIndexes[idxName] = idx
		}
	}
	return nil
}

func (t *Table) Insert(data string) error {
	// Validate data
	valid, err := t.ValidateInsertToSchema(data)
	if err != nil {
		return fmt.Errorf("Error validating data: %s", err)
	}
	if !valid {
		return fmt.Errorf("Data is not valid according to schema")
	}

	// Verify if id is unique
	var jsonData map[string]interface{}
	err = json.Unmarshal([]byte(data), &jsonData)

	if err != nil {
		return fmt.Errorf("Error unmarshalling data: %s", err)
	}
	if _, ok := jsonData["id"]; !ok {
		idType, err := t.GetIdType()
		if err != nil {
			return err
		}
		if idType == "string" {
			newId, err := uuid.NewUUID()
			if err != nil {
				return fmt.Errorf("Error generating uuid: %s", err)
			}
			jsonData["id"] = newId
		} else if idType == "integer" {
			newId := len(t.ids) + 1
			jsonData["id"] = newId
		} else {
			return fmt.Errorf("Id not found in data")
		}
	} else {
		if _, ok := t.ids[fmt.Sprintf("%v", jsonData["id"])]; ok {
			return fmt.Errorf("Id already exists")
		}
	}

	finalData, err := json.Marshal(jsonData)
	if err != nil {
		return fmt.Errorf("Error marshalling data: %s", err)
	}

	finalStrData := string(finalData)

	f, err := os.OpenFile(fmt.Sprintf("./data/%s/data.bin", t.name), os.O_WRONLY|os.O_APPEND, 0644)
	defer f.Close()
	filePointerPosition, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Error opening data file: %s", err)
	}
	if len(finalStrData) > MAX_SIZE {
		return fmt.Errorf("string exceeds fixed size: %s", finalStrData)
	}
	strBytes := []byte(padString(finalStrData, MAX_SIZE))
	err = binary.Write(f, binary.LittleEndian, strBytes)
	if err != nil {
		return fmt.Errorf("Error writing data to file: %s", err)
	}

	return t.IndexData(jsonData, filePointerPosition, len(strBytes))
}

func padString(str string, size int) string {
	if len(str) >= size {
		return str
	}
	padding := make([]byte, size-len(str))
	return str + string(padding)
}

func (t *Table) SelectAll() ([]map[string]interface{}, error) {
	// Open data file
	f, err := os.Open(fmt.Sprintf("./data/%s/data.bin", t.name))
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("Error opening data file: %s", err)
	}

	// Read data file
	var data []map[string]interface{}
	buf := make([]byte, MAX_SIZE)
	for {
		err := binary.Read(f, binary.LittleEndian, &buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error().Err(err).Msg(fmt.Sprintf("Error reading data file: %s", f.Name()))
			return nil, fmt.Errorf("Error reading data file: %s", err)
		}
		dataBytes := bytes.Trim(buf, "\x00")
		var jsonData map[string]interface{}
		err = json.Unmarshal(dataBytes, &jsonData)
		if err != nil {
			return nil, fmt.Errorf("Error unmarshalling data: %s", err)
		}
		data = append(data, jsonData)
	}

	return data, nil
}

func (t *Table) FilterIndexByValue(columnName string, value interface{}) ([]string, error) {
	var jsonSchema JSONSchemaForValidation
	err := json.Unmarshal([]byte(t.schema), &jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling schema: %s", err)
	}

	if columnName == "id" {
		id := fmt.Sprintf("%v", value)
		if _, ok := t.ids[id]; !ok {
			return []string{}, nil
		}
		return []string{id}, nil
	}

	if jsonSchema.Properties[columnName].Type == "boolean" {
		idx := t.boolIndexes[columnName]
		ids := idx.Get(value.(bool))
		return ids, nil
	}
	if jsonSchema.Properties[columnName].Type == "integer" {
		idx := t.intIndexes[columnName]
		ids := idx.Get(value.(int64))
		return ids, nil
	}
	if jsonSchema.Properties[columnName].Type == "number" {
		idx := t.floatIndexes[columnName]
		ids := idx.Get(value.(float64))
		return ids, nil
	}
	if jsonSchema.Properties[columnName].Type == "string" {
		idx := t.stringIndexes[columnName]
		idx.BTree.PrintTree()
		idMap, found := idx.BTree.Search(value.(string))
		if !found {
			return nil, fmt.Errorf("Value not found in index")
		}
		var ids []string
		for id, hasStr := range idMap {
			if hasStr {
				ids = append(ids, id)
			}
		}
		return ids, nil
	}

	return nil, fmt.Errorf("Column type not supported %s, %s", columnName, jsonSchema.Properties[columnName].Type)
}

func (t *Table) SelectWhereIds(clauseChain WhereClause) ([]string, error) {
	compositeIds, err := t.FilterIndexByValue(clauseChain.Column, clauseChain.Value)
	if err != nil {
		return nil, fmt.Errorf("Error selecting data: %s", err)
	}
	if clauseChain.And != nil {
		ids, err := t.SelectWhereIds(*clauseChain.And)
		if err != nil {
			return nil, fmt.Errorf("Error selecting data: %s", err)
		}
		compositeIds = intersect(compositeIds, ids)
	}
	if clauseChain.Or != nil {
		ids, err := t.SelectWhereIds(*clauseChain.Or)
		if err != nil {
			return nil, fmt.Errorf("Error selecting data: %s", err)
		}
		compositeIds = append(compositeIds, ids...)
	}

	return compositeIds, nil
}

func (t *Table) SelectWhere(clauseChain WhereClause) ([]map[string]interface{}, error) {
	ids, err := t.SelectWhereIds(clauseChain)
	if err != nil {
		return nil, fmt.Errorf("Error selecting data: %s", err)
	}
	data, err := t.selectByIds(ids)
	if err != nil {
		return nil, fmt.Errorf("Error selecting data: %s", err)
	}
	return data, nil
}

func intersect(ids []string, ids2 []string) []string {
	var intersectedIds []string
	for _, id := range ids {
		for _, id2 := range ids2 {
			if id == id2 {
				intersectedIds = append(intersectedIds, id)
			}
		}
	}
	return intersectedIds
}

func (t *Table) selectByIds(ids []string) ([]map[string]interface{}, error) {
	var data []map[string]interface{}
	for _, id := range ids {
		jsonData, err := t.GetById(id)
		if err != nil {
			return nil, fmt.Errorf("Error getting data by id: %s", err)
		}
		data = append(data, jsonData)
	}
	return data, nil
}

func (t *Table) GetById(id interface{}) (map[string]interface{}, error) {
	err := t.loadIds()
	if err != nil {
		return nil, fmt.Errorf("Error loading id index: %s", err)
	}
	if _, ok := t.ids[fmt.Sprintf("%v", id)]; !ok {
		return nil, fmt.Errorf("Id not found")
	}
	filePointerPosition := t.ids[fmt.Sprintf("%v", id)][0]
	dataLen := MAX_SIZE
	f, err := os.Open(fmt.Sprintf("./data/%s/data.bin", t.name))
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("Error opening data file: %s", err)
	}
	_, err = f.Seek(int64(filePointerPosition), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Error seeking data file: %s", err)
	}
	buf := make([]byte, dataLen)
	err = binary.Read(f, binary.LittleEndian, &buf)
	if err != nil {
		return nil, fmt.Errorf("Error reading data file: %s", err)
	}
	dataBytes := bytes.Trim(buf, "\x00")
	var jsonData map[string]interface{}
	err = json.Unmarshal(dataBytes, &jsonData)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling data: %s", err)
	}
	return jsonData, nil
}

func (t *Table) IndexData(jsonData map[string]interface{}, filePointerPosition int64, dataLen int) error {
	var jsonSchema JSONSchemaForValidation
	err := json.Unmarshal([]byte(t.schema), &jsonSchema)
	if err != nil {
		return fmt.Errorf("Error unmarshalling schema: %s", err)
	}

	// Hacky, but should work
	id := fmt.Sprintf("%v", jsonData["id"])

	for key, value := range jsonData {
		if key == "id" {
			t.ids[id] = [2]uint64{uint64(filePointerPosition), uint64(dataLen)}
			t.updateIds()
		} else {
			if jsonSchema.Properties[key].Type == "boolean" {
				if _, ok := t.boolIndexes[key]; !ok {
					t.boolIndexes[key] = NewHashIndex[bool]()
				}
				idx := t.boolIndexes[key]
				idx.Insert(value.(bool), id)
				idx.SaveToFile(fmt.Sprintf("./data/%s/indexes/b_%s_idx.bin", t.name, key))
				continue
			}
			if jsonSchema.Properties[key].Type == "integer" {
				if _, ok := t.intIndexes[key]; !ok {
					t.intIndexes[key] = NewHashIndex[int64]()
				}
				idx := t.intIndexes[key]
				idx.Insert(int64(value.(float64)), id)
				idx.SaveToFile(fmt.Sprintf("./data/%s/indexes/i_%s_idx.bin", t.name, key))
				continue
			}
			if jsonSchema.Properties[key].Type == "number" {
				if _, ok := t.floatIndexes[key]; !ok {
					t.floatIndexes[key] = NewHashIndex[float64]()
				}
				idx := t.floatIndexes[key]
				idx.Insert(value.(float64), id)
				idx.SaveToFile(fmt.Sprintf("./data/%s/indexes/f_%s_idx.bin", t.name, key))
				continue
			}
			if jsonSchema.Properties[key].Type == "string" {
				if _, ok := t.stringIndexes[key]; !ok {
					t.stringIndexes[key] = NewBTreeStringIndex()
				}
				idx := t.stringIndexes[key]
				for _, str := range strings.Fields(value.(string)) {
					idx.BTree.Insert(str, id)
				}
				idx.BTree.PrintTree()
				idx.SaveToFile(fmt.Sprintf("./data/%s/indexes/s_%s_idx.bin", t.name, key))
			}
		}
	}

	return nil
}

func (t *Table) loadIdIndexFromFile(path string) (map[string]uint64, error) {
	index := make(map[string]uint64)
	_, err := os.ReadFile(fmt.Sprintf("./data/%s/indexes/id_idx.bin", t.name, path))
	if err != nil {
		return index, fmt.Errorf("Error reading id index file: %s", err)
	}

	return index, nil
}

func PrintWhereClause(clause *WhereClause, level int) {
	if clause == nil {
		return
	}
	fmt.Printf("%s %s %s %v\n", indent(level), clause.Column, clause.Operator, clause.Value)
	if clause.And != nil {
		fmt.Printf("%sAND\n", indent(level))
		PrintWhereClause(clause.And, level+1)
	}
	if clause.Or != nil {
		fmt.Printf("%sOR\n", indent(level))
		PrintWhereClause(clause.Or, level+1)
	}
}

func indent(level int) string {
	return fmt.Sprintf("%s", "  ")
}
