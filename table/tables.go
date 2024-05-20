package table

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"os"
	"strings"
	"sync"
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

	table.LoadIndexes(&sync.Mutex{})

	return table, nil
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

func (t *Table) LoadIndexes(m *sync.Mutex) error {
	m.Lock()
	defer m.Unlock()
	files, err := os.ReadDir(fmt.Sprintf("./data/%s/indexes", t.name))
	if err != nil {
		return fmt.Errorf("Error reading indexes directory: %s", err)
	}
	for _, file := range files {
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
			t.boolIndexes[strings.TrimPrefix(file.Name(), "b_")] = idx
		}
		if strings.Contains(file.Name(), "i_") {
			idx := NewHashIndex[int64]()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading int index: %s", err)
			}
			t.intIndexes[strings.TrimPrefix(file.Name(), "i_")] = idx
		}
		if strings.Contains(file.Name(), "f_") {
			idx := NewHashIndex[float64]()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading float index: %s", err)
			}
			t.floatIndexes[strings.TrimPrefix(file.Name(), "f_")] = idx
		}
		if strings.Contains(file.Name(), "s_") {
			idx := NewBTreeStringIndex()
			err = idx.LoadFromFile(fmt.Sprintf("./data/%s/indexes/%s", t.name, file.Name()))
			if err != nil {
				return fmt.Errorf("Error loading string index: %s", err)
			}
			t.stringIndexes[strings.TrimPrefix(file.Name(), "s_")] = idx
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

	// Append data to data.bin file
	f, err := os.OpenFile(fmt.Sprintf("./data/%s/data.bin", t.name), os.O_WRONLY|os.O_APPEND, 0644)
	defer f.Close()
	filePointerPosition, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Error opening data file: %s", err)
	}
	if len(data) > MAX_SIZE {
		return fmt.Errorf("string exceeds fixed size: %s", data)
	}
	strBytes := []byte(padString(data, MAX_SIZE))
	err = binary.Write(f, binary.LittleEndian, strBytes)
	if err != nil {
		return fmt.Errorf("Error writing data to file: %s", err)
	}

	// Load data to json
	var jsonData map[string]interface{}
	err = json.Unmarshal([]byte(data), &jsonData)

	if err != nil {
		return fmt.Errorf("Error unmarshalling data: %s", err)
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

func (t *Table) IndexData(jsonData map[string]interface{}, filePointerPosition int64, dataLen int) error {
	// Load schema to json
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
			// Load index
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
					idx.bTree.Insert(str, id)
				}
			}
		}
	}

	return nil
}

func (t *Table) loadIdIndexFromFile(path string) (map[string]uint64, error) {
	// Find file
	index := make(map[string]uint64)
	_, err := os.ReadFile(fmt.Sprintf("./data/%s/indexes/id_idx.bin", t.name, path))
	if err != nil {
		return index, fmt.Errorf("Error reading id index file: %s", err)
	}

	return index, nil
}
