package table

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/xeipuuv/gojsonschema"
	"io/ioutil"
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

func (t *Table) LoadIndexes(m *sync.Mutex) {
	m.Lock()
	// Load indexes
	os.ReadDir(fmt.Sprintf("./data/%s/indexes", t.name))
	// Load id index from file
	m.Unlock()
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
	f, err := os.OpenFile(fmt.Sprintf("./data/%s/data.bin", t.name), os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()
	filePointerPosition, err := f.Seek(0, 2)
	if err != nil {
		return fmt.Errorf("Error opening data file: %s", err)
	}
	strBytes := []byte(data)
	err = binary.Write(f, binary.LittleEndian, uint64(len(strBytes)))
	if err != nil {
		return fmt.Errorf("Error writing data to file: %s", err)
	}
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

	t.IndexData(jsonData, filePointerPosition, len(strBytes))

	return nil
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
	for {
		var dataLen uint64
		err = binary.Read(f, binary.LittleEndian, &dataLen)
		if err != nil {
			break
		}
		dataBytes := make([]byte, dataLen)
		err = binary.Read(f, binary.LittleEndian, &dataBytes)
		if err != nil {
			return nil, fmt.Errorf("Error reading data from file: %s", err)
		}
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

	id := jsonData["id"].(string)

	for key, value := range jsonData {
		if key == "id" {
			t.ids[value.(string)] = [2]uint64{uint64(filePointerPosition), uint64(dataLen)}
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
	_, err := ioutil.ReadFile(fmt.Sprintf("./data/%s/indexes/id_idx.bin", t.name, path))
	if err != nil {
		return index, fmt.Errorf("Error reading id index file: %s", err)
	}

	return index, nil
}
