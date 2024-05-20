package table

import (
	"encoding/gob"
	"fmt"
	"github.com/kimuraz/golang-json-db/utils"
	"os"
	"sync"
)

type GobIndex struct {
	sync.Mutex
}
type Hashable interface {
	int64 | float64 | bool
}

type HashIndex[T Hashable] struct {
	GobIndex
	hashIndex map[T][]string
}

type BTreeStringIndex struct {
	GobIndex
	bTree *utils.BTree
}

func NewHashIndex[T Hashable]() *HashIndex[T] {
	return &HashIndex[T]{
		hashIndex: make(map[T][]string),
	}
}

func NewBTreeStringIndex() *BTreeStringIndex {
	return &BTreeStringIndex{
		bTree: utils.NewBTree(),
	}
}

func (hashIdx *HashIndex[T]) Insert(key T, id string) {
	hashIdx.Lock()
	defer hashIdx.Unlock()
	if hashIdx.hashIndex == nil {
		hashIdx.hashIndex = make(map[T][]string)
	}
	hashIdx.hashIndex[key] = append(hashIdx.hashIndex[key], id)
}

func (hashIdx *HashIndex[T]) Get(key T) []string {
	return hashIdx.hashIndex[key]
}

func (hashIdx *HashIndex[T]) Remove(key T, id string) {
	hashIdx.Lock()
	defer hashIdx.Unlock()
	if hashIdx.hashIndex == nil {
		return
	}
	for i, v := range hashIdx.hashIndex[key] {
		if v == id {
			hashIdx.hashIndex[key] = append(hashIdx.hashIndex[key][:i], hashIdx.hashIndex[key][i+1:]...)
			return
		}
	}
}

func (hashIdx *HashIndex[T]) Print() {
	for k, v := range hashIdx.hashIndex {
		fmt.Println(fmt.Sprintf("key: %v, value: %v", k, v))
	}
}

func (gobIndex *GobIndex) SaveToFile(fileName string) error {
	gobIndex.Lock()
	defer gobIndex.Unlock()
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	return encoder.Encode(gobIndex)
}

func (gobIndex *GobIndex) LoadFromFile(fileName string) error {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	decoder := gob.NewDecoder(file)
	return decoder.Decode(gobIndex)
}
