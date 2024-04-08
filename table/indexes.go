package table

import (
	"fmt"
	"github.com/kimuraz/golang-json-db/utils"
	"os"
	"sync"
)

type Hashable interface {
	int64 | float64 | bool
}

type HashIndex[T Hashable] struct {
	sync.Mutex
	hashIndex map[T][]string
}

type BTreeStringIndex struct {
	sync.Mutex
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

func (hashIdx *HashIndex[T]) SaveToFile(fileName string) {
	hashIdx.Lock()
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	keyBytes := make([]byte, len(hashIdx.hashIndex))
	for k, v := range hashIdx.hashIndex {
		keyBytes = append(keyBytes, []byte(fmt.Sprintf("%v", k))...)
		for _, id := range v {
			keyBytes = append(keyBytes, []byte(fmt.Sprintf("%v", id))...)
		}
	}
	if _, err = f.Write(keyBytes); err != nil {
		fmt.Println(err)
	}
	hashIdx.Unlock()
}

func (hashIdx *HashIndex[T]) LoadFromFile(fileName string) {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	defer f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	keyBytes := make([]byte, 8)
	if _, err = f.Read(keyBytes); err != nil {
		fmt.Println(err)
	}
	fmt.Println(keyBytes)
}
