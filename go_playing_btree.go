package main

import (
	"encoding/gob"
	"fmt"
	"github.com/kimuraz/golang-json-db/utils"
	"os"
	"strings"
)

func main() {
	btree := utils.BTree{}
	firstId := "123"
	secondId := "456"
	bla1 := "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Aliquet enim tortor at auctor urna nunc id cursus. Morbi quis commodo odio aenean sed adipiscing. Ornare aenean euismod elementum nisi. Aliquam faucibus purus in massa tempor nec feugiat nisl pretium. Ut lectus arcu bibendum at. Euismod in pellentesque massa placerat duis ultricies. Aliquam purus sit amet luctus venenatis lectus. Nunc sed blandit libero volutpat. Scelerisque eu ultrices vitae auctor eu augue ut. Mauris rhoncus aenean vel elit scelerisque mauris pellentesque. Sed vulputate odio ut enim blandit volutpat maecenas. Ipsum dolor sit amet consectetur. Velit ut tortor pretium viverra. Elit ullamcorper dignissim cras tincidunt lobortis feugiat vivamus."
	bla2 := "sed do bla bla bla"

	// Split string every space
	words := strings.Fields(bla1)

	for _, word := range words {
		btree.Insert(word, firstId)
	}

	words = strings.Fields(bla2)
	for _, word := range words {
		btree.Insert(word, secondId)
	}

	// Search for a word
	result, _ := btree.Search("sed")

	for id, _ := range result {
		fmt.Println(id)
	}

	firstStr := btree.String()

	f, err := os.Create("b1.bin")
	if err != nil {
		fmt.Println(err)
	}

	btree2 := utils.BTree{}
	enconder := gob.NewEncoder(f)
	err = enconder.Encode(btree)
	if err != nil {
		fmt.Println(err)
	}
	f.Close()

	f, err = os.Open("b1.bin")
	if err != nil {
		fmt.Println(err)
	}
	err = gob.NewDecoder(f).Decode(&btree2)
	if err != nil {
		fmt.Println(err)
	}

	secondStr := btree2.String()

	fmt.Println(firstStr == secondStr)
}
