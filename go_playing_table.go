package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func randomString(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	var str string
	// Less secure, but faster (math/rand):
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < length; i++ {
		str += strings.Split(letters, "")[rand.Intn(len(letters))]
	}

	return str
}

func PlayingWithTables() {
	table, err := NewTable("test", `{ "type": "object", "properties": { "id": { "type": "string" }, "name": { "type": "string" }, "value": { "type": "integer" }, "cost": { "type": "number" } }, "required": [ "id", "name", "value" ] }`)
	if err != nil {
		panic(err)
	}

	// Generate 1000 random string
	strs := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		for j := 0; j < rand.Intn(20)+1; j++ {
			strs[i] += randomString(rand.Intn(20)+1) + " "
		}
	}

	for i, str := range strs {
		fmt.Println(str)
		table.Insert(fmt.Sprintf(`{ "id": "id%s", "name": "%s", "value": %d, "cost": %f }`, i, str, rand.Int63n(100000), rand.Float32()))
	}

	data, err := table.SelectAll()

	if err != nil {
		panic(err)
	}

	fmt.Println(data)
	fmt.Println(len(data))
}
