package main

import (
	"fmt"
	"math/rand"
	"time"
)

func randomString(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Choose between math/rand and crypto/rand:
	bytes := make([]byte, length)

	// Less secure, but faster (math/rand):
	rand.Seed(time.Now().UnixNano())
	for i := range bytes {
		bytes[i] = letters[rand.Intn(len(letters))]
	}

	// More secure (crypto/rand):
	if _, err := rand.Read(bytes); err != nil {
		panic(err) // Handle the error appropriately
	}

	return string(bytes)
}

func main() {
	table, err := NewTable("test", `{ "$schema": "http://json-schema.org/draft-04/schema#", "type": "object", "properties": { "id": { "type": "string" }, "name": { "type": "string" }, "value": { "type": "integer" }, "cost": { "type": "number" } }, "required": [ "id", "name", "value" ] }`)
	if err != nil {
		panic(err)
	}

	// Generate 1000 random string
	strs := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		for j := 0; j < rand.Intn(20)+1; j++ {
			strs[j] = randomString(rand.Intn(20))
		}
	}

	for _, str := range strs {
		table.Insert(fmt.Sprintf(`{ "id": "id%s", "name": "%s", "value": %d, "cost": %f }`, randomString(30), str, rand.Int63n(100000), rand.Float32()))
	}
}
