package main

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
)

type Config struct {
	ServerPort int `json:"server_port"`
}

func NewConfig(filePath string) *Config {
	config := Config{}

	file, err := os.Open(filePath)

	if err != nil {
		fmt.Println(err.Error())
		log.Fatal().Err(err)
		os.Exit(-1)
	}
	parser := json.NewDecoder(file)
	if err = parser.Decode(&config); err != nil {
		fmt.Println(err.Error())
		log.Fatal().Err(err)
		os.Exit(-1)
	}

	return &config
}
