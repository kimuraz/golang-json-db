package client

import (
	"bufio"
	"fmt"
	"github.com/rs/zerolog/log"
	"net"
	"os"
	"strings"
)

type Client struct {
	Conn net.Conn
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Connect(port string) {
	conn, err := net.Dial("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	c.Conn = conn
	defer c.Conn.Close()

	log.Info().Msgf("Connection established on server %s\n", c.Conn.RemoteAddr())
	for {
		fmt.Print("> ")
		reader := bufio.NewReader(os.Stdin)
		query, err := reader.ReadString('\n')

		if err != nil {
			log.Error().Msgf("Error reading from stdin: %s", err.Error())
			continue
		}

		query = strings.Replace(query, "\n", "", -1)

		if conn != nil {
			conn.Write([]byte(query))
		} else {
			panic("Connection dropped")
			os.Exit(1)
		}

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			log.Error().Msgf("Error reading from server: %s", err.Error())
			os.Exit(1)
		}
		log.Info().Msgf("SERVER: %s\n", string(buffer[:n]))
	}
}
