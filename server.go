package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

type Client struct {
	Conn     net.Conn
	Received []string
	Sent     []string
}

func (c *Client) ReadLoop() {
	defer c.Conn.Close()
	for {
		buffer := make([]byte, 1024)
		n, err := c.Conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err)
			break
		}
		message := strings.Trim(string(buffer[:n]), " ")
		c.Received = append(c.Received, message)

		sqlCommand, err := NewSqlCommand(message)
		if err != nil {
			c.Conn.Write([]byte(fmt.Sprintf("Error parsing command: %s\n", err.Error())))
			continue
		}

		fmt.Println("Valid command:", sqlCommand.String())
		c.Conn.Write([]byte("Accepted: " + sqlCommand.String() + "\n"))
	}
}

func ConnectClient(conn net.Conn) *Client {
	client := &Client{
		Conn: conn,
	}

	fmt.Println("New connection from:", conn.RemoteAddr())

	go client.ReadLoop()
	return client
}

func ListenCli(clients *[]*Client) {
	for {
		var input string
		fmt.Scanln(&input)
		if input == "exit" {
			for _, client := range *clients {
				client.Conn.Close()
			}
			os.Exit(0)
		}

		if input == "list" {
			if len(*clients) == 0 {
				fmt.Println("No clients connected")
			}
			for _, client := range *clients {
				fmt.Println(client.Conn.RemoteAddr())
			}
		}
	}
}

func main() {
	fmt.Println("Starting server...")
	clients := make([]*Client, 0)

	ln, _ := net.Listen("tcp", ":9875")

	go ListenCli(&clients)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		clients = append(clients, ConnectClient(conn))
	}
}
