package server

import (
	"fmt"
	"github.com/kimuraz/golang-json-db/sql"
	"net"
	"os"
	"strconv"
	"strings"
)

type Server struct {
	Clients     []*ServerClient
	Port        int
	MessageChan chan string
}

type ServerClient struct {
	Conn     net.Conn
	Received []string
	Sent     []string
}

func (c *ServerClient) ReadLoop(messages chan string) {
	defer c.Conn.Close()
	for {
		buffer := make([]byte, 1024)
		n, err := c.Conn.Read(buffer)
		if err != nil {
			if err.Error() == "EOF" {
				messages <- fmt.Sprintf("Client %s disconnected", c.Conn.RemoteAddr().String())
				c.Conn.Close()
				c.Conn = nil
				break
			}
			messages <- fmt.Sprintf("Error reading from client: %s", err.Error())
			break
		}
		message := strings.Trim(string(buffer[:n]), " ")
		c.Received = append(c.Received, message)

		sqlCommand, err := sql.NewSqlCommand(message)
		if err != nil {
			c.Conn.Write([]byte(fmt.Sprintf("Error parsing command: %s\n", err.Error())))
			continue
		}

		c.Conn.Write([]byte("Accepted: " + sqlCommand.SQL + "\n"))
	}
}

func ConnectServerClient(conn net.Conn, messages chan string) *ServerClient {
	client := &ServerClient{
		Conn: conn,
	}

	messages <- "New connection from: " + conn.RemoteAddr().String()

	go client.ReadLoop(messages)
	return client
}

func (s *Server) ListenCli() {
	for {
		var input string
		fmt.Scanln(&input)
		if input == "exit" {
			for _, client := range s.Clients {
				client.Conn.Close()
			}
			os.Exit(0)
		}

		if input == "list" {
			if len(s.Clients) == 0 {
				s.MessageChan <- "No clients connected"
			}
			for _, client := range s.Clients {
				s.MessageChan <- client.Conn.RemoteAddr().String()
			}
		}
	}
}

func (s *Server) ListenDisconnect() {
	for {
		for i, c := range s.Clients {
			if c.Conn == nil {
				s.Clients = append(s.Clients[:i], s.Clients[i+1:]...)
			}
		}
	}
}

func NewServer(port int) *Server {
	return &Server{
		Clients:     make([]*ServerClient, 0),
		Port:        port,
		MessageChan: make(chan string),
	}
}

func (s *Server) StartServer() {
	portStr := strconv.Itoa(s.Port)
	s.MessageChan <- fmt.Sprintf("Starting server on port %s...", portStr)
	ln, _ := net.Listen("tcp", ":"+portStr)
	defer ln.Close()

	go s.ListenCli()
	go s.ListenDisconnect()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.MessageChan <- "Error accepting connection: " + err.Error()
			continue
		}
		s.Clients = append(s.Clients, ConnectServerClient(conn, s.MessageChan))
	}
}
