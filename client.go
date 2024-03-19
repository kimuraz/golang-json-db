package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func messageReceiver(conn net.Conn) {
	for {
		// read from server
		buffer := make([]byte, 1024)
		n, err := (conn).Read(buffer)
		if err != nil {
			fmt.Println("Connection dropped")
			os.Exit(1)
		}
		fmt.Println(string(buffer[:n]))
	}
}

func main() {
	conn, err := net.Dial("tcp", ":9875")
	if err != nil {
		panic(err)
	}

	fmt.Println("Connection established on server %s", conn.RemoteAddr())
	go messageReceiver(conn)
	for {
		// keyboard input
		fmt.Print("> ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		err := scanner.Err()

		if err != nil {
			panic(err)
		}

		if conn != nil {
			conn.Write([]byte(scanner.Text()))
		} else {
			panic("Connection dropped")
			os.Exit(1)
		}

	}
}
