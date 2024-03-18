package main

import (
	"fmt"
	"net"
)
import "bufio"

func main() {
	fmt.Println("Starting server...")

	ln, _ := net.Listen("tcp", ":9875")

	conn, _ := ln.Accept()

	for {
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Println(message)
	}
}
