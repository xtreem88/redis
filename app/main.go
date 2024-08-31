package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/server"
)

func main() {
	s, err := server.New("0.0.0.0:6379")
	if err != nil {
		fmt.Printf("Failed to create server: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Server starting...")
	if err := s.Listen(); err != nil {
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	}
}
