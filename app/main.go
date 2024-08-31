package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/server"
)

func main() {
	dir := flag.String("dir", "", "The directory where RDB files are stored")
	dbfilename := flag.String("dbfilename", "", "The name of the RDB file")
	flag.Parse()
	s, err := server.New("0.0.0.0", 6379, *dir, *dbfilename)
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
