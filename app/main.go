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
	port := flag.Int("port", 6379, "The port to run redis on")
	flag.Parse()
	s, err := server.New("0.0.0.0", *port, *dir, *dbfilename)
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
