package main

import (
	"log"

	"github.com/saksham-kumar-14/kafka-impl/internal/server"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("Starting TCP server application...")

	tcpServer := server.NewTCPServer("0.0.0.0:9092")

	if err := tcpServer.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
