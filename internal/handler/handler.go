package handler

import (
	"io"
	"log"
	"net"
)

func HandleConnection(conn net.Conn) {
	defer func() {
		log.Printf("Closing connection from %s", conn.RemoteAddr())
		conn.Close()
	}()

	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client %s disconnected", conn.RemoteAddr())
				return
			}
			log.Printf("Error reading from %s: %v", conn.RemoteAddr(), err)
			return
		}

		receivedData := buffer[:n]
		log.Printf("Received %d bytes from %s: %s", n, conn.RemoteAddr(), string(receivedData))

		message_size := []byte{0x00, 0x00, 0x00, 0x00}
		correlation_id := buffer[8:12]
		bytesWritten, err := conn.Write(append(message_size, correlation_id...))
		if err != nil {
			log.Printf("Error writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
		log.Printf("Sent %d bytes to %s", bytesWritten, bytesWritten)
	}
}
