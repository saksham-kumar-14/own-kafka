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

		response := []byte{0x00, 0x00, 0x00, 0x00} // only message size initially
		correlationId := buffer[8:12]
		apiVersion := int(buffer[6])<<8 | int(buffer[7])

		var errorCode byte = 0
		if apiVersion < 0 || apiVersion > 4 {
			errorCode = 35
		}

		response = append(response, correlationId...)
		response = append(response, []byte{0x00, errorCode}...)

		_, err = conn.Write(response)
		if err != nil {
			log.Printf("Error writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
