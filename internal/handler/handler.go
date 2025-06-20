package handler

import (
	"encoding/binary"
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

		correlationId := buffer[8:12]
		apiVersion := binary.BigEndian.Uint16(buffer[6:8])

		var errorCode byte = 0
		if apiVersion > 4 {
			errorCode = 35
		}

		response := []byte{0x00, 0x00, 0x00, 0x00} // only message size initially
		response = append(response, correlationId...)
		response = append(response, []byte{0x00, errorCode}...)
		response = append(response, []byte{18}...)
		response = append(response, []byte{0}...)
		response = append(response, []byte{4}...)

		_, err = conn.Write(response)
		if err != nil {
			log.Printf("Error writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
