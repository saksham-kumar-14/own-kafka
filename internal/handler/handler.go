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
		log.Printf("Received %d bytes from %s: %x", n, conn.RemoteAddr(), receivedData)

		apiKey := binary.BigEndian.Uint16(buffer[4:6])
		apiVersion := binary.BigEndian.Uint16(buffer[6:8])

		var errorCode int16 = 0
		if apiKey == 18 && apiVersion <= 4 {
			errorCode = 0
		} else if apiKey == 75 && apiVersion == 0 {
			errorCode = 0
		} else {
			errorCode = 35
		}

		// response
		// msgType [0 - 3]
		// correlationId[4 - 7] from buffer[8:12]
		// 0, errorcode
		// API KEY
		// 0, startApiVersion
		// 0, endApiVersion
		response := [23]byte{0, 0, 0, 19, buffer[8], buffer[9], buffer[10], buffer[11], 0, byte(errorCode), 2, 0, byte(apiKey), 0, 0, 0, 4, 0, 0, 0, 0, 0, 0}

		_, err = conn.Write(response[:])
		if err != nil {
			log.Printf("Error writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
