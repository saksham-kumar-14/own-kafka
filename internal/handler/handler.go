package handler

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
)

func putInt16(val int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(val))
	return buf
}

func putInt32(val int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(val))
	return buf
}

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

		correlationId := buffer[8:12]
		apiKey := binary.BigEndian.Uint16(buffer[4:6])
		apiVersion := binary.BigEndian.Uint16(buffer[6:8])

		var errorCode int16 = 0
		if apiVersion > 4 || apiKey != 18 {
			errorCode = 35
		}

		responseBodyBuf := new(bytes.Buffer)

		responseBodyBuf.Write(putInt16(errorCode))

		responseBodyBuf.WriteByte(0x01)

		responseBodyBuf.Write(putInt16(18))
		responseBodyBuf.Write(putInt16(0))
		responseBodyBuf.Write(putInt16(4))

		responseBodyBuf.WriteByte(0x00)
		responseBodyBuf.WriteByte(0x00)

		finalResponse := new(bytes.Buffer)

		messageLength := int32(len(correlationId) + responseBodyBuf.Len())
		finalResponse.Write(putInt32(messageLength))

		finalResponse.Write(responseBodyBuf.Bytes())

		bytesWritten, err := conn.Write(finalResponse.Bytes())
		if err != nil {
			log.Printf("Error writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
		log.Printf("Sent %d bytes to %s. Response: %x", bytesWritten, conn.RemoteAddr(), finalResponse.Bytes())
	}
}
