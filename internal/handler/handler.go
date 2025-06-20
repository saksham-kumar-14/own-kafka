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

		correlationId := receivedData[8:12]
		requestApiKey := binary.BigEndian.Uint16(receivedData[4:6])
		requestApiVersion := binary.BigEndian.Uint16(receivedData[6:8])

		log.Printf("Request API Key: %d, Version: %d, Correlation ID: %x",
			requestApiKey, requestApiVersion, correlationId)

		var errorCode int16 = 0

		responseBodyBuf := new(bytes.Buffer)

		responseBodyBuf.Write(putInt16(errorCode))

		responseBodyBuf.WriteByte(0x02)

		responseBodyBuf.Write(putInt16(18))
		responseBodyBuf.Write(putInt16(0))
		responseBodyBuf.Write(putInt16(4))
		responseBodyBuf.WriteByte(0x00)

		responseBodyBuf.Write(putInt16(75))
		responseBodyBuf.Write(putInt16(0))
		responseBodyBuf.Write(putInt16(0))
		responseBodyBuf.WriteByte(0x00)

		responseBodyBuf.WriteByte(0x00)

		finalResponse := new(bytes.Buffer)

		messageLength := int32(len(correlationId) + responseBodyBuf.Len())
		finalResponse.Write(putInt32(messageLength))

		finalResponse.Write(correlationId)

		finalResponse.Write(responseBodyBuf.Bytes())

		bytesWritten, err := conn.Write(finalResponse.Bytes())
		if err != nil {
			log.Printf("Error writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
		log.Printf("Sent %d bytes to %s. Response: %x", bytesWritten, conn.RemoteAddr(), finalResponse.Bytes())
	}
}
