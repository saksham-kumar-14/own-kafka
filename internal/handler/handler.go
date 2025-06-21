package handler

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

func writeInt16(w io.Writer, val int16) error {
	return binary.Write(w, binary.BigEndian, val)
}

func writeInt32(w io.Writer, val int32) error {
	return binary.Write(w, binary.BigEndian, val)
}

func readInt16(r io.Reader) (int16, error) {
	var val int16
	err := binary.Read(r, binary.BigEndian, &val)
	return val, err
}

func readInt32(r io.Reader) (int32, error) {
	var val int32
	err := binary.Read(r, binary.BigEndian, &val)
	return val, err
}

type RequestHeader struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
}

type Request struct {
	MessageSize int32
	Header      RequestHeader
	Body        []byte
}

func makeRequest(buffer []byte) (Request, error) {
	reader := bytes.NewReader(buffer)
	req := Request{}
	var err error
	req.MessageSize, err = readInt32(reader)
	if err != nil {
		return req, fmt.Errorf("msg size: %w", err)
	}
	req.Header.RequestApiKey, err = readInt16(reader)
	if err != nil {
		return req, fmt.Errorf("api key: %w", err)
	}
	req.Header.RequestApiVersion, err = readInt16(reader)
	if err != nil {
		return req, fmt.Errorf("api version: %w", err)
	}
	req.Header.CorrelationId, err = readInt32(reader)
	if err != nil {
		return req, fmt.Errorf("correlation: %w", err)
	}
	req.Body = make([]byte, req.MessageSize-8)
	_, err = io.ReadFull(reader, req.Body)
	if err != nil {
		return req, fmt.Errorf("body: %w", err)
	}
	return req, nil
}

func extractTopicNameFromRequest(body []byte) (string, error) {
	r := bytes.NewReader(body)
	var topicCount int32
	if err := binary.Read(r, binary.BigEndian, &topicCount); err != nil {
		return "", fmt.Errorf("topic count: %w", err)
	}
	if topicCount != 1 {
		return "", fmt.Errorf("expected 1 topic, got %d", topicCount)
	}
	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return "", fmt.Errorf("topic len: %w", err)
	}
	topic := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topic); err != nil {
		return "", fmt.Errorf("read topic: %w", err)
	}
	return string(topic), nil
}

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		req, err := makeRequest(buf[:n])
		if err != nil {
			log.Printf("bad request: %v", err)
			return
		}
		if req.Header.RequestApiKey == 75 && req.Header.RequestApiVersion == 0 {
			topicName, err := extractTopicNameFromRequest(req.Body)
			if err != nil {
				log.Printf("failed to extract topic: %v", err)
				return
			}
			respBody := new(bytes.Buffer)
			writeInt32(respBody, 0)
			writeInt32(respBody, 1)
			writeInt16(respBody, int16(len(topicName)))
			respBody.Write([]byte(topicName))
			respBody.Write(make([]byte, 16))
			writeInt16(respBody, 3)
			writeInt32(respBody, 0)
			resp := new(bytes.Buffer)
			writeInt32(resp, int32(4+respBody.Len()))
			writeInt32(resp, req.Header.CorrelationId)
			resp.Write(respBody.Bytes())
			conn.Write(resp.Bytes())
			return
		}
	}
}
