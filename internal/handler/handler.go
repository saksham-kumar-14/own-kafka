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

func writeCompactUvarint(w io.Writer, val int) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(val))
	_, err := w.Write(buf[:n])
	return err
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

type APIVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type APIVersionResponse struct {
	ErrorCode   int16
	ApiVersions []APIVersion
}

type Response struct {
	CorrelationId      int32
	ApiVersionResponse APIVersionResponse
}

func makeRequest(buffer []byte) (Request, error) {
	reader := bytes.NewReader(buffer)
	req := Request{}
	var err error

	req.MessageSize, err = readInt32(reader)
	if err != nil {
		return req, fmt.Errorf("failed to read message size: %w", err)
	}

	req.Header.RequestApiKey, err = readInt16(reader)
	if err != nil {
		return req, fmt.Errorf("failed to read request API key: %w", err)
	}

	req.Header.RequestApiVersion, err = readInt16(reader)
	if err != nil {
		return req, fmt.Errorf("failed to read request API version: %w", err)
	}

	req.Header.CorrelationId, err = readInt32(reader)
	if err != nil {
		return req, fmt.Errorf("failed to read correlation ID: %w", err)
	}

	body := make([]byte, req.MessageSize-8)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return req, fmt.Errorf("failed to read request body: %w", err)
	}
	req.Body = body

	return req, nil
}

func writeResponse(conn net.Conn, resp Response) error {
	bodyBuffer := new(bytes.Buffer)

	if err := writeInt16(bodyBuffer, resp.ApiVersionResponse.ErrorCode); err != nil {
		return fmt.Errorf("failed to write error code: %w", err)
	}

	if err := writeCompactUvarint(bodyBuffer, len(resp.ApiVersionResponse.ApiVersions)+1); err != nil {
		return fmt.Errorf("failed to write API versions array length: %w", err)
	}

	for _, apiVersion := range resp.ApiVersionResponse.ApiVersions {
		if err := writeInt16(bodyBuffer, apiVersion.ApiKey); err != nil {
			return fmt.Errorf("failed to write API key %d: %w", apiVersion.ApiKey, err)
		}
		if err := writeInt16(bodyBuffer, apiVersion.MinVersion); err != nil {
			return fmt.Errorf("failed to write min version for API %d: %w", apiVersion.ApiKey, err)
		}
		if err := writeInt16(bodyBuffer, apiVersion.MaxVersion); err != nil {
			return fmt.Errorf("failed to write max version for API %d: %w", apiVersion.ApiKey, err)
		}
		if err := writeCompactUvarint(bodyBuffer, 0); err != nil {
			return fmt.Errorf("failed to write tagged fields for API %d: %w", apiVersion.ApiKey, err)
		}
	}

	if err := writeInt32(bodyBuffer, 0); err != nil {
		return fmt.Errorf("failed to write throttle_time_ms: %w", err)
	}
	if err := writeCompactUvarint(bodyBuffer, 0); err != nil {
		return fmt.Errorf("failed to write top-level tagged fields: %w", err)
	}

	messageSize := int32(4 + bodyBuffer.Len())

	if err := writeInt32(conn, messageSize); err != nil {
		return fmt.Errorf("failed to write message size: %w", err)
	}
	if err := writeInt32(conn, resp.CorrelationId); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}
	if _, err := conn.Write(bodyBuffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write response body: %w", err)
	}

	return nil
}

func extractTopicNameFromRequest(body []byte) (string, error) {
	r := bytes.NewReader(body)

	var topicCount int32
	if err := binary.Read(r, binary.BigEndian, &topicCount); err != nil {
		return "", fmt.Errorf("failed to read topic count: %w", err)
	}
	if topicCount != 1 {
		return "", fmt.Errorf("expected 1 topic, got %d", topicCount)
	}

	var nameLen int16
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return "", fmt.Errorf("failed to read topic name length: %w", err)
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return "", fmt.Errorf("failed to read topic name bytes: %w", err)
	}
	return string(nameBytes), nil
}

func HandleConnection(conn net.Conn) {
	defer func() {
		log.Printf("Closing connection from %s", conn.RemoteAddr())
		conn.Close()
	}()

	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	for {
		buffer := make([]byte, 4096)
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
		log.Printf("Received %d bytes from %s. Data: %x", n, conn.RemoteAddr(), receivedData)

		request, err := makeRequest(receivedData)
		if err != nil {
			log.Printf("Error parsing request from %s: %v", conn.RemoteAddr(), err)
			return
		}

		log.Printf("Parsed Request: API Key=%d, Version=%d, Correlation ID=%d, Size=%d",
			request.Header.RequestApiKey,
			request.Header.RequestApiVersion,
			request.Header.CorrelationId,
			request.MessageSize,
		)

		if request.Header.RequestApiKey == 75 && request.Header.RequestApiVersion == 0 {
			topicName, err := extractTopicNameFromRequest(request.Body)
			if err != nil {
				log.Printf("Failed to extract topic name: %v", err)
				return
			}

			respBuf := new(bytes.Buffer)
			_ = writeInt32(respBuf, 0)
			_ = writeInt32(respBuf, 1)
			_ = writeInt16(respBuf, int16(len(topicName)))
			_, _ = respBuf.Write([]byte(topicName))
			_, _ = respBuf.Write(make([]byte, 16))
			_ = writeInt16(respBuf, 3)
			_ = writeInt32(respBuf, 0)

			fullResp := new(bytes.Buffer)
			_ = writeInt32(fullResp, int32(4+respBuf.Len()))
			_ = writeInt32(fullResp, request.Header.CorrelationId)
			_, _ = fullResp.Write(respBuf.Bytes())

			_, err = conn.Write(fullResp.Bytes())
			if err != nil {
				log.Printf("Error writing DescribeTopicPartitions response: %v", err)
			}
			continue
		}

		supportedAPIs := []APIVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
			{ApiKey: 75, MinVersion: 0, MaxVersion: 0},
		}

		var responseErrorCode int16 = 0
		foundAPI := false
		for _, supportedAPI := range supportedAPIs {
			if request.Header.RequestApiKey == supportedAPI.ApiKey {
				foundAPI = true
				if request.Header.RequestApiVersion < supportedAPI.MinVersion || request.Header.RequestApiVersion > supportedAPI.MaxVersion {
					responseErrorCode = 35
				}
				break
			}
		}
		if !foundAPI {
			responseErrorCode = 35
		}

		response := Response{
			CorrelationId: request.Header.CorrelationId,
			ApiVersionResponse: APIVersionResponse{
				ErrorCode:   responseErrorCode,
				ApiVersions: supportedAPIs,
			},
		}

		if err := writeResponse(conn, response); err != nil {
			log.Printf("Error writing response to %s: %v", conn.RemoteAddr(), err)
			return
		}

		log.Printf("Successfully sent APIVersions response to %s (Correlation ID: %d, ErrorCode: %d)",
			conn.RemoteAddr(), response.CorrelationId, response.ApiVersionResponse.ErrorCode)
	}
}
