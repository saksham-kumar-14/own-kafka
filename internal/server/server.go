package server

import (
	"log"
	"net"

	"github.com/saksham-kumar-14/kafka-impl/internal/handler"
)

type TCPServer struct {
	ListenAddr string
	listener   net.Listener
}

func NewTCPServer(addr string) *TCPServer {
	return &TCPServer{
		ListenAddr: addr,
	}
}

func (s *TCPServer) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}

	log.Printf("Server listening on %s", s.listener.Addr().String())

	defer func() {
		log.Println("Closing server listener")
		s.listener.Close()
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handler.HandleConnection(conn)
	}
}

func (s *TCPServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
