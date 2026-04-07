package tcpapi

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"

	"distributed-kv-store/internal/persistence"
)

const (
	// StatusOK indicates the operation succeeded.
	StatusOK byte = 0
	// StatusNotFound indicates the key does not exist.
	StatusNotFound byte = 1
	// StatusError indicates a server-side error.
	StatusError byte = 2
)

// Server serves the binary key-value protocol over TCP.
type Server struct {
	store  *persistence.Store
	logger *log.Logger
}

// NewServer creates a TCP server around a persistent store.
func NewServer(store *persistence.Store, logger *log.Logger) *Server {
	return &Server{
		store:  store,
		logger: logger,
	}
}

// Serve accepts connections until the listener is closed.
func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		record, err := persistence.ReadRequest(reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			s.writeResponse(conn, StatusError, []byte(err.Error()))
			return
		}

		status, payload := s.execute(record)
		if err := s.writeResponse(conn, status, payload); err != nil {
			if s.logger != nil {
				s.logger.Printf("write response: %v", err)
			}
			return
		}
	}
}

func (s *Server) execute(record persistence.Record) (byte, []byte) {
	switch record.Command {
	case persistence.CommandSet:
		if err := s.store.Set(record.Key, record.Value); err != nil {
			return StatusError, []byte(err.Error())
		}
		return StatusOK, nil
	case persistence.CommandGet:
		value, ok := s.store.Get(record.Key)
		if !ok {
			return StatusNotFound, nil
		}
		return StatusOK, []byte(value)
	case persistence.CommandDelete:
		deleted, err := s.store.Delete(record.Key)
		if err != nil {
			return StatusError, []byte(err.Error())
		}
		if !deleted {
			return StatusNotFound, nil
		}
		return StatusOK, nil
	default:
		return StatusError, []byte("unknown command")
	}
}

func (s *Server) writeResponse(writer io.Writer, status byte, payload []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	if _, err := writer.Write([]byte{status}); err != nil {
		return err
	}
	if _, err := writer.Write(lenBuf[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := writer.Write(payload)
	return err
}
