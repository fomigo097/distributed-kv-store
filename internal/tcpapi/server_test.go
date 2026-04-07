package tcpapi

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"path/filepath"
	"testing"
	"time"

	"distributed-kv-store/internal/persistence"
)

func TestServerRoundTripAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.wal")

	store, err := persistence.OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := NewServer(store, log.New(io.Discard, "", 0))
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(listener)
	}()

	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	reader := bufio.NewReader(conn)

	if err := persistence.WriteRequest(conn, persistence.Record{
		Command: persistence.CommandSet,
		Key:     "pet",
		Value:   "cat",
	}); err != nil {
		t.Fatalf("write set request: %v", err)
	}

	status, payload, err := readResponse(reader)
	if err != nil {
		t.Fatalf("read set response: %v", err)
	}
	if status != StatusOK || len(payload) != 0 {
		t.Fatalf("expected set success, got status=%d payload=%q", status, payload)
	}

	if err := persistence.WriteRequest(conn, persistence.Record{
		Command: persistence.CommandGet,
		Key:     "pet",
	}); err != nil {
		t.Fatalf("write get request: %v", err)
	}

	status, payload, err = readResponse(reader)
	if err != nil {
		t.Fatalf("read get response: %v", err)
	}
	if status != StatusOK || string(payload) != "cat" {
		t.Fatalf("expected get success with cat, got status=%d payload=%q", status, payload)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	serveErr := <-done
	if serveErr == nil {
		t.Fatalf("expected serve to stop after listener close")
	}

	recovered, err := persistence.OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer recovered.Close()

	if got, ok := recovered.Get("pet"); !ok || got != "cat" {
		t.Fatalf("expected recovery to restore pet=cat, got %q ok=%v", got, ok)
	}
}

func readResponse(reader *bufio.Reader) (byte, []byte, error) {
	status, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		return 0, nil, err
	}

	size := binary.BigEndian.Uint32(lenBuf[:])
	if size == 0 {
		return status, nil, nil
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return status, payload, nil
}
