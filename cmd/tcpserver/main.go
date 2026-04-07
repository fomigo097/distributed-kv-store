package main

import (
	"errors"
	"log"
	"net"
	"os"

	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/tcpapi"
)

func main() {
	port := os.Getenv("TCP_PORT")
	if port == "" {
		port = "9090"
	}

	walPath := os.Getenv("WAL_PATH")
	if walPath == "" {
		walPath = "data/kv.wal"
	}

	store, err := persistence.OpenStore(walPath)
	if err != nil {
		log.Fatalf("open persistent store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close store: %v", err)
		}
	}()

	addr := ":" + port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", addr, err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("close listener: %v", err)
		}
	}()

	log.Printf("starting persistent TCP server on %s with WAL at %s", addr, walPath)

	server := tcpapi.NewServer(store, log.Default())
	if err := server.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Fatalf("tcp server stopped: %v", err)
	}
}
