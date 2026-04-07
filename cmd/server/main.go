package main

import (
	"log"
	"net/http"
	"os"

	"distributed-kv-store/internal/httpapi"
	"distributed-kv-store/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	s := store.New()
	handler := httpapi.NewHandler(s)

	addr := ":" + port
	log.Printf("starting distributed-kv-store HTTP server on %s", addr)

	if err := http.ListenAndServe(addr, handler.Routes()); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
