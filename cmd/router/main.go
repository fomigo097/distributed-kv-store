package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"distributed-kv-store/internal/router"
)

func main() {
	port := os.Getenv("ROUTER_PORT")
	if port == "" {
		port = "7100"
	}

	leaders := parseLeaders(os.Getenv("SHARD_LEADERS"))
	if len(leaders) == 0 {
		log.Fatal("SHARD_LEADERS must be set, for example shard-a=http://127.0.0.1:7001,shard-b=http://127.0.0.1:7011")
	}

	r := router.New(leaders, 64)
	addr := ":" + port
	log.Printf("starting shard router on %s", addr)
	if err := http.ListenAndServe(addr, r.Handler()); err != nil {
		log.Fatalf("router stopped: %v", err)
	}
}

func parseLeaders(raw string) map[string]string {
	out := make(map[string]string)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			continue
		}
		out[parts[0]] = parts[1]
	}
	return out
}
