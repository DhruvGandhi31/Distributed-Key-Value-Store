package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/server"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
)

func main() {
	clientAddr := flag.String("client-addr", "127.0.0.1:8000", "address for the client-facing HTTP")
	flag.Parse()

	kv := store.New()
	srv := server.New(kv)

	log.Printf("kvstored listening on %s", *clientAddr)
	if err := http.ListenAndServe(*clientAddr, srv); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
