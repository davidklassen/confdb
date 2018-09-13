package main

import (
	"flag"
	"github.com/davidklassen/confdb/storage"
	"net/http"
)

var addr = flag.String("addr", ":8080", "confdb addr")

func main() {
	flag.Parse()
	http.Handle("/", &Handler{storage: storage.New()})
	http.ListenAndServe(*addr, nil)
}
