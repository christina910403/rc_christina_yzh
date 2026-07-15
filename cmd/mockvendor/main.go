package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

func main() {
	address := flag.String("address", ":9090", "listen address")
	flag.Parse()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /inventory/adjustments", handler("inventory"))
	mux.HandleFunc("POST /crm/orders", handler("crm"))
	server := &http.Server{Addr: *address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("mock vendor listening on %s", *address)
	log.Fatal(server.ListenAndServe())
}

func handler(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var body any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		status := http.StatusOK
		if raw := r.URL.Query().Get("status"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				status = parsed
			}
		}
		fmt.Printf("vendor=%s event_id=%s body=%v status=%d\n", name, r.Header.Get("X-Event-ID"), body, status)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"received":true,"vendor":%q}`, name)
	}
}
