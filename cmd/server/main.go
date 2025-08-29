package main

import (
	"log"
	"net/http"

	"distributed-storage/internal/api"
)

func main() {
	router := api.NewRouter()
	log.Println("Server started on http://localhost:8080")
	log.Println("Use the `README.md` file for instructions on how to test.")
	log.Fatal(http.ListenAndServe(":8080", router))
}
