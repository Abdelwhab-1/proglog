package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(address string) *http.Server {
	httpserver := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", httpserver.handleProduce).Methods("POST")
	router.HandleFunc("/", httpserver.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    address,
		Handler: router,
	}
}

type httpServer struct {
	log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		log: NewLog(),
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var consumeRequest ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&consumeRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	rec, err := s.log.Read(consumeRequest.Offset)
	if err != nil {
		if err == ErrOffsetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	consumeResp := ConsumeResponse{
		Record: rec,
	}
	err = json.NewEncoder(w).Encode(consumeResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

	}
	return
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var produceRequest ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&produceRequest)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	fmt.Println(*&produceRequest)
	offs, err := s.log.Append(produceRequest.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	produceResp := ProductResponse{Offset: offs}
	err = json.NewEncoder(w).Encode(produceResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	return
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProductResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}
