package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/VAibhav1031/backpressure-go/handler"
	"github.com/joho/godotenv"
)

func MakeServerHandler(jobChan chan handler.UserDetails) *http.ServeMux {

	mux := http.NewServeMux()
	mux.HandleFunc("POST /post", handler.BatchHttpHandler(jobChan))
	return mux

}

func main() {
	fmt.Println("Welcome to backpressure-test server")
	//------------DB handling start---------------------

	err := godotenv.Load() // loading .env file
	if err != nil {
		log.Println("No .env file found, using system enviromentt ..")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatalf("DATABASE_URL not set: '%v'", dbURL)
	}

	pool := handler.ConnectDB(dbURL) // connecting to the DB , will get the pool connection
	defer pool.Close()

	err = pool.Ping(context.Background())
	if err != nil {
		log.Fatalf("Database is reachable but not responding: %v", err)

	}
	//--------------DB handling end----------------------

	// initiations:
	jobchan := make(chan handler.UserDetails, 5000)
	collector := make(chan []handler.UserDetails, 2000)
	dbSender := make(chan []handler.UserDetails, 3000)
	defer close(jobchan)
	defer close(collector)

	// handling db Workers
	worker := handler.NewPooler(pool)

	// --------------DB_WORKER_MNMNT_START-------
	wg := &sync.WaitGroup{}
	server_stop := make(chan struct{})
	go func() {
		wg.Wait()
		fmt.Println("Error ,DBWorker Got some Error's")
		server_stop <- struct{}{}
	}()
	// ------------DB_WORKERK_MNGMNT_END---------

	for i := 0; i <= 3; i++ {
		wg.Add(1)
		go worker.DBWorker(wg, dbSender)
	}

	go handler.FlowManager(jobchan, collector)

	// handler..
	mux := MakeServerHandler(jobchan)

	server := &http.Server{
		Addr:           ":8033",
		Handler:        mux,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	server.ListenAndServe()
	select {
	case <-server_stop:
		fmt.Println("Server Going Down.... ")
		return
	}
}
