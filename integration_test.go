//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/VAibhav1031/backpressure-go/handler"
	"github.com/joho/godotenv"
	"log"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTheDamnServer2(t *testing.T) {
	fmt.Println("Integration-Test Started...")

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
	} else {
		log.Println("DB!! , ALL SET ")
	}

	test_samples := []handler.UserDetails{
		{
			Username:    "noice_142",
			MfaCode:     "34322341",
			DisplayName: "noic_choco",
		},
		{
			Username:    "dancy_214",
			MfaCode:     "7223451",
			DisplayName: "dancy_doll",
		},
		{
			Username:    "mono_093",
			MfaCode:     "0934023",
			DisplayName: "mono_loco",
		},
		{
			Username:    "choco_laty_12",
			MfaCode:     "145766",
			DisplayName: "laty_raise990",
		},
	}

	worker := handler.NewPooler(pool)
	// i need to loop these thing for 1500-1800 req ,   but after hitting  that we would wait and stat thing again would be nice to go
	var too_many, accepted int

	jobChan := make(chan handler.UserDetails, 5000)
	collector := make(chan []handler.UserDetails, 2000)
	dbSender := make(chan []handler.UserDetails, 3000)
	defer close(jobChan)
	defer close(collector)
	defer close(dbSender)

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
		log.Printf("%d DB worker started", i)
		go worker.DBWorker(wg, dbSender)
	}

	go handler.FlowManager(jobChan, collector)

	go handler.BatchManager(collector, dbSender)

	router := MakeServerHandler(jobChan)

	for i := 0; i < 2500; i++ {

		for _, test := range test_samples {
			st, _ := json.Marshal(test)
			new_req := httptest.NewRequest("POST", "http://127.0.0.1:8033/post", bytes.NewReader(st))
			new_req.Header.Set("Content-Type", "application/json")
			// we need to know the response also
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, new_req)

			// we need to check the result  for each req send to the handler through the router engine (handler.serveHTTP)
			// if rec.Code != http.StatusAccepted {
			// 	t.Errorf("Expected 202 (accepted..) but got %d", rec.Code)
			// }

			if rec.Header().Get("Content-Type") != "application/json" {
				t.Errorf("Wrong Content-Type")
			}

			var resp_det handler.Accepted
			err := json.NewDecoder(rec.Body).Decode(&resp_det)
			if err != nil {
				t.Fatalf("JSON Decoder Problem : %v", err)
			}

			if resp_det.Code == "ACCEPTED" {
				accepted++
				// fmt.Println("Accepted")
			} else if resp_det.Code == "TOO_MANY_REQUESTS" {
				too_many++
				// fmt.Println("Too Many Request")
			} else {
				t.Errorf("Incorrect 'CODE' : %v", resp_det.Code)
			}

		}
		// just for simulation work real time , even though it is not but still
		time.Sleep(2 * time.Millisecond)

	}
	fmt.Printf("\nResult Too-Many-Request: %d, Accepted: %d", too_many, accepted)
}
