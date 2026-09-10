package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ----------ALL-JSON's------------
type UserDetails struct {
	Username    string `json:"username"`
	MfaCode     string `json:"mfa_code"`
	DisplayName string `json:"display_name"`
}
type tooManyRequests struct {
	Code       string `json:"code"`
	StatusCode int    `json:"status_code"`
}
type Accepted struct {
	Code       string `json:"code"`
	StatusCode int    `json:"status_code"`
}

// --------------------------------

type DbPooler struct {
	dbPool *pgxpool.Pool
}

func NewPooler(p *pgxpool.Pool) *DbPooler {
	return &DbPooler{dbPool: p}
}

func BatchHttpHandler(jobChan chan UserDetails) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {

		defer req.Body.Close()

		var user UserDetails
		if err := json.NewDecoder(req.Body).Decode(&user); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			fmt.Println("json NewDecoder", err)
			return
		}

		// the error when i try to do
		if len(jobChan) >= 5000 {
			st := tooManyRequests{Code: "TOO_MANY_REQUESTS", StatusCode: 429}
			ret, err := json.Marshal(st)
			if err != nil {
				fmt.Println("Marshal Error", err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write(ret)
			return
		} //429

		// send thing to the channel, still room left
		jobChan <- user
		s_data := Accepted{Code: "ACCEPTED", StatusCode: 202}
		js, _ := json.Marshal(s_data)

		// it must be in the order, before sending the data, cause with that they packed up together
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write(js)
	}

}

func FlowManager(jobchan chan UserDetails, collector chan []UserDetails) {

	t := time.NewTicker(1 * time.Second)
	var det []UserDetails
	for {
		select {

		case <-t.C:
			rate := (5000 / 6)
			// cap thing brother ....
			if len(jobchan) < rate {
				rate = len(jobchan)
			}
			log.Println("rate:", rate)
			for i := 0; i <= rate; i++ {
				det = append(det, <-jobchan)
			}

			log.Printf("[Flow]:  Send the %v batch to the collector", len(det))
			collector <- det
			det = nil
		}
	}
}

func BatchManager(collector chan []UserDetails, dbSender chan []UserDetails) {

	t := time.NewTicker(850 * time.Millisecond)

	var rqueue []UserDetails
	for {
		select {
		case collection := <-collector:
			if len(rqueue) >= 3000 {
				dbSender <- rqueue
				rqueue = nil
			}
			rqueue = append(rqueue, collection...)
		case <-t.C:
			// log.Println("[Timer-out]:  Send the batch to the DbSender ")
			dbSender <- rqueue
			rqueue = nil

		}

	}

}

func (w *DbPooler) DBWorker(wg *sync.WaitGroup, dbSender chan []UserDetails) {
	// what we neexd to validate the incoming thing and just send to the commiter
	tries := 0
	for batch := range dbSender {
		// call the function
		if tries == 5 {
			wg.Done()
			return
		}

		if len(batch) >= 1 {
			err := w.dbCommit(context.Background(), batch) //let this function be the private it is not needed to be imported
			if err != nil {
				fmt.Printf("dbCommit Error: %v", err)
				tries++
			} else {
				fmt.Printf("Commited this batch %v\n", len(batch))
			}
		}
	}
}

type taskSource struct {
	index int
	tasks []UserDetails
}

// -----------TaskSoure-RELATED----------------------
func (s *taskSource) Next() bool {
	s.index++
	return s.index < len(s.tasks)
}
func (s *taskSource) Values() ([]any, error) {
	t := s.tasks[s.index]
	return []any{t.Username, t.MfaCode, t.DisplayName}, nil
}
func (s *taskSource) Err() error {
	return nil
}

// -------------------------------------

func (w *DbPooler) dbCommit(ctx context.Context, batch []UserDetails) error {

	source := &taskSource{
		index: -1,
		tasks: batch,
	}

	_, err := w.dbPool.CopyFrom(
		ctx,
		pgx.Identifier{"user_tab"},
		[]string{"username", "mfa_code", "display_name"},
		// pgx.CopyFromRows(rows),
		source,
	)

	if err != nil {

		fmt.Printf("Batch commit failed: %v", err)
		return err
	}
	fmt.Printf("Batch Commited Successfully")
	return nil

}
