package handler

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"time"
)

func ConnectDB(connString string) *pgxpool.Pool {

	config, err := pgxpool.ParseConfig(connString)

	if err != nil {
		log.Fatal("Cant parse the config", err)

	}

	config.MaxConns = 75
	config.MinConns = 10
	config.MaxConnIdleTime = 10 * time.Second // fast failure (cause heavy write, we really dont want to hog the thread for thte thing)
	config.MaxConnLifetime = 10 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)

	if err != nil {
		log.Fatal("Cant connect to DB", err)

	}

	return pool
}

