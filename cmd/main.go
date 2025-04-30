package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lucasolsi-wex/outbox-poc/internal/db"
	kafka2 "github.com/lucasolsi-wex/outbox-poc/internal/kafka"
	"github.com/lucasolsi-wex/outbox-poc/internal/outbox"
)

func main() {
	mongodb, err := db.NewMongoDB("mongodb://localhost:27017/?directConnection=true", "outbox")
	if err != nil {
		log.Fatalf("Failed to connect do MongoDB: %v", err)
	}

	producer, err := kafka2.NewProducer("localhost:9092")
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	processor := outbox.NewProcessor(mongodb, producer, "outbox-topic", 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go processor.Start(ctx)
	log.Printf("Outbox processor started")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}
