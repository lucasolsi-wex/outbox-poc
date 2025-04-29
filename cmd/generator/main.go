package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucasolsi-wex/outbox-poc/internal/db"
	"github.com/lucasolsi-wex/outbox-poc/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
	log.Println("Starting data generator")
	mongodb, err := db.NewMongoDB("mongodb://localhost:27017/?directConnection=true", "outbox")
	if err != nil {
		log.Fatalf("Failed to connect do MongoDB: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			call := generateRandomCall()
			err := mongodb.SaveCallWithOutboxMessage(ctx, call)
			if err != nil {
				log.Printf("Failed to save call: %v", err)
				continue
			}

			log.Printf("Generated call with ID: %s, CallerNumber: %s", call.CallID, call.CallerNumber)
		case <-sigCh:
			log.Println("Shutting down data generator...")
			return
		}
	}
}

func generateRandomCall() model.Call {
	return model.Call{
		CallID:       primitive.NewObjectID().Hex(),
		CallerNumber: "+1234567890",
		CalledNumber: "+0034567800",
		CallDuration: 2,
	}
}
