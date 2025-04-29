package outbox

import (
	"context"
	"log"
	"time"

	"github.com/lucasolsi-wex/outbox-poc/internal/db"
	kafka2 "github.com/lucasolsi-wex/outbox-poc/internal/kafka"
	"github.com/lucasolsi-wex/outbox-poc/model"
)

type Processor struct {
	mongodb  *db.MongoDB
	producer *kafka2.Producer
	topic    string
	interval time.Duration
}

func NewProcessor(mongodb *db.MongoDB, producer *kafka2.Producer, topic string, interval time.Duration) *Processor {
	return &Processor{
		mongodb:  mongodb,
		producer: producer,
		topic:    topic,
		interval: interval,
	}
}

func (p *Processor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.processMessages(ctx); err != nil {
				log.Printf("Error processing messages: %v", err)
			}
		}
	}
}

func (p *Processor) processMessages(ctx context.Context) error {
	messages, err := p.mongodb.GetPendingMessages(ctx, 100)
	if err != nil {
		return err
	}

	for _, eachMsg := range messages {
		if err := p.mongodb.UpdateMessageStatus(ctx, eachMsg.ID, model.MessageStatusProcessing); err != nil {
			log.Printf("Error updating message status to PROCESSING: %v", err)
			continue
		}

		err := p.producer.SendMessage(p.topic, eachMsg.AggregateID, eachMsg.Payload)
		if err != nil {
			_ = p.mongodb.UpdateMessageStatus(ctx, eachMsg.ID, model.MessageStatusPending)
			log.Printf("Error sending message to Kafka: %v", err)
			continue
		}

		if err := p.mongodb.UpdateMessageStatus(ctx, eachMsg.ID, model.MessageStatusProcessed); err != nil {
			log.Printf("Error updating message status to PROCESSED: %v", err)
			continue
		}
	}

	return nil
}
