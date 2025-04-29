package model

import "time"

type Call struct {
	CallID       string `json:"call_id"`
	CallerNumber string `json:"caller_number"`
	CalledNumber string `json:"called_number"`
	CallDuration int    `json:"call_duration"`
}

type MessageStatus string

const (
	MessageStatusPending    MessageStatus = "PENDING"
	MessageStatusProcessing MessageStatus = "PROCESSING"
	MessageStatusProcessed  MessageStatus = "PROCESSED"
)

type MessageType string

const (
	CallCreated MessageType = "CALL_CREATED"
)

type Message struct {
	ID          string        `bson:"_id"`
	AggregateID string        `bson:"aggregate_id"`
	Type        MessageType   `bson:"type"`
	Payload     string        `bson:"payload"`
	Status      MessageStatus `bson:"status"`
	CreatedAt   time.Time     `bson:"created_at"`
}
