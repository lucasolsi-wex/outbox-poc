package db

import (
	"context"
	"encoding/json"
	"time"

	"github.com/lucasolsi-wex/outbox-poc/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
	client           *mongo.Client
	database         *mongo.Database
	collection       *mongo.Collection
	outboxCollection *mongo.Collection
}

func NewMongoDB(uri, dbName string) (*MongoDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	database := client.Database(dbName)
	callsCollection := database.Collection("calls")
	outboxCollection := database.Collection("outbox")

	return &MongoDB{
		client:           client,
		database:         database,
		collection:       callsCollection,
		outboxCollection: outboxCollection,
	}, nil
}

func (db *MongoDB) SaveCallWithOutboxMessage(ctx context.Context, call model.Call) error {
	session, err := db.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessionCtx mongo.SessionContext) (interface{}, error) {
		_, err := db.collection.InsertOne(sessionCtx, call)
		if err != nil {
			return nil, err
		}

		outboxMsg := model.Message{
			ID:          primitive.NewObjectID().Hex(),
			AggregateID: call.CallID,
			Type:        model.CallCreated,
			Payload:     marshallToJSON(call),
			Status:      model.MessageStatusPending,
			CreatedAt:   time.Now(),
		}

		_, err = db.outboxCollection.InsertOne(sessionCtx, outboxMsg)
		return nil, err
	})

	return err
}

func (db *MongoDB) GetPendingMessages(ctx context.Context, limit int64) ([]model.Message, error) {
	var messages []model.Message
	opts := options.Find().SetLimit(limit).SetSort(bson.D{{"created_at", 1}})
	cursor, err := db.outboxCollection.Find(ctx, bson.M{"status": model.MessageStatusPending}, opts)
	if err != nil {
		return nil, err
	}

	if err = cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}

func (db *MongoDB) UpdateMessageStatus(ctx context.Context, messageID string, status model.MessageStatus) error {
	_, err := db.outboxCollection.UpdateOne(ctx, bson.M{"_id": messageID}, bson.M{"$set": bson.M{"status": status}})
	return err
}

func marshallToJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}
