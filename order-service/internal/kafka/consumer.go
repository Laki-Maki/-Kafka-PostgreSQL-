package kafka

import (
	"context"
	"encoding/json"
	"log"
	"order-service/internal/cache"
	"order-service/internal/db"
	"order-service/internal/models"

	"github.com/segmentio/kafka-go"
)

// StartConsumer запускает подписку на Kafka и обработку заказов
func StartConsumer(dbConn *db.DB, c *cache.Cache, topic, broker string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "order-service-group",
	})
	defer r.Close()

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("kafka read error: %v", err)
			continue
		}

		var order models.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("invalid order JSON: %v", err)
			continue
		}

		if err := dbConn.InsertOrder(order); err != nil {
			log.Printf("db insert error: %v", err)
			continue
		}

		c.Set(order)
		log.Printf("order %s saved & cached", order.OrderUID)
	}
}
