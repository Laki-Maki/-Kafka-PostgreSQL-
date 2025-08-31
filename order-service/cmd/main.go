package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"order-service/internal/cache"
	"order-service/internal/db"
	"order-service/internal/kafka"
)

func main() {
	// Подключение к PostgreSQL
	dbConn, err := db.Connect("user=demo_user password=demo_pass dbname=orders_db host=localhost port=5432 sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	// Инициализация кэша и загрузка данных из БД
	c := cache.New()
	orders, err := dbConn.LoadAllOrders()
	if err != nil {
		log.Printf("failed to load orders from DB: %v", err)
	}
	c.Load(orders)
	log.Printf("cache warmup: %d order(s) loaded from DB", c.Len())

	// Запуск Kafka consumer
	go kafka.StartConsumer(dbConn, c, "my-new-topic10", "localhost:9092")

	// HTTP API: отдаём JSON по /order/<id>
	http.HandleFunc("/order/", orderHandler(c))

	// Отдаём фронтенд-статик из internal/web
	fs := http.FileServer(http.Dir("./internal/web"))
	http.Handle("/", fs)

	log.Println("listening on :8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatal(err)
	}
}

func orderHandler(c *cache.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/order/")
		if id == "" {
			http.Error(w, "order id required", http.StatusBadRequest)
			return
		}
		if o, ok := c.Get(id); ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(o)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "order not found"})
	}
}
