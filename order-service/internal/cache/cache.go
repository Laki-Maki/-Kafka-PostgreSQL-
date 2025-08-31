package cache

import (
	"order-service/internal/models"
	"sync"
)

type Cache struct {
	mu    sync.RWMutex
	store map[string]models.Order
}

// Создаёт пустой кэш (с пустой map).
func New() *Cache {
	return &Cache{store: make(map[string]models.Order)}
}

// Добавление заказа
func (c *Cache) Set(o models.Order) {
	c.mu.Lock()
	c.store[o.OrderUID] = o
	c.mu.Unlock()
}

// Получение заказа
func (c *Cache) Get(id string) (models.Order, bool) {
	c.mu.RLock()
	o, ok := c.store[id]
	c.mu.RUnlock()
	return o, ok
}

// Загрузка списка заказов
func (c *Cache) Load(list []models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, o := range list {
		c.store[o.OrderUID] = o
	}
}

// Размер кэша
func (c *Cache) Len() int {
	c.mu.RLock()
	n := len(c.store)
	c.mu.RUnlock()
	return n
}
