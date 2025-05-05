package dedup

import (
"context"
"time"

"github.com/redis/go-redis/v9"
)

// RedisStorage implements the Storage interface with Redis
type RedisStorage struct {
	client *redis.Client
}

// NewRedisStorage creates a new RedisStorage instance
func NewRedisStorage(_ context.Context, client *redis.Client) *RedisStorage {
	return &RedisStorage{client: client}
}

// Exists checks if a key exists in Redis
func (r *RedisStorage) Exists(ctx context.Context, key string) (bool, error) {
	val, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// SetEX stores a value with a key and expiration time in Redis
func (r *RedisStorage) SetEX(ctx context.Context, key string, value string, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// TTL returns the remaining time-to-live for a key in Redis
func (r *RedisStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
	return r.client.TTL(ctx, key).Result()
}
