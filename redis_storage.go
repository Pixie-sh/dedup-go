package dedup

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage implements the binary-safe Storage interface with Redis
type RedisStorage struct {
	client *redis.Client
}

// NewRedisStorage creates a new RedisStorage instance
func NewRedisStorage(_ context.Context, client *redis.Client) *RedisStorage {
	return &RedisStorage{client: client}
}

// Get retrieves a binary-safe value by key
func (r *RedisStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	cmd := r.client.Get(ctx, string(key))
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(cmd.Val()), nil
}

// Exists checks if the given binary key exists
func (r *RedisStorage) Exists(ctx context.Context, key []byte) (bool, error) {
	val, err := r.client.Exists(ctx, string(key)).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// SetEX stores a binary-safe value with a key and expiration time in Redis
func (r *RedisStorage) SetEX(ctx context.Context, key []byte, value []byte, expiration ...time.Duration) error {
	exp := time.Hour // default
	if len(expiration) > 0 {
		exp = expiration[0]
	}
	return r.client.Set(ctx, string(key), value, exp).Err()
}

// TTL retrieves the remaining time-to-live for a given binary key
func (r *RedisStorage) TTL(ctx context.Context, key []byte) (time.Duration, error) {
	return r.client.TTL(ctx, string(key)).Result()
}
