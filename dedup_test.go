package dedup

import (
	"context"
	"crypto/sha1"
	"hash"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedisStorage(t *testing.T) {
	// Setup miniredis
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	// Create a Redis client connected to miniredis
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	// Create storage
	ctx := context.Background()
	storage := NewRedisStorage(ctx, client)

	t.Run("Exists", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		// Key doesn't exist initially
		exists, err := storage.Exists(ctx, "test-key")
		assert.NoError(t, err)
		assert.False(t, exists)

		// Set the key
		err = mr.Set("test-key", "value")
		assert.NoError(t, err)

		// Now it should exist
		exists, err = storage.Exists(ctx, "test-key")
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("SetEX", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		// Set a key with expiration
		err := storage.SetEX(ctx, "test-key", "value", 1*time.Second)
		assert.NoError(t, err)

		// Verify it exists
		exists, err := storage.Exists(ctx, "test-key")
		assert.NoError(t, err)
		assert.True(t, exists)

		// Verify the value
		val, err := client.Get(ctx, "test-key").Result()
		assert.NoError(t, err)
		assert.Equal(t, "value", val)

		// Fast forward time in miniredis
		mr.FastForward(2 * time.Second)

		// Key should be expired now
		exists, err = storage.Exists(ctx, "test-key")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("TTL", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		// Set a key with expiration
		err := storage.SetEX(ctx, "test-key", "value", 10*time.Second)
		assert.NoError(t, err)

		// Check TTL
		ttl, err := storage.TTL(ctx, "test-key")
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 10*time.Second)

		// Fast forward time in miniredis
		mr.FastForward(5 * time.Second)

		// Check TTL again
		ttl, err = storage.TTL(ctx, "test-key")
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 5*time.Second)

		// Key with no expiration
		err = client.Set(ctx, "persistent-key", "value", 0).Err()
		assert.NoError(t, err)

		ttl, err = storage.TTL(ctx, "persistent-key")
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl)

		// Non-existent key
		ttl, err = storage.TTL(ctx, "non-existent-key")
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(-2), ttl)
	})
}

// MockLogger implements the LoggerInterface
type MockLogger struct {
	fields map[string]interface{}
	logs   []string
	errors []string
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		fields: make(map[string]interface{}),
		logs:   []string{},
		errors: []string{},
	}
}

func (m *MockLogger) With(field string, value interface{}) LoggerInterface {
	clone := &MockLogger{
		fields: make(map[string]interface{}),
		logs:   m.logs,
		errors: m.errors,
	}

	for k, v := range m.fields {
		clone.fields[k] = v
	}
	clone.fields[field] = value

	return clone
}

func (m *MockLogger) Log(format string, args ...interface{}) {
	m.logs = append(m.logs, format)
}

func (m *MockLogger) Error(format string, args ...interface{}) {
	m.errors = append(m.errors, format)
}

// MockStorage is a mock implementation of the Storage interface for testing
type MockStorage struct {
	existsFunc func(ctx context.Context, key string) (bool, error)
	setExFunc  func(ctx context.Context, key string, value string, expiration time.Duration) error
	ttlFunc    func(ctx context.Context, key string) (time.Duration, error)
}

func (m *MockStorage) Exists(ctx context.Context, key string) (bool, error) {
	return m.existsFunc(ctx, key)
}

func (m *MockStorage) SetEX(ctx context.Context, key string, value string, expiration time.Duration) error {
	return m.setExFunc(ctx, key, value, expiration)
}

func (m *MockStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
	return m.ttlFunc(ctx, key)
}

// TestEntity represents a sample entity for deduplication testing
type TestEntity struct {
	ID   string
	Name string
}

func TestDeduperComprehensive(t *testing.T) {
	logger := NewMockLogger()
	ctx := context.Background()

	// Create a hash handler for TestEntity
	hashHandler := func(ctx context.Context, entity TestEntity) ([]byte, error) {
		return []byte(entity.ID + ":" + entity.Name), nil
	}

	t.Run("NewDeduper with default prefix", func(t *testing.T) {
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return nil
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		// Test that the default prefix is applied correctly
		entity := TestEntity{ID: "123", Name: "Test"}
		hash, err := deduper.Hash(ctx, entity)
		assert.NoError(t, err)

		mockStorage.existsFunc = func(ctx context.Context, key string) (bool, error) {
			// Check that the key has the expected prefix
			assert.True(t, len(key) > len(hash))
			assert.Contains(t, key, "dedup:dedup.TestEntity:")
			return false, nil
		}

		_, err = deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
	})

	t.Run("NewDeduper with custom prefix", func(t *testing.T) {
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return nil
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		customPrefix := "custom:dedup.TestEntity:"
		deduper := NewDeduper(hashHandler, mockStorage, logger,  func() hash.Hash {return sha1.New()}, customPrefix)
		assert.NotNil(t, deduper)

		entity := TestEntity{ID: "123", Name: "Test"}
		hash, err := deduper.Hash(ctx, entity)
		assert.NoError(t, err)

		mockStorage.existsFunc = func(ctx context.Context, key string) (bool, error) {
			// Check that the key has the expected custom prefix
			assert.True(t, len(key) > len(hash))
			assert.True(t, key == customPrefix+hash)
			return false, nil
		}

		_, err = deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
	})

	t.Run("Hash with nil entity", func(t *testing.T) {
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return nil
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		_, err := deduper.Hash(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "entity is nil")
	})

	t.Run("Hash with wrong type", func(t *testing.T) {
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return nil
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		_, err := deduper.Hash(ctx, "not a TestEntity")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "entity is not of type")
	})

	t.Run("IsDuplicate with error from storage", func(t *testing.T) {
		storageError := assert.AnError
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, storageError
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return nil
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, logger,  func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		entity := TestEntity{ID: "123", Name: "Test"}
		_, err := deduper.IsDuplicate(ctx, entity)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage error")
	})

	t.Run("IsDuplicate with storeIfNot and storage error", func(t *testing.T) {
		mockLogger := NewMockLogger()
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return assert.AnError
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, mockLogger,  func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		entity := TestEntity{ID: "123", Name: "Test"}
		isDuplicate, err := deduper.IsDuplicate(ctx, entity, 10*time.Second)
		assert.NoError(t, err)
		assert.False(t, isDuplicate)
	})

	t.Run("Store with error from storage", func(t *testing.T) {
		storageError := assert.AnError
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return storageError
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		entity := TestEntity{ID: "123", Name: "Test"}
		_, _, err := deduper.Store(ctx, entity, 10*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to store")
	})

	t.Run("TTL with different TTL responses", func(t *testing.T) {
		mockStorage := &MockStorage{
			existsFunc: func(ctx context.Context, key string) (bool, error) {
				return false, nil
			},
			setExFunc: func(ctx context.Context, key string, value string, expiration time.Duration) error {
				return nil
			},
			ttlFunc: func(ctx context.Context, key string) (time.Duration, error) {
				return 0, nil
			},
		}

		deduper := NewDeduper(hashHandler, mockStorage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		// Test storage error
		mockStorage.ttlFunc = func(ctx context.Context, key string) (time.Duration, error) {
			return 0, assert.AnError
		}
		_, err := deduper.TTL(ctx, "hash")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage error")

		// Test key does not exist (TTL = 0)
		mockStorage.ttlFunc = func(ctx context.Context, key string) (time.Duration, error) {
			return 0, nil
		}
		_, err = deduper.TTL(ctx, "hash")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")

		// Test key has no expiration (TTL = -1)
		mockStorage.ttlFunc = func(ctx context.Context, key string) (time.Duration, error) {
			return -1, nil
		}
		_, err = deduper.TTL(ctx, "hash")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has no expiration")

		// Test key with valid TTL
		mockStorage.ttlFunc = func(ctx context.Context, key string) (time.Duration, error) {
			return 10 * time.Second, nil
		}
		ttl, err := deduper.TTL(ctx, "hash")
		assert.NoError(t, err)
		assert.Equal(t, 10*time.Second, ttl)
	})
}

// TestDeduperWithRedis tests the Deduper with a real Redis implementation using miniredis
func TestDeduperWithRedis(t *testing.T) {
	// Setup miniredis
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	// Create storage and logger
	ctx := context.Background()
	storage := NewRedisStorage(ctx, client)
	logger := NewMockLogger()

	// Create a hash handler for TestEntity
	hashHandler := func(ctx context.Context, entity TestEntity) ([]byte, error) {
		return []byte(entity.ID + ":" + entity.Name), nil
	}

	t.Run("Full deduplication flow", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		deduper := NewDeduper(hashHandler, storage, logger,  func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		entity := TestEntity{ID: "123", Name: "Test"}

		// 1. Check if entity is a duplicate (should not be)
		isDuplicate, err := deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
		assert.False(t, isDuplicate)

		// 2. Store hash with expiration
		hash, _, err := deduper.Store(ctx, entity, 10*time.Second)
		assert.NoError(t, err)
		assert.NotEmpty(t, hash)

		// 3. Check if entity is now a duplicate (should be)
		isDuplicate, err = deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
		assert.True(t, isDuplicate)

		// 4. Get expiration time
		ttl, err := deduper.TTL(ctx, hash)
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 10*time.Second)

		// 5. Fast forward time
		mr.FastForward(5 * time.Second)

		// 6. Check TTL again
		ttl, err = deduper.TTL(ctx, hash)
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 5*time.Second)

		// 7. Fast forward past expiration
		mr.FastForward(6 * time.Second)

		// 8. Check if entity is still a duplicate (should not be after expiration)
		isDuplicate, err = deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
		assert.False(t, isDuplicate)

		// 9. TTL should return error for expired key
		_, err = deduper.TTL(ctx, hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has no expiration")
	})

	t.Run("IsDuplicate with storeIfNot parameter", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		deduper := NewDeduper(hashHandler, storage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		entity := TestEntity{ID: "123", Name: "Test"}

		// Check and store if not a duplicate
		isDuplicate, err := deduper.IsDuplicate(ctx, entity, 10*time.Second)
		assert.NoError(t, err)
		assert.False(t, isDuplicate)

		// Now it should be a duplicate
		isDuplicate, err = deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
		assert.True(t, isDuplicate)

		// Fast forward past expiration
		mr.FastForward(11 * time.Second)

		// Should not be a duplicate anymore
		isDuplicate, err = deduper.IsDuplicate(ctx, entity)
		assert.NoError(t, err)
		assert.False(t, isDuplicate)
	})

	t.Run("Multiple entities with same properties", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		deduper := NewDeduper(hashHandler, storage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, deduper)

		entity1 := TestEntity{ID: "123", Name: "Test"}
		entity2 := TestEntity{ID: "123", Name: "Test"} // Same properties, different instance

		// Store the first entity
		hash1, _, err := deduper.Store(ctx, entity1, 10*time.Second)
		assert.NoError(t, err)

		// Generate hash for second entity
		hash2, err := deduper.Hash(ctx, entity2)
		assert.NoError(t, err)

		// They should have the same hash
		assert.Equal(t, hash1, hash2)

		// Second entity should be considered a duplicate
		isDuplicate, err := deduper.IsDuplicate(ctx, entity2)
		assert.NoError(t, err)
		assert.True(t, isDuplicate)
	})

	t.Run("Different types with same handler", func(t *testing.T) {
		// Clean up before test
		mr.FlushAll()

		// First entity type
		type UserAction struct {
			UserID    string
			ActionID  string
			Timestamp time.Time
		}

		actionHandler := func(ctx context.Context, action UserAction) ([]byte, error) {
			return []byte(action.UserID + ":" + action.ActionID), nil
		}

		actionDeduper := NewDeduper(actionHandler, storage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, actionDeduper)

		// Second entity type with a compatible signature
		type CampaignAction struct {
			CampaignID string
			ActionID   string
		}

		campaignHandler := func(ctx context.Context, campaign CampaignAction) ([]byte, error) {
			return []byte(campaign.CampaignID + ":" + campaign.ActionID), nil
		}

		campaignDeduper := NewDeduper(campaignHandler, storage, logger, func() hash.Hash {return sha1.New()})
		assert.NotNil(t, campaignDeduper)

		// Create entities with same action ID but different types
		action := UserAction{
			UserID:    "user1",
			ActionID:  "action1",
			Timestamp: time.Now(),
		}

		campaign := CampaignAction{
			CampaignID: "user1", // Intentionally using same value
			ActionID:   "action1", // Intentionally using same value
		}

		// Store action hash
		actionHash, _, err := actionDeduper.Store(ctx, action, 10*time.Second)
		assert.NoError(t, err)

		// Generate campaign hash
		campaignHash, err := campaignDeduper.Hash(ctx, campaign)
		assert.NoError(t, err)

		// Verify same hash values but different keys in Redis due to different prefixes
		assert.Equal(t, actionHash, campaignHash, "Hashes should be the same")

		// Campaign should not be considered a duplicate because it uses a different prefix
		isDuplicate, err := campaignDeduper.IsDuplicate(ctx, campaign)
		assert.NoError(t, err)
		assert.False(t, isDuplicate, "Campaign should not be a duplicate despite having same hash")

		// Store campaign hash
		_, _, err = campaignDeduper.Store(ctx, campaign, 10*time.Second)
		assert.NoError(t, err)

		// Now campaign should be a duplicate
		isDuplicate, err = campaignDeduper.IsDuplicate(ctx, campaign)
		assert.NoError(t, err)
		assert.True(t, isDuplicate, "Campaign should now be a duplicate")
	})
}