package dedup

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash"
	"time"

	"github.com/pixie-sh/errors-go"
)

// hashHandler creates a string to be hashed based on the entity
type hashHandler = func(ctx context.Context, entity any) ([]byte, error)

// Storage defines the interface for storage operations required by Deduper
type Storage interface {
	Exists(ctx context.Context, key string) (bool, error)
	SetEX(ctx context.Context, key string, value string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
}

// LoggerInterface represents the basic logging interface.
type LoggerInterface interface {
	With(field string, value any) LoggerInterface
	Log(format string, args ...any)
	Error(format string, args ...any)
}

// Deduper provides deduplication functionality using SHA1 hashes
type Deduper struct {
	handler hashHandler
	storage Storage
	prefix  string
	logger LoggerInterface
	hasher func() hash.Hash
}

// NewDeduper creates a new Deduper instance
func NewDeduper[T any](
	handler func(ctx context.Context, t T) ([]byte, error),
	storage Storage,
	logger LoggerInterface,
	hasher func() hash.Hash,
	customPrefix ...string,
) *Deduper {
	prefix := fmt.Sprintf("dedup:%s:", nameOf[T]())
	if len(customPrefix) > 0 && customPrefix[0] != "" {
		prefix = customPrefix[0]
	}

	return &Deduper{
		handler: func(ctx context.Context, entity any) ([]byte, error) {
			if entity == nil {
				return nil, errors.New("entity is nil", DedupEntityNilErrorCode)
			}

			_, ok := entity.(T)
			if !ok {
				return nil, errors.New("entity is not of type '%s'", nameOf[T](), DedupEntityTypeMismatchErrorCode)
			}

			return handler(ctx, entity.(T))
		},
		storage: storage,
		prefix:  prefix,
		logger: logger,
		hasher: hasher,
	}
}

// Hash calculates the SHA1 hash for the given entity
func (d *Deduper) Hash(ctx context.Context, entity any) (string, error) {
	input, err := d.handler(ctx, entity)
	if err != nil {
		return "", errors.Wrap(err, "failed to create hash input", DedupInvalidHashErrorCode)
	}

	h := d.hasher()
	h.Write(input)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// IsDuplicate checks if the entity is a duplicate by checking if its hash exists in storage
// if storeIfNot is provided that value is used as expiration for Store calling
func (d *Deduper) IsDuplicate(ctx context.Context, entity any, storeIfNot ...time.Duration) (bool, error) {
	hash, err := d.Hash(ctx, entity)
	if err != nil {
		return false, err
	}

	key := d.prefix + hash
	exists, err := d.storage.Exists(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "storage error; %s", err.Error(), DedupStorageErrorCode)
	}


	if !exists && len(storeIfNot) > 0 {
		_, _, err = d.Store(ctx, entity, storeIfNot[0])
		if err != nil {
			d.logger.With("error", err).Error("failed to store hash at IsDuplicate; %s", err.Error())
		}
	}

	return exists, nil
}

// Store stores the hash with the given expiration time
// returns originated hash and stored key, respectively
func (d *Deduper) Store(ctx context.Context, entity any, expiration time.Duration) (string, string, error) {
	dedupHash, err := d.Hash(ctx, entity)
	if err != nil {
		return "", "", err
	}

	key := d.prefix + dedupHash
	err = d.storage.SetEX(ctx, key, "1", expiration)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to store dedupHash; %s", err.Error(), DedupStorageErrorCode)
	}

	return dedupHash, key, nil
}

// StoreHash stores the hash with the given expiration time
// returns the stored key
func (d *Deduper) StoreHash(ctx context.Context, hash string, expiration time.Duration) (string, error) {
	key := d.prefix + hash
	err := d.storage.SetEX(ctx, key, "1", expiration)
	if err != nil {
		return "", errors.Wrap(err,"failed to store dedupHash; %s", err.Error(), DedupStorageErrorCode)
	}

	return key, nil
}

// TTL returns the time left until the hash expires
func (d *Deduper) TTL(ctx context.Context, hash string) (time.Duration, error) {
	key := d.prefix + hash
	ttl, err := d.storage.TTL(ctx, key)
	if err != nil {
		return 0, errors.Wrap(err, "storage error for key '%s'; %s", key, err.Error(), DedupStorageErrorCode)
	}

	if ttl == 0 {
		return 0, errors.New("key '%s' does not exist", key, DedupMissingKeyErrorCode)
	}

	if ttl < 0 {
		return 0, errors.New("key '%s' has no expiration", key, DedupNoExpeirationKeyErrorCode)
	}

	return ttl, nil
}
