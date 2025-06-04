package dedup

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/pixie-sh/logger-go/logger"
	"go.worten.net/digital/packages/marketplace-libs/utils"
	"hash"
	"time"
	"unsafe"

	"github.com/pixie-sh/errors-go"
)

// hashHandler creates a string to be hashed based on the entity
type hashHandler = func(ctx context.Context, entity any) ([]byte, error)
type matchHandler = func(ctx context.Context, inputEntity any, storageEntity any) (bool, error)
type serializeHandler = func(ctx context.Context, inputEntity any) (string, error)

// Storage defines the interface for storage operations required by Deduper
type Storage interface {
	Exists(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string) (any, error)
	SetEX(ctx context.Context, key string, value string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
}

// Deduper provides deduplication functionality using SHA1 hashes
type Deduper struct {
	handler    hashHandler
	storage    Storage
	prefix     string
	logger     logger.Interface
	hasher     func() hash.Hash
	matcher    matchHandler
	serializer serializeHandler
}

// NewDeduper creates a new Deduper instance
func NewDeduper[T any](
	handler func(ctx context.Context, t T) ([]byte, error),
	storage Storage,
	logger logger.Interface,
	hasher func() hash.Hash,
	matcher matchHandler,
	serializer serializeHandler,
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
		storage:    storage,
		prefix:     prefix,
		logger:     logger,
		hasher:     hasher,
		matcher:    matcher,
		serializer: serializer,
	}
}

// Hash calculates the SHA1 hash for the given entity
func (d *Deduper) Hash(ctx context.Context, entity any) (string, error) {
	input, err := d.handler(ctx, entity)
	if err != nil {
		return "", errors.Wrap(err, "failed to create hash input", DedupInvalidHashErrorCode)
	}

	h := d.hasher()
	if h == nil {
		return *(*string)(unsafe.Pointer(&input)), nil
	}

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

// IsValueDuplicate checks if the entity hash+value are a duplicates by checking if its key hash exists in storage
// if storeIfNot is provided that value is used as expiration for Store calling
func (d *Deduper) IsValueDuplicate(ctx context.Context, entity any, storeIfNot ...time.Duration) (bool, error) {
	dedupHash, err := d.Hash(ctx, entity)
	if err != nil {
		return false, err
	}

	key := d.prefix + dedupHash
	exists, err := d.storage.Get(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "storage error; %s", err.Error(), DedupStorageErrorCode)
	}

	if utils.IsEmpty(exists) && len(storeIfNot) > 0 {
		_, _, err = d.store(ctx, dedupHash, entity, storeIfNot[0])
		if err != nil {
			d.logger.With("error", err).Error("failed to store dedupHash at IsDuplicate; %s", err.Error())
		}

		return false, nil
	}

	match, err := d.matcher(ctx, entity, exists)
	if err != nil {
		return false, errors.Wrap(err, "failed to match Value at IsDuplicate; %s", err.Error())
	}

	if !match && (len(storeIfNot) < 2 || storeIfNot[1] == 1) {
		_, _, err = d.store(ctx, dedupHash, entity, storeIfNot[0])
		if err != nil {
			d.logger.With("error", err).Error("failed to update dedupHash at IsDuplicate; %s", err.Error())
			return match, err
		}
	}

	return match, nil
}

// Store stores the hash with the given expiration time
// returns originated hash and stored key, respectively
func (d *Deduper) Store(ctx context.Context, entity any, expiration time.Duration) (string, string, error) {
	dedupHash, err := d.Hash(ctx, entity)
	if err != nil {
		return "", "", err
	}

	return d.store(ctx, dedupHash, entity, expiration)
}

func (d *Deduper) store(ctx context.Context, dedupHash string, entity any, expiration time.Duration) (string, string, error) {
	key := d.prefix + dedupHash
	ser, err := d.serializer(ctx, entity)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to serialize entity", DedupStorageErrorCode)
	}

	err = d.storage.SetEX(ctx, key, ser, expiration)
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
		return "", errors.Wrap(err, "failed to store dedupHash; %s", err.Error(), DedupStorageErrorCode)
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
