package dedup

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/pixie-sh/errors-go"
	"github.com/pixie-sh/logger-go/logger"
	"hash"
	"time"
)

type HashMode int

const (
	AutoSmart HashMode = iota
	AlwaysHash
	NeverHash
)

type HashStrategy struct {
	KeyHashMode   HashMode
	ValueHashMode HashMode
	KeyThreshold  int
	ValThreshold  int
}

func DefaultHashStrategy() HashStrategy {
	return HashStrategy{
		KeyHashMode:   AutoSmart,
		ValueHashMode: AutoSmart,
		KeyThreshold:  24,
		ValThreshold:  256,
	}
}

// hashHandler creates a string to be hashed based on the entity
type hashHandler = func(ctx context.Context, entity any) ([]byte, error)
type matchHandler = func(ctx context.Context, inputEntity any, storageEntity any) (bool, error)
type serializeHandler = func(ctx context.Context, inputEntity any) (string, error)

// Storage defines the interface for storage operations required by Deduper
type Storage interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	TTL(ctx context.Context, key []byte) (time.Duration, error)
	SetEX(ctx context.Context, key []byte, value []byte, expiration ...time.Duration) error
	Exists(ctx context.Context, key []byte) (bool, error)
}

type Deduper struct {
	handler    hashHandler
	storage    Storage
	prefix     []byte
	logger     logger.Interface
	hasher     func() hash.Hash
	matcher    matchHandler
	serializer serializeHandler
}

func NewDeduper[T any](
	handler func(ctx context.Context, t T) ([]byte, error),
	storage Storage,
	logger logger.Interface,
	hasher func() hash.Hash,
	matcher matchHandler,
	serializer serializeHandler,
	customPrefix ...string,
) *Deduper {
	prefix := []byte(fmt.Sprintf("dedup:%s:", nameOf[T]()))
	if len(customPrefix) > 0 && customPrefix[0] != "" {
		prefix = []byte(customPrefix[0])
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

func (d *Deduper) Hash(ctx context.Context, entity any, strategy HashStrategy, isValue bool) ([]byte, error) {
	input, err := d.handler(ctx, entity)
	if err != nil {
		return nil, err
	}

	mode := strategy.KeyHashMode
	threshold := strategy.KeyThreshold
	if isValue {
		mode = strategy.ValueHashMode
		threshold = strategy.ValThreshold
	}

	if mode == NeverHash || (mode == AutoSmart && len(input) <= threshold) {
		return input, nil
	}

	h := d.hasher()
	h.Write(input)
	return h.Sum(nil), nil
}

func (d *Deduper) buildKey(hash []byte) []byte {
	return append(d.prefix, hash...)
}

func (d *Deduper) IsDuplicate(ctx context.Context, entity any, strategy HashStrategy, storeIfNot ...time.Duration) (bool, error) {
	dedupHash, err := d.Hash(ctx, entity, strategy, false)
	if err != nil {
		return false, err
	}
	key := d.buildKey(dedupHash)

	exists, err := d.storage.Exists(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "storage error; %s", err.Error(), DedupStorageErrorCode)
	}

	if !exists && len(storeIfNot) > 0 {
		_, _, err = d.Store(ctx, entity, strategy, storeIfNot[0])
		if err != nil {
			d.logger.With("error", err).Error("failed to store hash at IsDuplicate; %s", err.Error())
		}
	}

	return exists, nil
}

func (d *Deduper) IsValueDuplicate(ctx context.Context, entity any, strategy HashStrategy, storeIfNot ...time.Duration) (bool, error) {
	dedupHash, err := d.Hash(ctx, entity, strategy, false)
	if err != nil {
		return false, err
	}
	key := d.buildKey(dedupHash)

	existing, err := d.storage.Get(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "storage error; %s", err.Error(), DedupStorageErrorCode)
	}

	if IsEmpty(existing) && len(storeIfNot) > 0 {
		_, _, err = d.store(ctx, dedupHash, entity, strategy, storeIfNot[0])
		if err != nil {
			d.logger.With("error", err).Error("failed to store dedupHash at IsDuplicate; %s", err.Error())
		}
		return false, nil
	}

	match, err := d.matcher(ctx, entity, existing)
	if err != nil {
		return false, errors.Wrap(err, "failed to match Value at IsDuplicate; %s", err.Error())
	}

	if !match && (len(storeIfNot) < 2 || storeIfNot[1] == 1) {
		_, _, err = d.store(ctx, dedupHash, entity, strategy, storeIfNot[0])
		if err != nil {
			d.logger.With("error", err).Error("failed to update dedupHash at IsDuplicate; %s", err.Error())
			return match, err
		}
	}

	return match, nil
}

func (d *Deduper) Store(ctx context.Context, entity any, strategy HashStrategy, expiration time.Duration) ([]byte, []byte, error) {
	dedupHash, err := d.Hash(ctx, entity, strategy, false)
	if err != nil {
		return nil, nil, err
	}
	return d.store(ctx, dedupHash, entity, strategy, expiration)
}

func (d *Deduper) store(ctx context.Context, dedupHash []byte, entity any, strategy HashStrategy, expiration time.Duration) ([]byte, []byte, error) {
	key := d.buildKey(dedupHash)
	serStr, err := d.serializer(ctx, entity)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to serialize entity", DedupStorageErrorCode)
	}
	ser := []byte(serStr)

	if strategy.ValueHashMode != NeverHash && (strategy.ValueHashMode == AlwaysHash || len(ser) > strategy.ValThreshold) {
		h := d.hasher()
		h.Write(ser)
		ser = []byte(hex.EncodeToString(h.Sum(nil)))
	}

	err = d.storage.SetEX(ctx, key, ser, expiration)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to store dedupHash; %s", err.Error(), DedupStorageErrorCode)
	}

	return dedupHash, key, nil
}

func (d *Deduper) StoreHash(ctx context.Context, hash []byte, expiration time.Duration) ([]byte, error) {
	key := d.buildKey(hash)
	err := d.storage.SetEX(ctx, key, []byte("1"), expiration)
	if err != nil {
		return nil, errors.Wrap(err, "failed to store dedupHash; %s", err.Error(), DedupStorageErrorCode)
	}
	return key, nil
}

func (d *Deduper) TTL(ctx context.Context, hash []byte) (time.Duration, error) {
	key := d.buildKey(hash)
	ttl, err := d.storage.TTL(ctx, key)
	if err != nil {
		return 0, errors.Wrap(err, "storage error for key; %s", err.Error(), DedupStorageErrorCode)
	}
	if ttl == 0 {
		return 0, errors.New("key does not exist", DedupMissingKeyErrorCode)
	}
	if ttl < 0 {
		return 0, errors.New("key has no expiration", DedupNoExpeirationKeyErrorCode)
	}
	return ttl, nil
}
