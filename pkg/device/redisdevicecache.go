package device

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
)

// RedisConfig holds configuration for the Redis client.
type RedisConfig struct {
	Addr     string        // e.g., "localhost:6379"
	Password string        // Leave empty if no password
	DB       int           // e.g., 0
	CacheTTL time.Duration // Time-to-live for cache entries, e.g., 5 * time.Minute
}

// RedisDeviceMetadataFetcher implements CachedFetcher using a Redis cache.
type RedisDeviceMetadataFetcher struct {
	redisClient *redis.Client
	logger      zerolog.Logger
	ttl         time.Duration
	// Removed the stored 'ctx context.Context' as methods will now receive it per call.
}

// deviceMetadataCache a helper struct for JSON serialization in Redis.
// This ensures that the data stored in Redis has a consistent structure.
type deviceMetadataCache struct {
	ClientID   string `json:"clientID"`
	LocationID string `json:"locationID"`
	Category   string `json:"category"`
}

// NewRedisDeviceMetadataFetcher creates a new Redis-based CachedFetcher.
// It establishes a connection to Redis.
func NewRedisDeviceMetadataFetcher(
	ctx context.Context, // This context is for the initial Ping, not stored for later use.
	cfg *RedisConfig,
	logger zerolog.Logger,
) (*RedisDeviceMetadataFetcher, error) {
	// Create a new Redis client.
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Ping the Redis server to ensure a connection is established.
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close() // Close client if ping fails
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info().Str("redis_address", cfg.Addr).Msg("Successfully connected to Redis for device cache")

	return &RedisDeviceMetadataFetcher{
		redisClient: rdb,
		logger:      logger.With().Str("component", "RedisCacheFetcher").Logger(), // Add component
		ttl:         cfg.CacheTTL,
		// Removed ctx storage
	}, nil
}

// FetchFromCache implements the CachedFetcher interface.
func (f *RedisDeviceMetadataFetcher) FetchFromCache(ctx context.Context, deviceEUI string) (clientID, locationID, category string, err error) { // Added ctx
	cachedData, err := f.redisClient.Get(ctx, deviceEUI).Result() // Use provided ctx
	if err == nil {
		f.logger.Debug().Str("device_eui", deviceEUI).Msg("Redis cache hit: Found device metadata")
		var metadata deviceMetadataCache
		if jsonErr := json.Unmarshal([]byte(cachedData), &metadata); jsonErr != nil {
			f.logger.Error().Err(jsonErr).Str("device_eui", deviceEUI).Msg("Failed to unmarshal cached device data from Redis (treating as miss)")
			// Treat as a cache miss if data is corrupted, so source can be queried.
			return "", "", "", ErrCacheMiss{EUI: deviceEUI}
		} else {
			return metadata.ClientID, metadata.LocationID, metadata.Category, nil
		}
	}

	if errors.Is(err, redis.Nil) {
		f.logger.Debug().Str("device_eui", deviceEUI).Msg("Redis cache miss: Device metadata not found")
		return "", "", "", ErrCacheMiss{EUI: deviceEUI} // Explicitly return ErrCacheMiss
	}

	// An actual error occurred with Redis, not just a cache miss.
	f.logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Error fetching from Redis cache")
	return "", "", "", fmt.Errorf("redis fetch error: %w", err)
}

// WriteToCache implements the CachedFetcher interface.
func (f *RedisDeviceMetadataFetcher) WriteToCache(ctx context.Context, deviceEUI, clientID, locationID, category string) error { // Added ctx
	metadataToCache := deviceMetadataCache{
		ClientID:   clientID,
		LocationID: locationID,
		Category:   category,
	}
	jsonData, jsonErr := json.Marshal(metadataToCache)
	if jsonErr != nil {
		f.logger.Error().Err(jsonErr).Str("device_eui", deviceEUI).Msg("Failed to marshal device data for Redis caching")
		return fmt.Errorf("failed to marshal for redis: %w", jsonErr)
	}

	if setErr := f.redisClient.Set(ctx, deviceEUI, jsonData, f.ttl).Err(); setErr != nil { // Use provided ctx
		f.logger.Error().Err(setErr).Str("device_eui", deviceEUI).Msg("Failed to set device metadata in Redis cache")
		return fmt.Errorf("failed to set in redis: %w", setErr)
	}
	f.logger.Debug().Str("device_eui", deviceEUI).Msg("Successfully stored device metadata in Redis cache")
	return nil
}

// Close gracefully closes the Redis client connection.
func (f *RedisDeviceMetadataFetcher) Close() error {
	if f.redisClient != nil {
		f.logger.Info().Msg("Closing Redis client connection...")
		return f.redisClient.Close()
	}
	return nil
}
