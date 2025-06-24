package device

import (
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog"
)

// SourceFetcher is an interface that defines the contract for a source of truth
// for device metadata. This allows the caching layer to be decoupled from a specific
// backend like Firestore. Any data source that can fetch device metadata and be
// closed should implement this interface.
type SourceFetcher interface {
	Fetch(deviceEUI string) (clientID, locationID, category string, err error)
	io.Closer
}

// NewChainedFetcher creates a `DeviceMetadataFetcher` that implements a cache-then-fallback
// pattern. It uses Redis as a fast cache and falls back to the provided `sourceFetcher`
// as the source of truth. This function is now generic and not tied to Firestore.
//
// The logic is as follows:
// 1. An incoming fetch request first checks the Redis cache.
// 2. If the data is found in Redis (cache hit), it is returned immediately.
// 3. If the data is not in Redis (cache miss), the request is passed to the `sourceFetcher`.
// 4. If the source finds the data, that data is returned to the caller.
// 5. In the background, the data from the source is also written to the Redis cache to speed up subsequent requests.
//
// **IMPORTANT USAGE NOTE:**
// This function returns a `fetcher` and a `cleanup` function. Because the underlying
// components hold open database connections, the returned `cleanup` function MUST be called
// (e.g., using `defer cleanup()`) when the fetcher is no longer needed to prevent resource leaks.
func NewChainedFetcher(
	ctx context.Context,
	redisConfig *RedisConfig,
	sourceFetcher SourceFetcher, // Accepts any implementation of the SourceFetcher interface.
	logger zerolog.Logger,
) (fetcher DeviceMetadataFetcher, cleanup func() error, err error) {

	if sourceFetcher == nil {
		return nil, nil, fmt.Errorf("sourceFetcher cannot be nil")
	}

	// 1. Create the caching fetcher (Redis) and pass the source's `Fetch` method as the fallback.
	redisCachingFetcher, err := NewRedisDeviceMetadataFetcher(ctx, redisConfig, sourceFetcher.Fetch, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create redis caching fetcher: %w", err)
	}

	// 2. Define the cleanup function that closes all underlying resources.
	// This pattern is used because we are returning a function type (`DeviceMetadataFetcher`)
	// which cannot have its own Close method.
	cleanupFunc := func() error {
		log := logger.With().Str("component", "ChainedFetcherCleanup").Logger()
		log.Info().Msg("Closing chained fetcher resources...")

		var firstErr error
		// The Redis fetcher is responsible for closing the Redis client it creates.
		if err := redisCachingFetcher.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing redis fetcher client")
			firstErr = err // Capture the first error
		}

		// Close the source fetcher that was passed in.
		if err := sourceFetcher.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing source fetcher")
			if firstErr == nil {
				firstErr = err
			}
		}

		log.Info().Msg("Chained fetcher cleanup complete.")
		return firstErr // Return the first error encountered, if any.
	}

	// 3. The `Fetch` method of the redisCachingFetcher now implements the full caching logic.
	// This method conforms to the `DeviceMetadataFetcher` function signature.
	return redisCachingFetcher.Fetch, cleanupFunc, nil
}
