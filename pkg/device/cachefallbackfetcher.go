package device

import (
	"context"
	"fmt"
	"io"
	"time" // Import time for context.WithTimeout

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

// CachedFetcher is a new interface that defines the contract for any caching layer
// for device metadata. This allows the caching implementation (e.g., Redis, in-memory)
// to be swapped out.
type CachedFetcher interface {
	// FetchFromCache attempts to retrieve data from the cache using the provided context.
	// It returns the data if found (cache hit), or ErrCacheMiss if not found.
	// Any other error indicates a problem with the cache itself.
	FetchFromCache(ctx context.Context, deviceEUI string) (clientID, locationID, category string, err error) // Context added
	// WriteToCache writes data to the cache using the provided context.
	WriteToCache(ctx context.Context, deviceEUI, clientID, locationID, category string) error // Context added
	io.Closer                                                                                 // The cache fetcher must also be closable to release resources.
}

// NewCacheFallbackFetcher creates a `DeviceMetadataFetcher` that implements a cache-then-fallback
// pattern. It uses a provided `CachedFetcher` as a fast cache and falls back to the provided
// `sourceFetcher` as the source of truth.
//
// The logic is as follows:
// 1. An incoming fetch request first checks the `cacheFetcher`.
// 2. If the data is found in `cacheFetcher` (cache hit), it is returned immediately.
// 3. If the data is not in `cacheFetcher` (cache miss), the request is passed to the `sourceFetcher`.
// 4. If the source finds the data, that data is returned to the caller.
// 5. In the background, the data from the source is also written to the `cacheFetcher` to speed up subsequent requests.
//
// **IMPORTANT USAGE NOTE:**
// This function returns a `fetcher` and a `cleanup` function. Because the underlying
// components hold open database connections, the returned `cleanup` function MUST be called
// (e.g., using `defer cleanup()`) when the fetcher is no longer needed to prevent resource leaks.
func NewCacheFallbackFetcher(
	parentCtx context.Context, // Context for overall operations. This is the top-level context for the fetcher.
	cacheFetcher CachedFetcher, // Accepts any implementation of the CachedFetcher interface.
	sourceFetcher SourceFetcher, // Accepts any implementation of the SourceFetcher interface.
	logger zerolog.Logger,
) (fetcher DeviceMetadataFetcher, cleanup func() error, err error) {

	if cacheFetcher == nil {
		return nil, nil, fmt.Errorf("cacheFetcher cannot be nil")
	}
	if sourceFetcher == nil {
		return nil, nil, fmt.Errorf("sourceFetcher cannot be nil")
	}

	// The chained fetcher's core logic now resides here.
	// It will attempt to fetch from cache, and if not found, fallback to source and write-back.
	chainedFetchLogic := func(deviceEUI string) (clientID, locationID, category string, err error) {
		// 1. Try to fetch from cache using the *request-scoped* context (parentCtx for this call)
		clientID, locationID, category, err = cacheFetcher.FetchFromCache(parentCtx, deviceEUI) // Pass parentCtx
		if err == nil {
			logger.Debug().Str("device_eui", deviceEUI).Msg("Cache hit.")
			return clientID, locationID, category, nil // Cache hit
		}
		if !IsCacheMiss(err) { // If it's an error other than ErrCacheMiss, propagate it
			logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Error fetching from cache.")
			return "", "", "", fmt.Errorf("error fetching from cache: %w", err)
		}
		logger.Debug().Str("device_eui", deviceEUI).Msg("Cache miss. Falling back to source.")

		// 2. Cache miss, fallback to source
		clientID, locationID, category, err = sourceFetcher.Fetch(deviceEUI)
		if err != nil {
			logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Error fetching from source.")
			return "", "", "", fmt.Errorf("error fetching from source: %w", err)
		}
		logger.Debug().Str("device_eui", deviceEUI).Msg("Source hit. Writing back to cache.")

		// 3. Source hit, write back to cache (in a goroutine to not block the response)
		// Use a derived context from parentCtx for the background write operation.
		go func(goCtx context.Context, eui, cID, lID, cat string) { // Pass goCtx (derived from parentCtx)
			writeCtx, writeCancel := context.WithTimeout(goCtx, 5*time.Second) // Use goCtx
			defer writeCancel()
			if writeErr := cacheFetcher.WriteToCache(writeCtx, eui, cID, lID, cat); writeErr != nil { // Pass writeCtx
				logger.Error().Err(writeErr).Str("device_eui", eui).Msg("Failed to write to cache in background.")
			} else {
				logger.Debug().Str("device_eui", eui).Msg("Data written to cache in background.")
			}
		}(parentCtx, deviceEUI, clientID, locationID, category) // Pass the original parentCtx to the goroutine

		return clientID, locationID, category, nil
	}

	// Define the cleanup function that closes all underlying resources.
	cleanupFunc := func() error {
		log := logger.With().Str("component", "ChainedFetcherCleanup").Logger()
		log.Info().Msg("Closing chained fetcher resources...")

		var firstErr error
		// Close the cache fetcher.
		if err := cacheFetcher.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing cache fetcher")
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

	// The returned fetcher is now the `chainedFetchLogic` function.
	return chainedFetchLogic, cleanupFunc, nil
}

// ErrCacheMiss is an error specifically indicating that the item was not found in the cache.
type ErrCacheMiss struct {
	EUI string
}

func (e ErrCacheMiss) Error() string {
	return fmt.Sprintf("device metadata not found in cache for EUI: %s", e.EUI)
}

// IsCacheMiss checks if a given error is an ErrCacheMiss.
func IsCacheMiss(err error) bool {
	_, ok := err.(ErrCacheMiss)
	return ok
}
