package device

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// InMemoryCacheFetcher implements the CachedFetcher interface using a simple in-memory map.
// It's primarily for testing or scenarios where a persistent cache isn't required.
type InMemoryCacheFetcher struct {
	cache  map[string]deviceMetadataCache // Stores cached data
	mu     sync.RWMutex                   // Protects access to the map
	ttl    time.Duration                  // Time-to-live for cache entries (not actively enforced with cleanup goroutine here, but useful for logic)
	logger zerolog.Logger
	// For simplicity, this in-memory cache does not have an active cleanup goroutine for TTL.
	// Entries will simply expire logically based on ttl, but won't be removed from the map.
}

// NewInMemoryCacheFetcher creates a new InMemoryCacheFetcher.
// It accepts a context, logger, and a TTL for cache entries.
func NewInMemoryCacheFetcher(ctx context.Context, ttl time.Duration, logger zerolog.Logger) *InMemoryCacheFetcher {
	return &InMemoryCacheFetcher{
		cache:  make(map[string]deviceMetadataCache),
		ttl:    ttl,
		logger: logger.With().Str("component", "InMemoryCacheFetcher").Logger(),
	}
}

// FetchFromCache implements the CachedFetcher interface.
func (i *InMemoryCacheFetcher) FetchFromCache(ctx context.Context, deviceEUI string) (clientID, locationID, category string, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Check if context is cancelled before proceeding
	select {
	case <-ctx.Done():
		return "", "", "", ctx.Err()
	default:
		// Continue
	}

	data, found := i.cache[deviceEUI]
	if !found {
		i.logger.Debug().Str("device_eui", deviceEUI).Msg("In-memory cache miss.")
		return "", "", "", ErrCacheMiss{EUI: deviceEUI}
	}

	// In a real in-memory cache, you'd check data.ExpiryTime here as well.
	i.logger.Debug().Str("device_eui", deviceEUI).Msg("In-memory cache hit.")
	return data.ClientID, data.LocationID, data.Category, nil
}

// WriteToCache implements the CachedFetcher interface.
func (i *InMemoryCacheFetcher) WriteToCache(ctx context.Context, deviceEUI, clientID, locationID, category string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Check if context is cancelled before proceeding
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Continue
	}

	i.cache[deviceEUI] = deviceMetadataCache{
		ClientID:   clientID,
		LocationID: locationID,
		Category:   category,
		// ExpiryTime: time.Now().Add(i.ttl), // Set expiry for explicit TTL management
	}
	i.logger.Debug().Str("device_eui", deviceEUI).Msg("Data written to in-memory cache.")
	return nil
}

// Close implements the io.Closer interface for cleanup.
func (i *InMemoryCacheFetcher) Close() error {
	// For a simple map, no explicit close needed, but good to implement the interface.
	i.logger.Info().Msg("In-memory cache fetcher closed.")
	return nil
}
