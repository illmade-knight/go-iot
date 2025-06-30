package enrichment_test // Changed package to enrichment_test

import (
	"fmt"
	"github.com/illmade-knight/go-iot/pkg/device" // Import device package
	"sync"
)

// --- Mock Device Metadata Cache ---

// InMemoryDeviceMetadataCache provides a mock implementation of the device.DeviceMetadataFetcher
// for testing purposes. It uses a simple map to store device metadata.
type InMemoryDeviceMetadataCache struct {
	mu       sync.RWMutex
	metadata map[string]struct {
		ClientID   string
		LocationID string
		Category   string
	}
}

// NewInMemoryDeviceMetadataCache creates a new, empty in-memory cache.
func NewInMemoryDeviceMetadataCache() *InMemoryDeviceMetadataCache {
	return &InMemoryDeviceMetadataCache{
		metadata: make(map[string]struct {
			ClientID   string
			LocationID string
			Category   string
		}),
	}
}

// AddDevice populates the cache with metadata for a given device EUI.
func (c *InMemoryDeviceMetadataCache) AddDevice(eui, clientID, locationID, category string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metadata[eui] = struct {
		ClientID   string
		LocationID string
		Category   string
	}{
		ClientID:   clientID,
		LocationID: locationID,
		Category:   category,
	}
}

// Fetcher returns a function that conforms to the device.DeviceMetadataFetcher interface.
// This is the function that will be passed to the NewMessageEnricher.
func (c *InMemoryDeviceMetadataCache) Fetcher() device.DeviceMetadataFetcher {
	return func(deviceEUI string) (clientID, locationID, category string, err error) {
		c.mu.RLock()
		defer c.mu.RUnlock()

		data, ok := c.metadata[deviceEUI]
		if !ok {
			// Return the specific error type that the main service expects.
			return "", "", "", fmt.Errorf("%w: EUI %s not in cache", device.ErrMetadataNotFound, deviceEUI)
		}
		return data.ClientID, data.LocationID, data.Category, nil
	}
}
