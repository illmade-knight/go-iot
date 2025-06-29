package enrichment_test // Changed package to enrichment_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/illmade-knight/go-iot/pkg/device" // Import device package
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"

	"github.com/rs/zerolog"
)

// --- Corrected Struct Definitions for Test ---
// The original `EnrichedMessage` embeds `types.ConsumedMessage`, which contains
// non-serializable func() fields (Ack/Nack), causing the JSON error.
// The fix is to define a new struct for the payload that only contains data.

// TestEnrichedMessage is a data-only representation of the final message payload.
// This is the struct that will be serialized to JSON and published.
type TestEnrichedMessage struct {
	OriginalPayload json.RawMessage   `json:"originalPayload"`
	OriginalInfo    *types.DeviceInfo `json:"originalInfo"`
	PublishTime     time.Time         `json:"publishTime"`
	ClientID        string            `json:"clientID,omitempty"`
	LocationID      string            `json:"locationID,omitempty"`
	Category        string            `json:"category,omitempty"`
}

// NewTestMessageEnricher creates the MessageTransformer for the test.
// It correctly separates the data payload from the message's Ack/Nack functions.
func NewTestMessageEnricher(fetcher device.DeviceMetadataFetcher, logger zerolog.Logger) messagepipeline.MessageTransformer[TestEnrichedMessage] {
	return func(msg types.ConsumedMessage) (*TestEnrichedMessage, bool, error) {
		// Start with the basic, un-enriched data.
		payload := &TestEnrichedMessage{
			OriginalPayload: msg.Payload,
			OriginalInfo:    msg.DeviceInfo,
			PublishTime:     msg.PublishTime,
		}

		// If there's no device info, we can't enrich, but we still pass the message on.
		if msg.DeviceInfo == nil || msg.DeviceInfo.UID == "" {
			logger.Warn().Str("msg_id", msg.ID).Msg("Message has no device UID for enrichment, skipping lookup.")
			return payload, false, nil
		}

		// Attempt to fetch metadata for enrichment.
		deviceEUI := msg.DeviceInfo.UID
		clientID, locationID, category, err := fetcher(deviceEUI)
		if err != nil {
			logger.Error().Err(err).Str("device_eui", deviceEUI).Str("msg_id", msg.ID).Msg("Failed to fetch device metadata for enrichment.")
			// We NACK by returning an error, which stops this message from being published.
			return nil, false, fmt.Errorf("failed to enrich message for EUI %s: %w", deviceEUI, err)
		}

		// If successful, add the enriched data to the payload.
		payload.ClientID = clientID
		payload.LocationID = locationID
		payload.Category = category

		logger.Debug().Str("msg_id", msg.ID).Str("device_eui", deviceEUI).Msg("Message enriched with device metadata.")
		return payload, false, nil
	}
}

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
