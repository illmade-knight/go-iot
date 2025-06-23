package enrichment

import (
	"fmt"

	"github.com/illmade-knight/go-iot/pkg/device"          // Changed import path
	"github.com/illmade-knight/go-iot/pkg/messagepipeline" // No change
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
)

// EnrichedMessage represents the structure of a message after enrichment.
// It embeds the original ConsumedMessage and adds enrichment-specific fields.
type EnrichedMessage struct {
	types.ConsumedMessage
	// Device metadata fields
	ClientID   string `json:"clientID"`
	LocationID string `json:"locationID"`
	Category   string `json:"category"`
	// Add other enriched data fields as needed, e.g., product info, user info
	// linkedData map[string]interface{} `json:"linkedData,omitempty"` // For generic linked data
}

// DeviceMetadataFetcher is an interface for fetching device metadata.
// This allows different implementations (Firestore, Redis, etc.) to be used interchangeably.
// Note: This is now just an alias for device.DeviceMetadataFetcher.
type DeviceMetadataFetcher = device.DeviceMetadataFetcher

// NewMessageEnricher creates the MessageTransformer function for enrichment.
// It returns a function that conforms to the messagepipeline.MessageTransformer signature.
func NewMessageEnricher(fetcher DeviceMetadataFetcher, logger zerolog.Logger) messagepipeline.MessageTransformer[EnrichedMessage] {
	return func(msg types.ConsumedMessage) (transformedPayload EnrichedMessage, skip bool, err error) {
		if msg.DeviceInfo == nil || msg.DeviceInfo.UID == "" {
			logger.Warn().Str("msg_id", msg.ID).Msg("Message has no device UID for enrichment, skipping lookup.")
			return EnrichedMessage{ConsumedMessage: msg}, false, nil // Pass original message, don't skip pipeline
		}

		deviceEUI := msg.DeviceInfo.UID // Assuming UID is the deviceEUI
		clientID, locationID, category, err := fetcher(deviceEUI)
		if err != nil {
			logger.Error().Err(err).Str("device_eui", deviceEUI).Str("msg_id", msg.ID).Msg("Failed to fetch device metadata for enrichment.")
			// If device.ErrMetadataNotFound, you might choose to ACK and skip (false, true)
			// or NACK (false, error). Nacking means retrying.
			// For this example, let's Nack if enrichment fails.
			return EnrichedMessage{}, false, fmt.Errorf("failed to enrich message for EUI %s: %w", deviceEUI, err)
		}

		enrichedMsg := EnrichedMessage{
			ConsumedMessage: msg,
			ClientID:        clientID,
			LocationID:      locationID,
			Category:        category,
		}
		logger.Debug().Str("msg_id", msg.ID).Str("device_eui", deviceEUI).Msg("Message enriched with device metadata.")
		return enrichedMsg, false, nil // Don't skip, process the enriched message
	}
}
