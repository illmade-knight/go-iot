package enrichment

import (
	"context"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
)

// DeviceMetadataFetcher is an alias for the fetcher interface.
type DeviceMetadataFetcher = device.DeviceMetadataFetcher

// NewMessageEnricher creates the MessageTransformer function for enrichment.
// It now uses the new, simpler publisher for dead-lettering.
func NewMessageEnricher(
	fetcher DeviceMetadataFetcher,
	deadLetterPublisher messagepipeline.SimplePublisher, // Use the new simple publisher interface
	logger zerolog.Logger,
) messagepipeline.MessageTransformer[types.PublishMessage] {

	return func(msg types.ConsumedMessage) (*types.PublishMessage, bool, error) {
		// handleFailure now uses the simple publisher.
		handleFailure := func(reason, eui string) {
			if deadLetterPublisher != nil {
				attributes := map[string]string{
					"error":      reason,
					"device_eui": eui,
					"msg_id":     msg.ID,
				}
				// The call is now a simple, direct Publish.
				_ = deadLetterPublisher.Publish(context.Background(), msg.Payload, attributes)
			}
			msg.Ack()
		}

		// The service assumes the initial consumer has populated DeviceInfo from attributes.
		if msg.DeviceInfo == nil || msg.DeviceInfo.UID == "" {
			handleFailure("message_missing_uid_attribute", "")
			return nil, true, nil // Return skip=true to Ack the message
		}

		deviceEUI := msg.DeviceInfo.UID
		clientID, locationID, category, err := fetcher(deviceEUI)
		if err != nil {
			logger.Error().Err(err).Str("device_eui", deviceEUI).Str("msg_id", msg.ID).Msg("Failed to fetch device metadata for enrichment.")
			handleFailure("metadata_fetch_failed", deviceEUI)
			return nil, true, nil // Return skip=true to Ack the message
		}

		// On success, create a new PublishMessage with the updated DeviceInfo.
		enrichedMsg := &types.PublishMessage{
			ID:          msg.ID,
			Payload:     msg.Payload,
			PublishTime: msg.PublishTime,
			DeviceInfo: &types.DeviceInfo{
				UID:        deviceEUI,
				Name:       clientID,
				Location:   locationID,
				ServiceTag: category, // Assuming category maps to ServiceTag
			},
		}

		logger.Debug().Str("msg_id", msg.ID).Str("device_eui", deviceEUI).Msg("Message enriched with device metadata.")
		return enrichedMsg, false, nil // Return the enriched message for the main pipeline
	}
}
