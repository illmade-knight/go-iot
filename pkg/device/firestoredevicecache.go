package connectors

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
)

// FirestoreFetcherConfig holds configuration for the Firestore metadata fetcher.
type FirestoreFetcherConfig struct {
	ProjectID      string
	CollectionName string // e.g., "devices"
	// CredentialsFile removed as the client is now passed in
}

// LoadFirestoreFetcherConfigFromEnv loads Firestore fetcher configuration.
// It no longer loads CredentialsFile as the client is now passed in.
func LoadFirestoreFetcherConfigFromEnv() (*FirestoreFetcherConfig, error) {
	cfg := &FirestoreFetcherConfig{
		ProjectID:      os.Getenv("GCP_PROJECT_ID"),
		CollectionName: os.Getenv("FIRESTORE_COLLECTION_DEVICES"),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Firestore")
	}
	if cfg.CollectionName == "" {
		return nil, errors.New("FIRESTORE_COLLECTION_DEVICES environment variable not set")
	}
	return cfg, nil
}

// GoogleDeviceMetadataFetcher implements DeviceMetadataFetcher using Google Cloud Firestore.
type GoogleDeviceMetadataFetcher struct {
	client         *firestore.Client // Client is now injected
	collectionName string
	logger         zerolog.Logger
}

// NewGoogleDeviceMetadataFetcher creates a new fetcher that uses Firestore.
// It now takes an existing *firestore.Client instance, allowing for dependency injection.
func NewGoogleDeviceMetadataFetcher(ctx context.Context, client *firestore.Client, cfg *FirestoreFetcherConfig, logger zerolog.Logger) (*GoogleDeviceMetadataFetcher, error) { // Signature changed
	if client == nil {
		return nil, errors.New("firestore client cannot be nil")
	}

	logger.Info().Str("project_id", cfg.ProjectID).Str("collection", cfg.CollectionName).Msg("GoogleDeviceMetadataFetcher initialized successfully with provided client")
	return &GoogleDeviceMetadataFetcher{
		client:         client, // Assign the injected client
		collectionName: cfg.CollectionName,
		logger:         logger,
	}, nil
}

// Fetch retrieves device metadata from Firestore.
// Assumes document ID in Firestore is the deviceEUI.
func (f *GoogleDeviceMetadataFetcher) Fetch(deviceEUI string) (clientID, locationID, category string, err error) {
	f.logger.Debug().Str("device_eui", deviceEUI).Msg("Fetching metadata from Firestore")

	// Use the injected client
	docRef := f.client.Collection(f.collectionName).Doc(deviceEUI)
	docSnap, err := docRef.Get(context.Background()) // Using context.Background() here for simplicity,
	// consider passing ctx from Fetcher if operation context is needed.

	if err != nil {
		if status.Code(err) == codes.NotFound {
			f.logger.Warn().Str("device_eui", deviceEUI).Msg("Device metadata not found in Firestore")
			return "", "", "", ErrMetadataNotFound
		}
		f.logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Failed to get device document from Firestore")
		return "", "", "", fmt.Errorf("firestore Get for %s: %w", deviceEUI, err)
	}

	var deviceData struct {
		ClientID       string `firestore:"clientID"`
		LocationID     string `firestore:"locationID"`
		DeviceCategory string `firestore:"deviceCategory"`
	}
	if err := docSnap.DataTo(&deviceData); err != nil {
		f.logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Failed to map Firestore document data")
		return "", "", "", fmt.Errorf("firestore DataTo for %s: %w", deviceEUI, err)
	}

	if deviceData.ClientID == "" || deviceData.LocationID == "" || deviceData.DeviceCategory == "" {
		f.logger.Warn().Str("device_eui", deviceEUI).Interface("data", deviceData).Msg("Fetched metadata from Firestore is incomplete")
		return "", "", "", fmt.Errorf("incomplete metadata for %s: %+v", deviceEUI, deviceData)
	}

	f.logger.Debug().Str("device_eui", deviceEUI).
		Str("clientID", deviceData.ClientID).
		Str("locationID", deviceData.LocationID).
		Str("category", deviceData.DeviceCategory).
		Msg("Successfully fetched metadata from Firestore")
	return deviceData.ClientID, deviceData.LocationID, deviceData.DeviceCategory, nil
}

// Close no longer closes the client it didn't create.
// The caller of NewGoogleDeviceMetadataFetcher is responsible for closing the client.
func (f *GoogleDeviceMetadataFetcher) Close() error {
	// The client is passed in, so the caller owns its lifecycle.
	// We should not close a client we didn't create.
	f.logger.Info().Msg("GoogleDeviceMetadataFetcher does not close the injected Firestore client.")
	return nil
}
