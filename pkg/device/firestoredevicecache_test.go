package device

import (
	"context" // Add context import
	"io"
	"testing"

	"cloud.google.com/go/firestore" // Import firestore for the mock client
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// No longer need testGoogleDeviceMetadataFetcher struct here, as we are testing the concrete implementation.

func TestNewGoogleDeviceMetadataFetcher(t *testing.T) {
	logger := zerolog.New(io.Discard)
	ctx := context.Background()

	// Create a dummy Firestore client for the test.
	// In a real scenario, this would come from the emulator or a mock framework.
	// For this basic unit test, a non-nil pointer is sufficient.
	dummyClient := &firestore.Client{}

	type args struct {
		ctx    context.Context
		client *firestore.Client
		cfg    *FirestoreFetcherConfig
		logger zerolog.Logger
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string // Expected error message part
	}{
		{
			name: "Successful Initialization",
			args: args{
				ctx:    ctx,
				client: dummyClient,
				cfg: &FirestoreFetcherConfig{
					ProjectID:      "test-project",
					CollectionName: "test-collection",
				},
				logger: logger,
			},
			wantErr: false,
		},
		{
			name: "Nil Firestore Client",
			args: args{
				ctx:    ctx,
				client: nil, // This is the nil client case
				cfg: &FirestoreFetcherConfig{
					ProjectID:      "test-project",
					CollectionName: "test-collection",
				},
				logger: logger,
			},
			wantErr: true,
			errMsg:  "firestore client cannot be nil",
		},
		{
			name: "Missing ProjectID in Config",
			args: args{
				ctx:    ctx,
				client: dummyClient,
				cfg: &FirestoreFetcherConfig{
					CollectionName: "test-collection",
				},
				logger: logger,
			},
			wantErr: false, // Constructor doesn't check this, it's done by LoadFirestoreFetcherConfigFromEnv
			// However, a real Firestore client might error later if ProjectID is truly empty.
			// This test focuses on the *constructor's* immediate validation.
		},
		{
			name: "Missing CollectionName in Config",
			args: args{
				ctx:    ctx,
				client: dummyClient,
				cfg: &FirestoreFetcherConfig{
					ProjectID: "test-project",
				},
				logger: logger,
			},
			wantErr: false, // Constructor doesn't check this, it's done by LoadFirestoreFetcherConfigFromEnv
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetcher, err := NewGoogleDeviceMetadataFetcher(tt.args.client, tt.args.cfg, tt.args.logger)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, fetcher)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, fetcher)
				// Verify fields are correctly assigned
				assert.Equal(t, tt.args.cfg.CollectionName, fetcher.collectionName)
				assert.Equal(t, tt.args.client, fetcher.client)
			}
		})
	}
}

// TestLoadFirestoreFetcherConfigFromEnv tests the environment variable loading function.
func TestLoadFirestoreFetcherConfigFromEnv(t *testing.T) {
	t.Run("Success with all variables", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "test-project-env")
		t.Setenv("FIRESTORE_COLLECTION_DEVICES", "test-devices-env")

		cfg, err := LoadFirestoreFetcherConfigFromEnv()
		require.NoError(t, err)
		assert.NotNil(t, cfg)
		assert.Equal(t, "test-project-env", cfg.ProjectID)
		assert.Equal(t, "test-devices-env", cfg.CollectionName)
	})

	t.Run("Missing GCP_PROJECT_ID", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "") // Ensure it's unset for this test
		t.Setenv("FIRESTORE_COLLECTION_DEVICES", "test-devices-env")

		cfg, err := LoadFirestoreFetcherConfigFromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GCP_PROJECT_ID environment variable not set")
		assert.Nil(t, cfg)
	})

	t.Run("Missing FIRESTORE_COLLECTION_DEVICES", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "test-project-env")
		t.Setenv("FIRESTORE_COLLECTION_DEVICES", "") // Ensure it's unset for this test

		cfg, err := LoadFirestoreFetcherConfigFromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FIRESTORE_COLLECTION_DEVICES environment variable not set")
		assert.Nil(t, cfg)
	})
}
