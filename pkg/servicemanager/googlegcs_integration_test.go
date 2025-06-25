//go:build integration

package servicemanager_test

import (
	"context"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	gcsTestProjectID = "gcs-adapter-test-project"
	gcsTestBucket    = "gcs-adapter-test-bucket"
)

// TestGoogleGCSAdapter_Integration_WithManager tests the StorageManager's interaction
// with real GCS (via emulator) using the StorageClient adapter.
func TestGoogleGCSAdapter_Integration_WithManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	gceCfg := emulators.GetDefaultGCSConfig(gcsTestProjectID, gcsTestBucket)
	connection := emulators.SetupGCSEmulator(t, ctx, gceCfg)
	client := emulators.GetStorageClient(t, ctx, gceCfg, connection.ClientOptions)

	// --- Configuration ---
	// Define the resources for the test directly as a struct, instead of parsing YAML.
	testBucketName := "adapter-test-bucket"
	testResources := servicemanager.ResourcesSpec{
		GCSBuckets: []servicemanager.GCSBucket{
			{
				Name:              testBucketName,
				StorageClass:      "STANDARD",
				VersioningEnabled: false, // NOTE: GCS emulator (fs-storage) doesn't support versioning.
				Labels:            map[string]string{"tested_by": "gcs-adapter"},
				LifecycleRules: []servicemanager.LifecycleRuleSpec{
					{
						Action:    servicemanager.LifecycleActionSpec{Type: "Delete"},
						Condition: servicemanager.LifecycleConditionSpec{AgeDays: 7},
					},
				},
			},
		},
	}
	defaultLocation := "us-central1"
	defaultLabels := map[string]string{"environment": "integration-test"}

	// --- Client and Adapter Setup ---
	// Wrap the real client with our adapter.
	adapter := servicemanager.NewGCSClientAdapter(client)
	require.NotNil(t, adapter)

	// Create the StorageManager, injecting the adapter.
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewStorageManager(adapter, logger)
	require.NoError(t, err)

	// --- Run Setup and Verify ---
	t.Run("SetupResources_Through_Adapter", func(t *testing.T) {
		// Call Setup with the new, refactored signature.
		err = manager.Setup(ctx, gcsTestProjectID, defaultLocation, defaultLabels, testResources)
		require.NoError(t, err, "StorageManager.Setup through adapter failed")

		// Verify Bucket
		bucketHandle := client.Bucket(testBucketName)
		attrs, err := bucketHandle.Attrs(ctx)
		require.NoError(t, err, "Failed to get attributes for created bucket")

		assert.Equal(t, testBucketName, attrs.Name, "Bucket name mismatch")
		assert.False(t, attrs.VersioningEnabled, "Versioning should be disabled for the test")

		t.Log("Skipping lifecycle rule verification; emulator may not support it reliably.")
	})

	// --- Run Teardown and Verify ---
	t.Run("TeardownResources_Through_Adapter", func(t *testing.T) {
		// Call Teardown with the new, refactored signature.
		err = manager.Teardown(ctx, testResources, false) // teardownProtection is false
		require.NoError(t, err, "StorageManager.Teardown through adapter failed")

		// Verify Bucket is gone
		bucketHandle := client.Bucket(testBucketName)
		_, err = bucketHandle.Attrs(ctx)
		assert.ErrorIs(t, err, storage.ErrBucketNotExist, "Bucket should have been deleted by the manager via the adapter")
	})
}
