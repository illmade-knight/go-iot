//go:build integration

package servicemanager_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
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

	client, emulatorCleanupFunc := emulators.SetupGCSEmulator(t, ctx, emulators.GetDefaultGCSConfig(gcsTestProjectID, gcsTestBucket))
	defer emulatorCleanupFunc()

	// --- Configuration ---
	testBucketName := "adapter-test-bucket"
	testBucketLocation := "us-central1"
	// NOTE: Versioning is set to false here because the GCS emulator (fs-storage)
	// does not support bucket versioning and will return an error if it's enabled.
	yamlContent := fmt.Sprintf(`
default_project_id: "%s"
default_location: "%s"
environments:
  integration:
    project_id: "%s"
    default_labels:
      environment: "integration-test"
resources:
  gcs_buckets:
    - name: "%s"
      storage_class: "STANDARD"
      versioning_enabled: false
      labels: { "tested_by": "gcs-adapter" }
      lifecycle_rules:
        - action: { type: "Delete" }
          condition: { age_days: 7 }
`, gcsTestProjectID, testBucketLocation, gcsTestProjectID, testBucketName)

	configFilePath := CreateManagerTestYAMLFile(t, yamlContent)
	cfg, err := servicemanager.LoadAndValidateConfig(configFilePath)
	require.NoError(t, err)

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
		err = manager.Setup(ctx, cfg, "integration")
		require.NoError(t, err, "StorageManager.Setup through adapter failed")

		// Verify Bucket
		bucketHandle := client.Bucket(testBucketName)
		attrs, err := bucketHandle.Attrs(ctx)
		require.NoError(t, err, "Failed to get attributes for created bucket")

		assert.Equal(t, testBucketName, attrs.Name, "Bucket name mismatch")
		assert.False(t, attrs.VersioningEnabled, "Versioning should be disabled for the test")

		t.Log("Skipping lifecycle rule verification; emulator may not support it.")
	})

	// --- Run Teardown and Verify ---
	t.Run("TeardownResources_Through_Adapter", func(t *testing.T) {
		err = manager.Teardown(ctx, cfg, "integration")
		require.NoError(t, err, "StorageManager.Teardown through adapter failed")

		// Verify Bucket is gone
		bucketHandle := client.Bucket(testBucketName)
		_, err = bucketHandle.Attrs(ctx)
		assert.ErrorIs(t, err, storage.ErrBucketNotExist, "Bucket should have been deleted by the manager via the adapter")
	})
}
