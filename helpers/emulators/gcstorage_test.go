package emulators

import (
	"context"
	"testing"
	"time"
)

func TestSetupGCSEmulator(t *testing.T) {

	// Use a context with timeout for *test operations*, not container lifecycle.
	testCtx, testCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer testCancel()

	projectID := "test-project-gcs"
	baseBucket := "test-bucket"

	cfg := GetDefaultGCSConfig(projectID, baseBucket)

	// --- Setup GCS Emulator ---
	// Pass context.Background() to SetupGCSEmulator for container lifecycle
	// This ensures the container termination is not prematurely canceled by testCtx.
	connInfo := SetupGCSEmulator(t, context.Background(), cfg)

	// --- Verify EmulatorConnectionInfo ---
	if connInfo.HTTPEndpoint.Endpoint == "" {
		t.Error("HTTPEndpoint.Endpoint is empty")
	}
	if connInfo.HTTPEndpoint.Port == "" {
		t.Error("HTTPEndpoint.Port is empty")
	}
	if len(connInfo.ClientOptions) == 0 {
		t.Error("ClientOptions are empty")
	}

	// --- Test Connectivity (using GetStorageClient) ---
	// Use testCtx for GCS client operations
	gcsClient := GetStorageClient(t, testCtx, cfg, connInfo.ClientOptions)
	if gcsClient == nil {
		t.Fatal("GetStorageClient returned nil client")
	}

	// Verify the base bucket exists (should be created by GetStorageClient)
	_, err := gcsClient.Bucket(baseBucket).Attrs(testCtx) // Use testCtx for bucket operations
	if err != nil {
		t.Fatalf("Failed to get attributes for base bucket %q: %v", baseBucket, err)
	}

	t.Logf("GCS emulator test passed. Connected to: %s", connInfo.HTTPEndpoint.Endpoint)
}

func TestGetDefaultGCSConfig(t *testing.T) {
	projectID := "default-gcs-proj"
	baseBucket := "default-bucket-name"

	cfg := GetDefaultGCSConfig(projectID, baseBucket)

	if cfg.EmulatorImage != testGCSImage {
		t.Errorf("Expected image %q, got %q", testGCSImage, cfg.EmulatorImage)
	}
	if cfg.EmulatorPort != testGCSPort {
		t.Errorf("Expected port %q, got %q", testGCSPort, cfg.EmulatorPort)
	}
	if cfg.ProjectID != projectID {
		t.Errorf("Expected project ID %q, got %q", projectID, cfg.ProjectID)
	}
	if cfg.BaseBucket != baseBucket {
		t.Errorf("Expected base bucket %q, got %q", baseBucket, cfg.BaseBucket)
	}
	if cfg.BaseStorage != "/storage/v1/b" {
		t.Errorf("Expected BaseStorage %q, got %q", "/storage/v1/b", cfg.BaseStorage)
	}
}
