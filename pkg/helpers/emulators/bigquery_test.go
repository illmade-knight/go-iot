package emulators

import (
	"cloud.google.com/go/bigquery"
	"context"
	"reflect" // Added for reflect.DeepEqual in TestGetDefaultBigQueryConfig
	"testing"
	"time"
)

func TestSetupBigQueryEmulator(t *testing.T) {
	t.Parallel() // Allow tests to run in parallel if supported by testcontainers

	// Use a context with timeout for *test operations*, not container lifecycle.
	// Pass context.Background() or a longer-lived context to testcontainers.GenericContainer.
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer testCancel()

	projectID := "test-project-bigquery"
	datasetName := "test_dataset"
	tableName := "test_table"

	// Define a simple schema struct for the table
	type TestData struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	cfg := GetDefaultBigQueryConfig(projectID, map[string]string{datasetName: tableName}, map[string]interface{}{tableName: TestData{}})

	// --- Setup BigQuery Emulator ---
	// Pass context.Background() to SetupBigQueryEmulator for container lifecycle
	// This ensures the container termination is not prematurely canceled by testCtx.
	connInfo := SetupBigQueryEmulator(t, context.Background(), cfg)

	// --- Verify EmulatorConnectionInfo ---
	if connInfo.HTTPEndpoint.Endpoint == "" {
		t.Error("HTTPEndpoint.Endpoint is empty")
	}
	if connInfo.GRPCEndpoint.Endpoint == "" {
		t.Error("GRPCEndpoint.Endpoint is empty")
	}
	if connInfo.HTTPEndpoint.Port == "" {
		t.Error("HTTPEndpoint.Port is empty")
	}
	if connInfo.GRPCEndpoint.Port == "" {
		t.Error("GRPCEndpoint.Port is empty")
	}
	if len(connInfo.ClientOptions) == 0 {
		t.Error("ClientOptions are empty")
	}

	// --- Test Connectivity ---
	// Use testCtx for BigQuery client operations
	client, err := bigquery.NewClient(testCtx, projectID, connInfo.ClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer client.Close()

	// Verify dataset and table exist (they should be pre-created by SetupBigQueryEmulator)
	ds := client.Dataset(datasetName)
	_, err = ds.Metadata(testCtx) // Use testCtx for metadata operations
	if err != nil {
		t.Errorf("Failed to get dataset %q metadata: %v", datasetName, err)
	}

	table := ds.Table(tableName)
	_, err = table.Metadata(testCtx) // Use testCtx for metadata operations
	if err != nil {
		t.Errorf("Failed to get table %q metadata: %v", tableName, err)
	}

	t.Logf("BigQuery emulator test passed. Connected to HTTP: %s, gRPC: %s", connInfo.HTTPEndpoint.Endpoint, connInfo.GRPCEndpoint.Endpoint)
}

func TestGetDefaultBigQueryConfig(t *testing.T) {
	projectID := "test-proj-defaults"
	datasetTables := map[string]string{"ds1": "tbl1"}
	schemaMappings := map[string]interface{}{"tbl1": struct{ Name string }{}}

	cfg := GetDefaultBigQueryConfig(projectID, datasetTables, schemaMappings)

	if cfg.EmulatorImage != testBigQueryEmulatorImage {
		t.Errorf("Expected image %q, got %q", testBigQueryEmulatorImage, cfg.EmulatorImage)
	}
	if cfg.EmulatorPort != testBigQueryRestPort {
		t.Errorf("Expected REST port %q, got %q", testBigQueryRestPort, cfg.EmulatorPort)
	}
	if cfg.EmulatorGRPCPort != testBigQueryGRPCPort {
		t.Errorf("Expected gRPC port %q, got %q", testBigQueryGRPCPort, cfg.EmulatorGRPCPort)
	}
	if cfg.ProjectID != projectID {
		t.Errorf("Expected project ID %q, got %q", projectID, cfg.ProjectID)
	}
	if !reflect.DeepEqual(cfg.DatasetTables, datasetTables) {
		t.Errorf("DatasetTables mismatch: expected %v, got %v", datasetTables, cfg.DatasetTables)
	}
	if !reflect.DeepEqual(cfg.Schemas, schemaMappings) {
		t.Errorf("Schemas mismatch: expected %v, got %v", schemaMappings, cfg.Schemas)
	}
	if cfg.SetEnvVariables != false {
		t.Errorf("Expected SetEnvVariables to be false, got true")
	}
}
