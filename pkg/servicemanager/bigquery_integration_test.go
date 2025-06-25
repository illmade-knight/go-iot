//go:build integration

package servicemanager_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	telemetry "github.com/illmade-knight/go-iot/gen/go/protos/telemetry"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	projectID                   = "sm-bq-test-project"
	testSMBQEmulatorImage       = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testSMBQEmulatorGRPCPortStr = "9060"
	testSMBQEmulatorRestPortStr = "9050"
	testSMBQProjectID           = "sm-bq-test-project"
	testSMBQDatasetID           = "sm_test_dataset_integ"
	testSMBQAnotherDatasetID    = "another_dataset_sm_integ"
	testSMBQTableID             = "sm_test_meter_readings_integ"
)

// newEmulatorBQClient creates a BigQuery client configured for the emulator.
func newEmulatorBQClient(ctx context.Context, t *testing.T, projectID string, clientOpts []option.ClientOption) *bigquery.Client {
	t.Helper()
	require.NotEmpty(t, projectID, "projectID must be set for newEmulatorBQClient")
	client, err := bigquery.NewClient(ctx, projectID, clientOpts...)
	require.NoError(t, err, "newEmulatorBQClient: Failed to create BigQuery client. Project: %s", projectID)
	return client
}

func TestBigQueryManager_Integration_SetupAndTeardown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Setup BigQuery Emulator
	dm := map[string]string{}
	sm := map[string]interface{}{}
	clientConnection := emulators.SetupBigQueryEmulator(t, ctx, emulators.GetDefaultBigQueryConfig(projectID, dm, sm))

	// Define the resources for the test directly as a struct
	yamlTimePartitioningField := "original_mqtt_time"
	yamlClusteringFields := []string{"location_id", "device_type"}

	testResources := servicemanager.ResourcesSpec{
		BigQueryDatasets: []servicemanager.BigQueryDataset{
			{
				Name:        testSMBQDatasetID,
				Description: "Test dataset for meter readings (SM)",
				Labels:      map[string]string{"env": "integration_test_bq_sm", "owner": "sm_test_bq"},
			},
			{
				Name:     testSMBQAnotherDatasetID,
				Location: "US",
			},
		},
		BigQueryTables: []servicemanager.BigQueryTable{
			{
				Name:                   testSMBQTableID,
				Dataset:                testSMBQDatasetID,
				Description:            "Stores decoded meter readings from various devices (SM).",
				SchemaSourceType:       "go_struct",
				SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings",
				TimePartitioningField:  yamlTimePartitioningField,
				TimePartitioningType:   "DAY",
				ClusteringFields:       yamlClusteringFields,
			},
		},
	}

	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings": telemetry.MeterReading{},
	}

	// Create the BQ client for the manager pointing to the emulator
	managerConcreteClient := newEmulatorBQClient(ctx, t, projectID, clientConnection.ClientOptions)
	defer managerConcreteClient.Close()

	managerBQClientAdapter := servicemanager.NewBigQueryClientAdapter(managerConcreteClient)
	manager, err := servicemanager.NewBigQueryManager(managerBQClientAdapter, logger, schemaRegistry)
	require.NoError(t, err)

	// --- Test Setup ---
	t.Run("SetupBigQueryResources", func(t *testing.T) {
		// Call Setup with the new signature
		err = manager.Setup(ctx, testSMBQProjectID, "EU", testResources)
		require.NoError(t, err, "BigQueryManager.Setup failed")

		// Verification client - create a new one to verify state independently
		verifyClient := newEmulatorBQClient(ctx, t, testSMBQProjectID, clientConnection.ClientOptions)
		defer verifyClient.Close()

		// Verify dataset1
		ds1 := verifyClient.Dataset(testSMBQDatasetID)
		ds1Meta, err := ds1.Metadata(ctx)
		require.NoError(t, err, "Error getting metadata for dataset %s", testSMBQDatasetID)
		assert.Equal(t, "Test dataset for meter readings (SM)", ds1Meta.Description)
		assert.Equal(t, "EU", ds1Meta.Location, "Dataset location should be EU from default location passed to Setup")

		// Verify dataset2
		ds2 := verifyClient.Dataset(testSMBQAnotherDatasetID)
		ds2Meta, err := ds2.Metadata(ctx)
		require.NoError(t, err, "Error getting metadata for dataset %s", testSMBQAnotherDatasetID)
		assert.Equal(t, "US", ds2Meta.Location, "Dataset location should be US as specified in its config")

		// Verify table
		table := ds1.Table(testSMBQTableID)
		tableMeta, err := table.Metadata(ctx)
		require.NoError(t, err, "Error getting metadata for table %s", testSMBQTableID)
		assert.Equal(t, "Stores decoded meter readings from various devices (SM).", tableMeta.Description)
		require.NotNil(t, tableMeta.TimePartitioning, "Time partitioning should be set")
		assert.Equal(t, yamlTimePartitioningField, tableMeta.TimePartitioning.Field)
		require.NotNil(t, tableMeta.Clustering, "Clustering should be set")
		assert.ElementsMatch(t, yamlClusteringFields, tableMeta.Clustering.Fields)
	})

	// --- Test Teardown ---
	t.Run("TeardownBigQueryResources", func(t *testing.T) {
		// Call Teardown with the new signature
		err = manager.Teardown(ctx, testSMBQProjectID, testResources, false)
		require.NoError(t, err, "BigQueryManager.Teardown failed")

		verifyClient := newEmulatorBQClient(ctx, t, testSMBQProjectID, clientConnection.ClientOptions)
		defer verifyClient.Close()

		table := verifyClient.Dataset(testSMBQDatasetID).Table(testSMBQTableID)
		_, err = table.Metadata(ctx)
		require.Error(t, err, "Table %s should not exist after teardown", testSMBQTableID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for table GetMetadata should be 'notFound'")

		ds := verifyClient.Dataset(testSMBQDatasetID)
		_, err = ds.Metadata(ctx)
		require.Error(t, err, "Dataset %s should not exist after teardown", testSMBQDatasetID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for dataset GetMetadata should be 'notFound'")
	})
}
