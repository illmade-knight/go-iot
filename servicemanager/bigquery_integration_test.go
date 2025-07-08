//go:build integration

package servicemanager_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	telemetry "github.com/illmade-knight/go-iot/gen/go/protos/telemetry"
	"github.com/illmade-knight/go-iot/helpers/emulators" // This is the user's emulator package
	"github.com/illmade-knight/go-iot/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	projectID                = "sm-bq-test-project"
	testSMBQProjectID        = "sm-bq-test-project"
	testSMBQDatasetID        = "sm_test_dataset_integ"
	testSMBQAnotherDatasetID = "another_dataset_sm_integ"
	testSMBQTableID          = "sm_test_meter_readings_integ"
)

// getTestBigQueryResourcesForIntegration returns a CloudResourcesSpec for integration tests.
func getTestBigQueryResourcesForIntegration() servicemanager.CloudResourcesSpec {
	return servicemanager.CloudResourcesSpec{
		BigQueryDatasets: []servicemanager.BigQueryDataset{
			{CloudResource: servicemanager.CloudResource{Name: testSMBQDatasetID, Description: "Main dataset for SM integration tests", Labels: map[string]string{"env": "integration"}}, Location: "US"},
			{CloudResource: servicemanager.CloudResource{Name: testSMBQAnotherDatasetID, Description: "Another dataset for SM integration tests", Labels: map[string]string{"env": "integration"}, TeardownProtection: true}, Location: "US"}, // TeardownProtection re-added
		},
		BigQueryTables: []servicemanager.BigQueryTable{
			{
				CloudResource:          servicemanager.CloudResource{Name: testSMBQTableID, Description: "Table for meter readings from various devices (SM).", Labels: map[string]string{"purpose": "meter-readings"}},
				Dataset:                testSMBQDatasetID,
				SchemaSourceType:       "go_struct",
				SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/gen/go/protos/telemetry.MeterReading", // Full path to the Go struct
				TimePartitioningField:  "original_mqtt_time",                                                    // Field for time partitioning
				TimePartitioningType:   "DAY",                                                                   // Type of partitioning (e.g., DAY, HOUR, MONTH, YEAR)
				ClusteringFields:       []string{"meter_id", "device_id"},                                       // Fields for clustering
			},
		},
	}
}

func TestBigQueryManager_Integration(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel) // Ensure debug level is enabled for verbose logs
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	ctx := context.Background()

	logger.Info().Msg("Starting TestBigQueryManager_Integration test.")

	testResources := getTestBigQueryResourcesForIntegration()

	// Prepare datasetTables map for the emulator config from testResources
	datasetTables := make(map[string]string)
	for _, table := range testResources.BigQueryTables {
		datasetTables[table.Dataset] = table.CloudResource.Name
	}
	logger.Debug().Msg("Prepared datasetTables map for emulator config.")

	// Prepare schemaMappings map for the emulator config from schemaRegistry
	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/go-iot/gen/go/protos/telemetry.MeterReading": &telemetry.MeterReadingBQWrapper{},
	}
	logger.Debug().Msg("Prepared schemaMappings map for emulator config.")

	// Create the BigQueryConfig using the user's GetDefaultBigQueryConfig
	bqConfig := emulators.GetDefaultBigQueryConfig(
		testSMBQProjectID,
		datasetTables,
		schemaRegistry, // Pass the schema mappings directly
	)
	logger.Debug().Msg("Created BigQueryConfig for emulator.")

	// --- Emulator Setup using user's SetupBigQueryEmulator ---
	logger.Info().Msg("Attempting to set up BigQuery emulator...")
	connInfo := emulators.SetupBigQueryEmulator(t, ctx, bqConfig)
	logger.Info().Msg("BigQuery emulator started and resources initialized by emulator.")

	logger.Info().Msg("Creating new BigQueryManager...")
	manager, err := servicemanager.NewBigQueryManager(nil, logger, schemaRegistry)
	require.NoError(t, err, "Failed to create BigQueryManager")
	logger.Info().Msg("BigQueryManager created.")

	// --- Test Setup: manager.Setup will now verify/update existing resources ---
	t.Run("SetupBigQueryResources", func(t *testing.T) {
		logger.Info().Msg("Starting 'SetupBigQueryResources' sub-test. Calling manager.Setup...")
		err = manager.Setup(ctx,
			servicemanager.Environment{
				Name:               "default",
				ProjectID:          testSMBQProjectID,
				Location:           "US",
				TeardownProtection: false,
			}, testResources)
		require.NoError(t, err, "BigQueryManager.Setup failed (should verify/update existing resources)")
		logger.Info().Msg("manager.Setup completed. Resources should be verified/updated.")

		// Verify resources manually via a direct client connection to the emulator
		logger.Info().Msg("Creating verification client for BigQuery emulator...")
		// Inlined newEmulatorBQClient:
		verifyClient, err := bigquery.NewClient(ctx, testSMBQProjectID, connInfo.ClientOptions...)
		require.NoError(t, err, "Failed to create BigQuery client for emulator verification")
		defer verifyClient.Close()
		logger.Info().Msg("Verification client created. Starting resource verification...")

		logger.Debug().Str("dataset", testSMBQDatasetID).Msg("Verifying Dataset 1...")
		ds1 := verifyClient.Dataset(testSMBQDatasetID)
		dsMeta1, err := ds1.Metadata(ctx)
		require.NoError(t, err, "Dataset %s should exist", testSMBQDatasetID)
		assert.Equal(t, "Main dataset for SM integration tests", dsMeta1.Description)
		assert.Contains(t, dsMeta1.Labels, "env")
		assert.Equal(t, "integration", dsMeta1.Labels["env"])
		assert.Equal(t, "US", dsMeta1.Location)
		logger.Debug().Str("dataset", testSMBQDatasetID).Msg("Dataset 1 verified.")

		logger.Debug().Str("dataset", testSMBQAnotherDatasetID).Msg("Verifying Dataset 2 (Protected)...")
		ds2 := verifyClient.Dataset(testSMBQAnotherDatasetID)
		dsMeta2, err := ds2.Metadata(ctx)
		require.NoError(t, err, "Dataset %s should exist", testSMBQAnotherDatasetID)
		assert.Equal(t, "Another dataset for SM integration tests", dsMeta2.Description)
		logger.Debug().Str("dataset", testSMBQAnotherDatasetID).Msg("Dataset 2 verified.")

		logger.Debug().Str("table", testSMBQTableID).Msg("Verifying Table...")
		table := verifyClient.Dataset(testSMBQDatasetID).Table(testSMBQTableID)
		tableMeta, err := table.Metadata(ctx)
		require.NoError(t, err, "Table %s should exist", testSMBQTableID)
		assert.Equal(t, "Table for meter readings from various devices (SM).", tableMeta.Description)
		assert.Contains(t, tableMeta.Labels, "purpose")
		assert.Equal(t, "meter-readings", tableMeta.Labels["purpose"])
		logger.Debug().Str("table", testSMBQTableID).Msg("Table verified.")

		logger.Debug().Msg("Verifying schema, time partitioning, and clustering...")
		yamlTimePartitioningField := "original_mqtt_time"
		yamlClusteringFields := []string{"meter_id", "device_id"}
		expectedSchema, err := bigquery.InferSchema(&telemetry.MeterReadingBQWrapper{})
		require.NoError(t, err, "Failed to infer MeterReadingBQWrapper schema")

		assert.ElementsMatch(t, expectedSchema, tableMeta.Schema, "Table schema should match the inferred Go struct schema")
		require.NotNil(t, tableMeta.TimePartitioning, "Time partitioning should be set")
		assert.Equal(t, yamlTimePartitioningField, tableMeta.TimePartitioning.Field)
		require.NotNil(t, tableMeta.Clustering, "Clustering should be set")
		assert.ElementsMatch(t, yamlClusteringFields, tableMeta.Clustering.Fields)
		logger.Info().Msg("'SetupBigQueryResources' sub-test completed successfully.")
	})

	// --- Test Teardown ---
	t.Run("TeardownBigQueryResources", func(t *testing.T) {
		logger.Info().Msg("Starting 'TeardownBigQueryResources' sub-test. Calling manager.Teardown...")
		err = manager.Teardown(ctx,
			servicemanager.Environment{
				Name:               "default",
				ProjectID:          testSMBQProjectID,
				Location:           "EU", // Location doesn't matter for teardown
				TeardownProtection: false,
			}, testResources)
		require.NoError(t, err, "BigQueryManager.Teardown failed")
		logger.Info().Msg("manager.Teardown completed. Starting resource verification...")

		// Verify resources were torn down, except for the protected dataset
		logger.Info().Msg("Creating verification client for BigQuery emulator after teardown...")
		// Inlined newEmulatorBQClient:
		verifyClient, err := bigquery.NewClient(ctx, testSMBQProjectID, connInfo.ClientOptions...)
		require.NoError(t, err, "Failed to create BigQuery client for emulator verification after teardown")
		defer verifyClient.Close()
		logger.Info().Msg("Verification client created. Verifying teardown results...")

		logger.Debug().Str("table", testSMBQTableID).Msg("Verifying Table non-existence...")
		table := verifyClient.Dataset(testSMBQDatasetID).Table(testSMBQTableID)
		_, err = table.Metadata(ctx)
		require.Error(t, err, "Table %s should not exist after teardown", testSMBQTableID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for table GetMetadata should be 'notFound'")
		logger.Debug().Str("table", testSMBQTableID).Msg("Table non-existence verified.")

		logger.Debug().Str("dataset", testSMBQDatasetID).Msg("Verifying Dataset 1 non-existence...")
		ds1 := verifyClient.Dataset(testSMBQDatasetID)
		_, err = ds1.Metadata(ctx)
		require.Error(t, err, "Dataset %s should not exist after teardown", testSMBQDatasetID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for dataset GetMetadata should be 'notFound'")
		logger.Debug().Str("dataset", testSMBQDatasetID).Msg("Dataset 1 non-existence verified.")

		logger.Debug().Str("dataset", testSMBQAnotherDatasetID).Msg("Verifying Protected Dataset 2 existence...")
		ds2 := verifyClient.Dataset(testSMBQAnotherDatasetID)
		_, err = ds2.Metadata(ctx)
		require.NoError(t, err, "Protected dataset %s should still exist after teardown", testSMBQAnotherDatasetID)
		logger.Debug().Str("dataset", testSMBQAnotherDatasetID).Msg("Protected Dataset 2 existence verified.")
		logger.Info().Msg("'TeardownBigQueryResources' sub-test completed successfully.")
	})

	logger.Info().Msg("TestBigQueryManager_Integration test finished.")
}
