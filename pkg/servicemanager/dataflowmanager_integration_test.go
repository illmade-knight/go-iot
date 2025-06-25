//go:build integration

package servicemanager_test

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// TestDataflowManager_Integration_Emulators tests the DataflowManager's setup/teardown lifecycle against local emulators.
// This test follows the same pattern as the successful ServiceManager integration test.
func TestDataflowManager_Integration_Emulators(t *testing.T) {
	ctx := context.Background()
	projectID := "df-manager-it-project"
	runID := uuid.New().String()[:8]

	// Define resource names for the test
	topicName := "df-it-topic-" + runID
	subName := "df-it-sub-" + runID
	// Note: Bucket names must be globally unique and follow DNS naming conventions.
	bucketName := "df-it-bucket-" + strings.ReplaceAll(runID, "-", "")
	datasetName := "df_it_dataset_" + runID
	tableName := "df_it_table_" + runID

	// Define the specific ResourceGroup for this DataflowManager to handle.
	dataflowSpec := &servicemanager.ResourceGroup{
		Name: "isolated-dataflow-" + runID,
		Resources: servicemanager.ResourcesSpec{
			GCSBuckets: []servicemanager.GCSBucket{{Name: bucketName, VersioningEnabled: false}},
			Topics:     []servicemanager.TopicConfig{{Name: topicName}},
			Subscriptions: []servicemanager.SubscriptionConfig{
				{Name: subName, Topic: topicName, AckDeadlineSeconds: 123},
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: datasetName}},
			BigQueryTables: []servicemanager.BigQueryTable{
				{
					Name:                   tableName,
					Dataset:                datasetName,
					SchemaSourceType:       "go_struct",
					SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings",
				},
			},
		},
	}

	// --- 1. Setup Emulators and Real Clients ---
	gcsConfig := emulators.GetDefaultGCSConfig(projectID, "") // Don't pre-create bucket
	gcsConnection := emulators.SetupGCSEmulator(t, ctx, gcsConfig)
	gcsClient := emulators.GetStorageClient(t, ctx, gcsConfig, gcsConnection.ClientOptions)
	defer gcsClient.Close()

	psConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID, nil))
	psClient, err := pubsub.NewClient(ctx, projectID, psConnection.ClientOptions...)
	require.NoError(t, err)
	defer psClient.Close()

	bqConnection := emulators.SetupBigQueryEmulator(t, ctx, emulators.GetDefaultBigQueryConfig(projectID, nil, nil))
	bqClient := newEmulatorBQClient(ctx, t, projectID, bqConnection.ClientOptions)
	defer bqClient.Close()

	// --- 2. Create Real Managers with Emulator-Connected Clients ---
	logger := zerolog.New(zerolog.NewConsoleWriter())
	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings": types.GardenMonitorReadings{},
	}

	gcsAdapter := servicemanager.NewGCSClientAdapter(gcsClient)
	storageManager, err := servicemanager.NewStorageManager(gcsAdapter, logger)
	require.NoError(t, err)

	psAdapter := servicemanager.MessagingClientFromPubsubClient(psClient)
	messagingManager, err := servicemanager.NewMessagingManager(psAdapter, logger)
	require.NoError(t, err)

	bqAdapter := servicemanager.NewBigQueryClientAdapter(bqClient)
	bigqueryManager, err := servicemanager.NewBigQueryManager(bqAdapter, logger, schemaRegistry)
	require.NoError(t, err)

	// --- 3. Create the DataflowManager using the real managers ---
	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		messagingManager,
		storageManager,
		bigqueryManager,
		dataflowSpec,
		projectID,
		"us-central1",
		nil,
		"integration",
		logger,
	)
	require.NoError(t, err)

	// Teardown is deferred to ensure resources are cleaned up even if tests fail.
	defer func() {
		t.Log("--- Starting deferred teardown for DataflowManager test ---")
		err := dfm.Teardown(ctx, false)
		assert.NoError(t, err, "Deferred teardown should not fail")
	}()

	// --- 4. Run Setup and Verify ---
	t.Run("DataflowManager_Setup_And_Verify_With_Emulators", func(t *testing.T) {
		_, err := dfm.Setup(ctx)
		require.NoError(t, err)

		// Create direct clients for verification
		gcsVerifyClient := emulators.GetStorageClient(t, ctx, gcsConfig, gcsConnection.ClientOptions)
		defer gcsVerifyClient.Close()
		psVerifyClient, err := pubsub.NewClient(ctx, projectID, psConnection.ClientOptions...)
		require.NoError(t, err)
		defer psVerifyClient.Close()
		bqVerifyClient := newEmulatorBQClient(ctx, t, projectID, bqConnection.ClientOptions)
		defer bqVerifyClient.Close()

		// Verify GCS Bucket
		_, err = gcsVerifyClient.Bucket(bucketName).Attrs(ctx)
		assert.NoError(t, err, "GCS bucket should exist after setup")

		// Verify Pub/Sub
		topic := psVerifyClient.Topic(topicName)
		topicExists, err := topic.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, topicExists, "Pub/Sub topic should exist after setup")

		// Verify BigQuery
		_, err = bqVerifyClient.Dataset(datasetName).Metadata(ctx)
		require.NoError(t, err, "BigQuery dataset should exist after setup")
		_, err = bqVerifyClient.Dataset(datasetName).Table(tableName).Metadata(ctx)
		require.NoError(t, err, "BigQuery table should exist after setup")
	})

	// --- 5. Run Teardown and Verify ---
	t.Run("DataflowManager_Teardown_And_Verify_With_Emulators", func(t *testing.T) {
		// Teardown the resources created in the previous sub-test.
		err = dfm.Teardown(ctx, false)
		require.NoError(t, err)

		// Create direct clients for verification
		gcsVerifyClient := emulators.GetStorageClient(t, ctx, gcsConfig, gcsConnection.ClientOptions)
		defer gcsVerifyClient.Close()
		psVerifyClient, err := pubsub.NewClient(ctx, projectID, psConnection.ClientOptions...)
		require.NoError(t, err)
		defer psVerifyClient.Close()
		bqVerifyClient := newEmulatorBQClient(ctx, t, projectID, bqConnection.ClientOptions)
		defer bqVerifyClient.Close()

		// Verify all resources are gone
		_, err = gcsVerifyClient.Bucket(bucketName).Attrs(ctx)
		assert.ErrorIs(t, err, storage.ErrBucketNotExist, "GCS bucket should NOT exist after teardown")

		topicExists, err := psVerifyClient.Topic(topicName).Exists(ctx)
		require.NoError(t, err)
		assert.False(t, topicExists, "Pub/Sub topic should NOT exist after teardown")

		_, err = bqVerifyClient.Dataset(datasetName).Metadata(ctx)
		assert.Error(t, err, "BigQuery dataset should NOT exist after teardown")
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for BQ dataset should be notFound")
	})
}
