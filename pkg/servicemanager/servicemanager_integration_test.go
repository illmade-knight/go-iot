//go:build integration

package servicemanager_test

import (
	"context"
	"github.com/illmade-knight/go-iot/pkg/types"
	"io"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServiceManager_Integration_Emulators tests the full setup/teardown lifecycle against local emulators.
func TestServiceManager_Integration_Emulators(t *testing.T) {
	ctx := context.Background()
	projectID := "emulator-test-project"
	runID := uuid.New().String()[:8]

	dataflowName := "full-stack-test-df-" + runID
	topicName := "test-topic-" + runID
	subName := "test-sub-" + runID
	bucketName := servicemanager.GenerateTestBucketName("test-bucket-" + runID)
	datasetName := "test_dataset_" + runID
	tableName := "test_table_" + runID

	require.True(t, servicemanager.IsValidBucketName(bucketName))

	// Define the test configuration using the new ResourceGroup structure.
	cfg := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"integration": {ProjectID: projectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "test-service"},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name: dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
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
							SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/pkg/servicemanager.GardenMonitorReadings",
						},
					},
				},
			},
		},
	}

	// --- 1. Setup Emulators and Clients (following the original, working pattern) ---
	// FIX: Pass an empty string for the bucket name to prevent the emulator helper
	// from pre-creating the bucket. This gives our ServiceManager a clean slate.
	gcsConfig := emulators.GetDefaultGCSConfig(projectID, "")
	gcsConnection := emulators.SetupGCSEmulator(t, ctx, gcsConfig)
	gcsClient := emulators.GetStorageClient(t, ctx, gcsConfig, gcsConnection.ClientOptions)
	defer gcsClient.Close()

	psConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID, nil))
	psEmulatorClient, err := pubsub.NewClient(ctx, projectID, psConnection.ClientOptions...)
	require.NoError(t, err)
	defer psEmulatorClient.Close()

	bqConnection := emulators.SetupBigQueryEmulator(t, ctx, emulators.GetDefaultBigQueryConfig(projectID, nil, nil))
	bqGoogleClient := newEmulatorBQClient(ctx, t, projectID, bqConnection.ClientOptions)
	defer bqGoogleClient.Close()

	// --- 2. Create Adapters and ServiceManager using injection ---
	gcsAdapter := servicemanager.NewGCSClientAdapter(gcsClient)
	psAdapter := servicemanager.MessagingClientFromPubsubClient(psEmulatorClient)
	bqAdapter := servicemanager.NewBigQueryClientAdapter(bqGoogleClient)

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(cfg)
	require.NoError(t, err)
	logger := zerolog.New(io.Discard)
	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/go-iot/pkg/servicemanager.GardenMonitorReadings": types.GardenMonitorReadings{},
	}

	manager, err := servicemanager.NewServiceManagerFromClients(psAdapter, gcsAdapter, bqAdapter, servicesDef, schemaRegistry, logger)
	require.NoError(t, err)

	// --- 3. Run Setup and Verify ---
	t.Run("SetupAll_And_Verify_With_Emulators", func(t *testing.T) {
		_, err := manager.SetupAll(ctx, "integration")
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
		gcsAttrs, err := gcsVerifyClient.Bucket(bucketName).Attrs(ctx)
		require.NoError(t, err, "GCS bucket should exist after setup")
		assert.Equal(t, bucketName, gcsAttrs.Name)

		// Verify Pub/Sub Topic and Subscription
		topic := psVerifyClient.Topic(topicName)
		topicExists, err := topic.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, topicExists, "Pub/Sub topic should exist after setup")

		sub := psVerifyClient.Subscription(subName)
		subExists, err := sub.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, subExists, "Pub/Sub subscription should exist after setup")

		// Verify BigQuery Dataset and Table
		ds := bqVerifyClient.Dataset(datasetName)
		_, err = ds.Metadata(ctx)
		require.NoError(t, err, "BigQuery dataset should exist after setup")

		tbl := ds.Table(tableName)
		_, err = tbl.Metadata(ctx)
		require.NoError(t, err, "BigQuery table should exist after setup")
	})

	// --- 4. Teardown and Verify ---
	t.Run("TeardownAll_And_Verify_With_Emulators", func(t *testing.T) {
		err := manager.TeardownAll(ctx, "integration")
		require.NoError(t, err)

		// Create direct clients for verification
		gcsVerifyClient := emulators.GetStorageClient(t, ctx, gcsConfig, gcsConnection.ClientOptions)
		defer gcsVerifyClient.Close()
		psVerifyClient, err := pubsub.NewClient(ctx, projectID, psConnection.ClientOptions...)
		require.NoError(t, err)
		defer psVerifyClient.Close()
		bqVerifyClient := newEmulatorBQClient(ctx, t, projectID, bqConnection.ClientOptions)
		defer bqVerifyClient.Close()

		// Verify GCS Bucket is gone
		_, err = gcsVerifyClient.Bucket(bucketName).Attrs(ctx)
		assert.ErrorIs(t, err, storage.ErrBucketNotExist, "GCS bucket should NOT exist after teardown")

		// Verify Pub/Sub resources are gone
		topicExists, err := psVerifyClient.Topic(topicName).Exists(ctx)
		require.NoError(t, err)
		assert.False(t, topicExists, "Pub/Sub topic should NOT exist after teardown")

		subExists, err := psVerifyClient.Subscription(subName).Exists(ctx)
		require.NoError(t, err)
		assert.False(t, subExists, "Pub/Sub subscription should NOT exist after teardown")

		// Verify BigQuery resources are gone
		_, err = bqVerifyClient.Dataset(datasetName).Metadata(ctx)
		assert.Error(t, err, "BigQuery dataset should NOT exist after teardown")
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for dataset GetMetadata should be 'notFound'")
	})
}
