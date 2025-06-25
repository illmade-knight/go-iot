//go:build cloudintegration

package servicemanager_test

import (
	"cloud.google.com/go/bigquery"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServiceManager_Integration_CloudProject tests the manager against a real GCP project.
// To run: go test -v -tags=cloudintegration .
func TestServiceManager_Integration_CloudProject(t *testing.T) {
	cloudProjectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if cloudProjectID == "" {
		t.Skip("Skipping cloud integration test: GOOGLE_CLOUD_PROJECT must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	runID := uuid.New().String()[:8]

	dataflowName := "sm-cit-df-" + runID
	topicName := "sm-cit-topic-" + runID
	subName := "sm-cit-sub-" + runID
	bucketName := "sm-cit-bucket-" + runID // Note: Bucket names must be globally unique
	datasetName := "sm_cit_dataset_" + runID
	tableName := "sm_cit_table_" + runID

	// Define the configuration using the new dataflow-centric structure
	cfg := &servicemanager.TopLevelConfig{
		DefaultProjectID: cloudProjectID,
		Environments:     map[string]servicemanager.EnvironmentSpec{"cloudtest": {ProjectID: cloudProjectID}},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name: dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager.ResourcesSpec{
					GCSBuckets: []servicemanager.GCSBucket{{Name: bucketName}},
					Topics:     []servicemanager.TopicConfig{{Name: topicName}},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{Name: subName, Topic: topicName, AckDeadlineSeconds: 25},
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
			},
		},
	}

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(cfg)
	require.NoError(t, err)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 1. Create real clients for the cloud test and an emulated one for BQ ---
	realGCSClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	defer realGCSClient.Close()

	realPSClient, err := pubsub.NewClient(ctx, cloudProjectID)
	require.NoError(t, err)
	defer realPSClient.Close()

	bqClient, err := bigquery.NewClient(ctx, cloudProjectID)
	defer bqClient.Close()

	// --- 2. Create adapters and ServiceManager ---
	gcsAdapter := servicemanager.NewGCSClientAdapter(realGCSClient)
	psAdapter := servicemanager.MessagingClientFromPubsubClient(realPSClient)
	bqAdapter := servicemanager.NewBigQueryClientAdapter(bqClient)

	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings": types.GardenMonitorReadings{},
	}

	manager, err := servicemanager.NewServiceManagerFromClients(psAdapter, gcsAdapter, bqAdapter, servicesDef, schemaRegistry, logger)
	require.NoError(t, err)

	// --- Teardown is deferred to ensure resources are cleaned up even if tests fail ---
	defer func() {
		t.Log("--- Starting deferred teardown ---")
		// Use the new TeardownAll signature
		err := manager.TeardownAll(ctx, "cloudtest")
		assert.NoError(t, err, "Deferred teardown should not fail")

		// Verify GCS Bucket is gone
		_, err = realGCSClient.Bucket(bucketName).Attrs(ctx)
		assert.ErrorIs(t, err, storage.ErrBucketNotExist, "GCS bucket should NOT exist after deferred teardown")

		// Verify Pub/Sub resources are gone
		topicExists, err := realPSClient.Topic(topicName).Exists(ctx)
		assert.NoError(t, err)
		assert.False(t, topicExists, "Pub/Sub topic should NOT exist after deferred teardown")
		subExists, err := realPSClient.Subscription(subName).Exists(ctx)
		assert.NoError(t, err)
		assert.False(t, subExists, "Pub/Sub subscription should NOT exist after deferred teardown")

		_, err = bqClient.Dataset(datasetName).Metadata(ctx)
		assert.Error(t, err, "BigQuery dataset should NOT exist after deferred teardown")
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for BQ dataset should be notFound")

		t.Log("--- Deferred teardown complete ---")
	}()

	// --- 4. Setup and Verify ---
	t.Run("Cloud_Setup_And_Verify", func(t *testing.T) {
		// Use the new SetupAll signature
		_, err := manager.SetupAll(ctx, "cloudtest")
		require.NoError(t, err)

		// Give GCP a moment for propagation
		t.Log("Waiting for GCP resource propagation after setup...")
		time.Sleep(10 * time.Second)

		// Verify GCS Bucket
		_, err = realGCSClient.Bucket(bucketName).Attrs(ctx)
		assert.NoError(t, err, "GCS bucket should exist after setup")

		// Verify Pub/Sub
		topic := realPSClient.Topic(topicName)
		topicExists, err := topic.Exists(ctx)
		assert.NoError(t, err)
		assert.True(t, topicExists, "Pub/Sub topic should exist after setup")

		sub := realPSClient.Subscription(subName)
		subExists, err := sub.Exists(ctx)
		assert.NoError(t, err)
		assert.True(t, subExists, "Pub/Sub subscription should exist after setup")

		ds := bqClient.Dataset(datasetName)
		_, err = ds.Metadata(ctx)
		require.NoError(t, err, "BigQuery dataset should exist after setup")
		tbl := ds.Table(tableName)
		_, err = tbl.Metadata(ctx)
		require.NoError(t, err, "BigQuery table should exist after setup")
	})
}
