//go:build cloudintegration

package servicemanager_test

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServiceManager_Integration_CloudProject tests the manager against a real GCP project.
// To run: go test -v -tags=cloudintegration .
func TestServiceManager_Integration_CloudProject(t *testing.T) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud integration test: GOOGLE_CLOUD_PROJECT must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	runID := uuid.New().String()[:8]

	topicName := "sm-it-topic-" + runID
	subName := "sm-it-sub-" + runID
	bucketName := "sm-it-bucket-" + runID

	cfg := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments:     map[string]servicemanager.EnvironmentSpec{"cloudtest": {ProjectID: projectID}},
		Resources: servicemanager.ResourcesSpec{
			GCSBuckets:      []servicemanager.GCSBucket{{Name: bucketName}},
			MessagingTopics: []servicemanager.MessagingTopicConfig{{Name: topicName}},
			MessagingSubscriptions: []servicemanager.MessagingSubscriptionConfig{
				{Name: subName, Topic: topicName, AckDeadlineSeconds: 25},
			},
		},
	}

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(cfg)
	require.NoError(t, err)

	// Use a more detailed logger for cloud tests to aid debugging.
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 1. Create real clients for the cloud test ---
	realGCSClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	defer realGCSClient.Close()

	realPSClient, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer realPSClient.Close()

	// --- 2. Create adapters and sub-managers ---
	gcsAdapter := servicemanager.NewGCSClientAdapter(realGCSClient)
	psAdapter := servicemanager.NewGoogleMessagingClientAdapter(realPSClient)
	mockBQClient := &servicemanager.MockBQClient{} // BQ is not under test

	// Create the MessagingManager that will be injected
	psManager, err := servicemanager.NewMessagingManager(psAdapter, logger)
	require.NoError(t, err)

	// --- 3. Create ServiceManager using the corrected constructor ---
	manager, err := servicemanager.NewServiceManagerFromSubManagers(psManager, gcsAdapter, mockBQClient, servicesDef, logger)
	require.NoError(t, err)

	// Teardown is deferred to ensure resources are cleaned up even if tests fail.
	defer func() {
		t.Log("--- Starting deferred teardown ---")
		err := manager.TeardownAll(ctx, cfg, "cloudtest")
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
		t.Log("--- Deferred teardown complete ---")
	}()

	// --- 4. Setup and Verify ---
	t.Run("Cloud_Setup_And_Verify", func(t *testing.T) {
		_, err := manager.SetupAll(ctx, cfg, "cloudtest")
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

		// Verify subscription config was applied
		subCfg, err := sub.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, 25*time.Second, subCfg.AckDeadline)
	})
}
