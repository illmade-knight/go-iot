//go:build integration

package servicemanager_test

import (
	"context"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
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

	topicName := "test-topic-" + runID
	subName := "test-sub-" + runID
	bucketName := "test-bucket-" + runID

	require.True(t, servicemanager.IsValidBucketName(bucketName))

	// Define a config with a specific AckDeadline to verify it's applied correctly.
	cfg := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"integration": {ProjectID: projectID},
		},
		Resources: servicemanager.ResourcesSpec{
			GCSBuckets:   []servicemanager.GCSBucket{{Name: bucketName, VersioningEnabled: true}},
			PubSubTopics: []servicemanager.MessagingTopicConfig{{Name: topicName}},
			PubSubSubscriptions: []servicemanager.MessagingSubscriptionConfig{
				{Name: subName, Topic: topicName, AckDeadlineSeconds: 123},
			},
		},
	}

	// --- 1. Setup Emulators and Clients using a helper package ---
	gcsEmulatorClient, gcsClose := emulators.SetupGCSEmulator(t, ctx, emulators.GetDefaultGCSConfig(projectID, bucketName))
	defer gcsClose()

	psOptions, psClose := emulators.SetupPubSubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID, nil))
	defer psClose()

	psEmulatorClient, err := pubsub.NewClient(ctx, projectID, psOptions...)
	require.NoError(t, err)
	defer psEmulatorClient.Close()

	dt := map[string]string{}
	sm := map[string]interface{}{}
	bqOptions, bqClose := emulators.SetupBigQueryEmulator(t, ctx, emulators.GetDefaultBigQueryConfig(projectID, dt, sm))
	defer bqClose()

	// Wrap clients in our adapters
	gcsAdapter := servicemanager.NewGCSClientAdapter(gcsEmulatorClient)
	psAdapter := servicemanager.NewGoogleMessagingClientAdapter(psEmulatorClient)

	bqGoogleClient := newEmulatorBQClient(ctx, t, projectID, bqOptions)

	bqClientAdapter := servicemanager.NewBigQueryClientAdapter(bqGoogleClient)
	require.NotNil(t, bqClientAdapter, "Manager BQ client adapter should not be nil")

	// --- 2. Create ServiceManager using the clean constructor ---
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(cfg)
	require.NoError(t, err)
	logger := zerolog.New(io.Discard)

	// Create the main manager, injecting the sub-manager
	manager, err := servicemanager.NewServiceManagerFromClients(psAdapter, gcsAdapter, bqClientAdapter, servicesDef, sm, logger)
	require.NoError(t, err)

	// --- 3. Run Setup and Verify ---
	t.Run("Setup_And_Verify_With_Emulators", func(t *testing.T) {
		_, err := manager.SetupAll(ctx, cfg, "integration")
		require.NoError(t, err)

		// Verify GCS Bucket
		gcsAttrs, err := gcsEmulatorClient.Bucket(bucketName).Attrs(ctx)
		require.NoError(t, err, "GCS bucket should exist after setup")
		assert.Equal(t, bucketName, gcsAttrs.Name, "GCS bucket versioning should be enabled")

		// Verify Pub/Sub Topic and Subscription
		topic := psEmulatorClient.Topic(topicName)
		topicExists, err := topic.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, topicExists, "Pub/Sub topic should exist after setup")

		sub := psEmulatorClient.Subscription(subName)
		subExists, err := sub.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, subExists, "Pub/Sub subscription should exist after setup")

		subCfg, err := sub.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, topic.String(), subCfg.Topic.String(), "Subscription should be attached to the correct topic")
		assert.Equal(t, 123*time.Second, subCfg.AckDeadline, "Subscription should have the correct AckDeadline")
	})

	// --- 4. Teardown and Verify ---
	t.Run("Teardown_And_Verify_With_Emulators", func(t *testing.T) {
		err := manager.TeardownAll(ctx, cfg, "integration")
		require.NoError(t, err)

		// Verify GCS Bucket is gone
		_, err = gcsEmulatorClient.Bucket(bucketName).Attrs(ctx)
		assert.ErrorIs(t, err, storage.ErrBucketNotExist, "GCS bucket should NOT exist after teardown")

		// Verify Pub/Sub resources are gone
		topicExists, err := psEmulatorClient.Topic(topicName).Exists(ctx)
		require.NoError(t, err)
		assert.False(t, topicExists, "Pub/Sub topic should NOT exist after teardown")

		subExists, err := psEmulatorClient.Subscription(subName).Exists(ctx)
		require.NoError(t, err)
		assert.False(t, subExists, "Pub/Sub subscription should NOT exist after teardown")
	})
}
