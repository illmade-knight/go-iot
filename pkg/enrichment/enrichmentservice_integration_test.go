//go:build integration

package enrichment_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/enrichment"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"time"
)

// --- Integration Test ---

func TestEnrichmentService_Integration(t *testing.T) {
	// --- Test Setup ---
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// 1. Configure Pub/Sub resources for the test.
	const (
		projectID         = "test-project"
		inputTopicID      = "raw-messages-topic"
		inputSubID        = "enrichment-service-sub"
		outputTopicID     = "enriched-messages-topic"
		deadLetterTopicID = "enrichment-dead-letter-topic"
	)

	// 2. Set up the Pub/Sub emulator.
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		inputTopicID: inputSubID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	testClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer testClient.Close()

	outputTopic, err := testClient.CreateTopic(ctx, outputTopicID)
	require.NoError(t, err)
	dltTopic, err := testClient.CreateTopic(ctx, deadLetterTopicID)
	require.NoError(t, err)
	inputTopic := testClient.Topic(inputTopicID)

	// 3. Set up the mock metadata cache.
	metadataCache := NewInMemoryDeviceMetadataCache()
	metadataCache.AddDevice("DEVICE_EUI_001", "client-123", "location-abc", "temperature-sensor")

	// --- Instantiate Pipeline Components ---
	sharedClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer sharedClient.Close()

	consumer, err := messagepipeline.NewGooglePubsubConsumer(
		&messagepipeline.GooglePubsubConsumerConfig{ProjectID: projectID, SubscriptionID: inputSubID},
		sharedClient, logger,
	)
	require.NoError(t, err)

	// The main producer now expects the simplified types.PublishMessage.
	mainProducer, err := messagepipeline.NewGooglePubsubProducer[types.PublishMessage](
		sharedClient,
		&messagepipeline.GooglePubsubProducerConfig{ProjectID: projectID, TopicID: outputTopicID, BatchDelay: 20 * time.Millisecond},
		logger,
	)
	require.NoError(t, err)

	// Use the new SimplePublisher for the dead-letter topic.
	dltPublisher, err := messagepipeline.NewGoogleSimplePublisher(sharedClient, deadLetterTopicID, logger)
	require.NoError(t, err)
	defer dltPublisher.Stop()

	enricher := enrichment.NewMessageEnricher(metadataCache.Fetcher(), dltPublisher, logger)

	// The processing service is now generic over types.PublishMessage.
	processingService, err := messagepipeline.NewProcessingService(2, consumer, mainProducer, enricher, logger)
	require.NoError(t, err)

	// --- Run Test ---
	err = processingService.Start()
	require.NoError(t, err)
	defer processingService.Stop()

	// --- Test Case 1: Successful Enrichment ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		verifierSub, err := testClient.CreateSubscription(ctx, "verifier-sub-success", pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		defer verifierSub.Delete(ctx)

		originalPayload := `{"device_id": "DEVICE_EUI_001", "value": 25.5}`
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data:       []byte(originalPayload),
			Attributes: map[string]string{"uid": "DEVICE_EUI_001"},
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		receivedMsg := receiveSingleMessage(t, ctx, verifierSub, 5*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message on the output topic")

		// Verify the enriched message has the new DeviceInfo.
		var enrichedResult types.PublishMessage
		err = json.Unmarshal(receivedMsg.Data, &enrichedResult)
		require.NoError(t, err, "Failed to unmarshal enriched message")

		assert.Equal(t, "client-123", enrichedResult.DeviceInfo.Name)
		assert.Equal(t, "location-abc", enrichedResult.DeviceInfo.Location)
		assert.Equal(t, "temperature-sensor", enrichedResult.DeviceInfo.ServiceTag)
		assert.JSONEq(t, originalPayload, string(enrichedResult.Payload))
	})

	// --- Test Case 2: Device Not Found Sends to Dead-Letter Topic ---
	t.Run("Device Not Found Sends to Dead-Letter Topic", func(t *testing.T) {
		dltVerifierSub, err := testClient.CreateSubscription(ctx, "verifier-sub-dlt", pubsub.SubscriptionConfig{Topic: dltTopic})
		require.NoError(t, err)
		defer dltVerifierSub.Delete(ctx)

		originalPayload := `{"device_id": "UNKNOWN_DEVICE"}`
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data:       []byte(originalPayload),
			Attributes: map[string]string{"uid": "UNKNOWN_DEVICE"},
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		// Verify the original message arrives on the DLT.
		dltMsg := receiveSingleMessage(t, ctx, dltVerifierSub, 5*time.Second)
		require.NotNil(t, dltMsg, "Did not receive a message on the dead-letter topic")
		assert.JSONEq(t, originalPayload, string(dltMsg.Data))
		assert.Equal(t, "metadata_fetch_failed", dltMsg.Attributes["error"])
	})
}

// receiveSingleMessage is a helper to wait for exactly one message from a subscription.
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			receivedMsg = msg
			msg.Ack()
			receiveCancel()
		} else {
			msg.Nack()
		}
	})

	if err != nil && err != context.Canceled {
		t.Logf("Receive loop ended with error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}
