//go:build integration

package enrichment_test // Changed package to enrichment_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/enrichment" // Import main enrichment package
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
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
	require.NotEmpty(t, "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators", "Test requires Docker to be running.")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Use a verbose logger for tests to see detailed component logs.
	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// 1. Configure Pub/Sub topics and subscriptions for the test.
	const (
		projectID     = "test-project"
		inputTopicID  = "raw-messages-topic"
		inputSubID    = "enrichment-service-sub"
		outputTopicID = "enriched-messages-topic"
	)

	// 2. Set up the Pub/Sub emulator.
	// This will create the input topic and its corresponding subscription.
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		inputTopicID: inputSubID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	// Create a pubsub client that will be used for test setup and verification.
	publisherClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer publisherClient.Close()

	// The producer requires the output topic to exist before it starts.
	// We create it here manually. The test cases will create their own subscriptions to it.
	outputTopic := publisherClient.Topic(outputTopicID)
	exists, err := outputTopic.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		t.Logf("ENRICHMENT TEST [Setup]: Output topic '%s' does not exist, creating it.", outputTopicID)
		outputTopic, err = publisherClient.CreateTopic(ctx, outputTopicID)
		require.NoError(t, err)
	}
	inputTopic := publisherClient.Topic(inputTopicID)

	// 3. Set up the mock metadata cache and populate it with test data.
	// Using enrichment.NewInMemoryDeviceMetadataCache now
	metadataCache := enrichment.NewInMemoryDeviceMetadataCache()
	metadataCache.AddDevice("DEVICE_EUI_001", "client-123", "location-abc", "temperature-sensor")

	// --- Instantiate Pipeline Components ---

	// Shared client setup
	// The producer needs its own client instance, as its lifecycle is managed by the pipeline.
	sharedClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer sharedClient.Close()

	// Consumer: Reads from the input topic.
	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      projectID,
		SubscriptionID: inputSubID,
	}
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, sharedClient, logger)
	require.NoError(t, err)

	// Producer: Writes to the output topic.
	producerCfg := &messagepipeline.GooglePubsubProducerConfig{
		ProjectID:              projectID,
		TopicID:                outputTopicID,
		BatchDelay:             20 * time.Millisecond, // Use a short delay for testing.
		InputChannelMultiplier: 2,                     // Explicitly set to a positive value to avoid warning
	}

	// NOTE: The generic type for the producer is now our test-specific, data-only struct.
	producer, err := messagepipeline.NewGooglePubsubProducer[TestEnrichedMessage](sharedClient, producerCfg, logger)
	require.NoError(t, err)

	// Transformer: Use the corrected enricher function from enrichment package.
	// Using metadataCache.Fetcher() as the device.DeviceMetadataFetcher
	enricher := enrichment.NewTestMessageEnricher(metadataCache.Fetcher(), logger)

	// Processing Service: Ties all the components together.
	processingService, err := messagepipeline.NewProcessingService(
		2, // Number of workers
		consumer,
		producer,
		enricher,
		logger,
	)
	require.NoError(t, err)

	// --- Run Test Cases ---

	// Start the entire pipeline once for all sub-tests.
	err = processingService.Start()
	require.NoError(t, err)
	// Ensure the service is stopped gracefully at the end of all tests.
	defer processingService.Stop()

	// --- Test Case 1: Successful Enrichment ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		// Create a dedicated subscription for this test case to verify the output.
		outputSubID := "test-verifier-sub-success"
		outputSub, err := publisherClient.CreateSubscription(ctx, outputSubID, pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		defer outputSub.Delete(ctx)

		// Publish a test message that should be enriched successfully.
		t.Logf("ENRICHMENT TEST [Success]: Publishing message with UID 'DEVICE_EUI_001'")
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data: []byte(`{"value": 25.5}`),
			Attributes: map[string]string{
				"uid": "DEVICE_EUI_001",
			},
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		// --- Verification ---
		t.Log("ENRICHMENT TEST [Success]: Waiting for enriched message on output topic...")
		receivedMsg := receiveSingleMessage(t, ctx, outputSub, 15*time.Second)

		// Assert that we actually received a message.
		require.NotNil(t, receivedMsg, "Did not receive an enriched message on the output topic within the timeout.")

		// Unmarshal the result and check the enriched fields.
		var enrichedResult TestEnrichedMessage
		err = json.Unmarshal(receivedMsg.Data, &enrichedResult)
		require.NoError(t, err, "Failed to unmarshal enriched message from output")

		t.Logf("ENRICHMENT TEST [Success]: Received and unmarshalled enriched message: %+v", enrichedResult)

		// Assertions: Check if the message was correctly enriched.
		assert.Equal(t, "client-123", enrichedResult.ClientID, "ClientID was not enriched correctly.")
		assert.Equal(t, "location-abc", enrichedResult.LocationID, "LocationID was not enriched correctly.")
		assert.Equal(t, "temperature-sensor", enrichedResult.Category, "Category was not enriched correctly.")
		require.NotNil(t, enrichedResult.OriginalInfo, "OriginalInfo should be preserved.")
		assert.Equal(t, "DEVICE_EUI_001", enrichedResult.OriginalInfo.UID, "Original UID should be preserved.")
		// Check that the original payload is still there
		assert.JSONEq(t, `{"value": 25.5}`, string(enrichedResult.OriginalPayload))
	})

	// --- Test Case 2: Device Not Found ---
	t.Run("Device Not Found", func(t *testing.T) {
		outputSubID := "test-verifier-sub-notfound"
		outputSub, err := publisherClient.CreateSubscription(ctx, outputSubID, pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		defer outputSub.Delete(ctx)

		// Publish a message with a UID that does not exist in our cache.
		// This should cause the enricher to fail and the message to be NACKed.
		t.Logf("ENRICHMENT TEST [Not Found]: Publishing message with unknown UID 'DEVICE_EUI_UNKNOWN'")
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data: []byte(`{"value": 99.9}`),
			Attributes: map[string]string{
				"uid": "DEVICE_EUI_UNKNOWN",
			},
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		// --- Verification ---
		// We expect this message to be NACKed, so it should NOT appear on the output topic.
		t.Log("ENRICHMENT TEST [Not Found]: Waiting to confirm NO message arrives on output topic...")
		receivedMsg := receiveSingleMessage(t, ctx, outputSub, 5*time.Second) // Use a shorter timeout.

		// Assert that we received NOTHING.
		assert.Nil(t, receivedMsg, "A message was incorrectly published to the output topic for a non-existent device.")
		t.Log("ENRICHMENT TEST [Not Found]: Confirmed no message arrived, as expected.")
	})

	// --- Test Case 3: Message without UID Attribute ---
	t.Run("Message without UID Attribute", func(t *testing.T) {
		outputSubID := "test-verifier-sub-nouid"
		outputSub, err := publisherClient.CreateSubscription(ctx, outputSubID, pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		defer outputSub.Delete(ctx)

		// Publish a message with no "uid" attribute.
		// The enricher should skip the lookup and pass the message through unenriched.
		t.Logf("ENRICHMENT TEST [No UID]: Publishing message without UID attribute")
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data: []byte(`{"value": 101.0}`),
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		// --- Verification ---
		t.Log("ENRICHMENT TEST [No UID]: Waiting for pass-through message on output topic...")
		receivedMsg := receiveSingleMessage(t, ctx, outputSub, 15*time.Second)

		require.NotNil(t, receivedMsg, "Did not receive a pass-through message on the output topic within the timeout.")

		var unenrichedResult TestEnrichedMessage
		err = json.Unmarshal(receivedMsg.Data, &unenrichedResult)
		require.NoError(t, err, "Failed to unmarshal pass-through message from output")

		t.Logf("ENRICHMENT TEST [No UID]: Received and unmarshalled pass-through message: %+v", unenrichedResult)

		// Assertions: The enrichment fields should be empty.
		assert.Empty(t, unenrichedResult.ClientID, "ClientID should be empty for a message without a UID.")
		assert.Empty(t, unenrichedResult.LocationID, "LocationID should be empty.")
		assert.Empty(t, unenrichedResult.Category, "Category should be empty.")
		assert.Nil(t, unenrichedResult.OriginalInfo, "OriginalInfo should be nil as it wasn't in the original message attributes.")
	})
}

// receiveSingleMessage is a helper to wait for exactly one message from a subscription
// or return nil if the timeout is reached.
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	// Create a context for receiving the message, with the specified timeout.
	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	// Use a goroutine to receive from the output subscription.
	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			t.Logf("Helper: Received message ID %s from subscription %s.", msg.ID, sub.ID())
			receivedMsg = msg
			msg.Ack()
			receiveCancel() // Stop receiving once we have our message.
		} else {
			// This shouldn't happen if the test is structured correctly, but it's a good safeguard.
			t.Logf("Helper: Received an unexpected extra message (ID: %s), Nacking it.", msg.ID)
			msg.Nack()
		}
	})

	// Check if Receive exited due to context cancellation (which is expected on success or timeout)
	if err != nil && err != context.Canceled {
		t.Fatalf("Helper: Failed to receive from subscription %s: %v", sub.ID(), err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}
