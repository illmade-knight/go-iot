package enrichment

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/types"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Corrected Struct Definitions for Test ---
// The original `EnrichedMessage` embeds `types.ConsumedMessage`, which contains
// non-serializable func() fields (Ack/Nack), causing the JSON error.
// The fix is to define a new struct for the payload that only contains data.

// TestEnrichedMessage is a data-only representation of the final message payload.
// This is the struct that will be serialized to JSON and published.
type TestEnrichedMessage struct {
	OriginalPayload json.RawMessage   `json:"originalPayload"`
	OriginalInfo    *types.DeviceInfo `json:"originalInfo"`
	PublishTime     time.Time         `json:"publishTime"`
	ClientID        string            `json:"clientID,omitempty"`
	LocationID      string            `json:"locationID,omitempty"`
	Category        string            `json:"category,omitempty"`
}

// NewTestMessageEnricher creates the MessageTransformer for the test.
// It correctly separates the data payload from the message's Ack/Nack functions.
func NewTestMessageEnricher(fetcher device.DeviceMetadataFetcher, logger zerolog.Logger) messagepipeline.MessageTransformer[TestEnrichedMessage] {
	return func(msg types.ConsumedMessage) (*TestEnrichedMessage, bool, error) {
		// Start with the basic, un-enriched data.
		payload := &TestEnrichedMessage{
			OriginalPayload: msg.Payload,
			OriginalInfo:    msg.DeviceInfo,
			PublishTime:     msg.PublishTime,
		}

		// If there's no device info, we can't enrich, but we still pass the message on.
		if msg.DeviceInfo == nil || msg.DeviceInfo.UID == "" {
			logger.Warn().Str("msg_id", msg.ID).Msg("Message has no device UID for enrichment, skipping lookup.")
			return payload, false, nil
		}

		// Attempt to fetch metadata for enrichment.
		deviceEUI := msg.DeviceInfo.UID
		clientID, locationID, category, err := fetcher(deviceEUI)
		if err != nil {
			logger.Error().Err(err).Str("device_eui", deviceEUI).Str("msg_id", msg.ID).Msg("Failed to fetch device metadata for enrichment.")
			// We NACK by returning an error, which stops this message from being published.
			return nil, false, fmt.Errorf("failed to enrich message for EUI %s: %w", deviceEUI, err)
		}

		// If successful, add the enriched data to the payload.
		payload.ClientID = clientID
		payload.LocationID = locationID
		payload.Category = category

		logger.Debug().Str("msg_id", msg.ID).Str("device_eui", deviceEUI).Msg("Message enriched with device metadata.")
		return payload, false, nil
	}
}

// --- Mock Device Metadata Cache ---

// InMemoryDeviceMetadataCache provides a mock implementation of the DeviceMetadataFetcher
// for testing purposes. It uses a simple map to store device metadata.
type InMemoryDeviceMetadataCache struct {
	mu       sync.RWMutex
	metadata map[string]struct {
		ClientID   string
		LocationID string
		Category   string
	}
}

// NewInMemoryDeviceMetadataCache creates a new, empty in-memory cache.
func NewInMemoryDeviceMetadataCache() *InMemoryDeviceMetadataCache {
	return &InMemoryDeviceMetadataCache{
		metadata: make(map[string]struct {
			ClientID   string
			LocationID string
			Category   string
		}),
	}
}

// AddDevice populates the cache with metadata for a given device EUI.
func (c *InMemoryDeviceMetadataCache) AddDevice(eui, clientID, locationID, category string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metadata[eui] = struct {
		ClientID   string
		LocationID string
		Category   string
	}{
		ClientID:   clientID,
		LocationID: locationID,
		Category:   category,
	}
}

// Fetcher returns a function that conforms to the device.DeviceMetadataFetcher interface.
// This is the function that will be passed to the NewMessageEnricher.
func (c *InMemoryDeviceMetadataCache) Fetcher() device.DeviceMetadataFetcher {
	return func(deviceEUI string) (clientID, locationID, category string, err error) {
		c.mu.RLock()
		defer c.mu.RUnlock()

		data, ok := c.metadata[deviceEUI]
		if !ok {
			// Return the specific error type that the main service expects.
			return "", "", "", fmt.Errorf("%w: EUI %s not in cache", device.ErrMetadataNotFound, deviceEUI)
		}
		return data.ClientID, data.LocationID, data.Category, nil
	}
}

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
	metadataCache := NewInMemoryDeviceMetadataCache()
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
		ProjectID:  projectID,
		TopicID:    outputTopicID,
		BatchDelay: 20 * time.Millisecond, // Use a short delay for testing.
	}

	// NOTE: The generic type for the producer is now our test-specific, data-only struct.
	producer, err := messagepipeline.NewGooglePubsubProducer[TestEnrichedMessage](sharedClient, producerCfg, logger)
	require.NoError(t, err)

	// Transformer: Use the corrected enricher function.
	enricher := NewTestMessageEnricher(metadataCache.Fetcher(), logger)

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
		assert.Nil(t, unenrichedResult.OriginalInfo, "DeviceInfo should be nil as it wasn't in the original message attributes.")
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
