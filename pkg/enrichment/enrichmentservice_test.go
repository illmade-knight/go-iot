package enrichment_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/go-redis/redis/v8"
	"github.com/illmade-knight/go-iot/pkg/device" // Changed import path
	"github.com/illmade-knight/go-iot/pkg/enrichment"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline" // No change
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessageProcessingService_Enrichment_Integration tests the full enrichment pipeline
// using the generic MessageProcessingService.
func TestMessageProcessingService_Enrichment_Integration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := zerolog.New(io.Discard) // Mute logger for tests

	const (
		testProjectID           = "test-enrichment-project"
		firestoreCollectionName = "devices"
		inputSubscriptionID     = "test-input-sub"
		inputTopicID            = "test-input-topic"
		outputTopicID           = "test-enriched-output-topic"
	)

	// --- 1. Setup Emulators ---
	// Firestore Emulator
	firestoreEmulatorCfg := emulators.GetDefaultFirestoreConfig(testProjectID)
	firestoreConnection := emulators.SetupFirestoreEmulator(t, ctx, firestoreEmulatorCfg)
	firestoreClient, err := firestore.NewClient(ctx, testProjectID, firestoreConnection.ClientOptions...)
	require.NoError(t, err, "Failed to create Firestore client")
	t.Cleanup(func() { firestoreClient.Close() })

	// Pub/Sub Emulator
	topicSubs := map[string]string{inputTopicID: inputSubscriptionID, outputTopicID: ""}
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(testProjectID, topicSubs)
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	pubsubClient, err := pubsub.NewClient(ctx, testProjectID, pubsubConnection.ClientOptions...)
	require.NoError(t, err, "Failed to create Pub/Sub client")
	t.Cleanup(func() { pubsubClient.Close() })

	// Redis Client (direct connection)
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	require.NoError(t, redisClient.Ping(ctx).Err(), "Failed to connect to Redis")
	t.Cleanup(func() { redisClient.Close() })
	redisClient.FlushDB(ctx).Err() // Flush Redis for a clean state per test run

	// Set emulator environment variables for the device package where they might be loaded
	os.Setenv("GCP_PROJECT_ID", testProjectID)
	os.Setenv("FIRESTORE_COLLECTION_DEVICES", firestoreCollectionName)
	os.Setenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT", inputSubscriptionID)
	os.Setenv("PUBSUB_TOPIC_ID_ENRICHED_OUTPUT", outputTopicID)
	os.Setenv("REDIS_ADDR", redisAddr)
	os.Setenv("REDIS_CACHE_TTL", "1s")

	// --- 2. Prepare Data in Firestore (Device Metadata) ---
	deviceEUI := "test-device-001"
	expectedClientID := "client-A"
	expectedLocationID := "location-X"
	expectedCategory := "sensor"

	_, err = firestoreClient.Collection(firestoreCollectionName).Doc(deviceEUI).Set(ctx, map[string]interface{}{
		"clientID":       expectedClientID,
		"locationID":     expectedLocationID,
		"deviceCategory": expectedCategory,
	})
	require.NoError(t, err, "Failed to set up Firestore test data")
	time.Sleep(100 * time.Millisecond) // Give Firestore a moment

	// --- 3. Assemble the MessageProcessingService ---
	// Pub/Sub Consumer
	consumerCfg, err := messagepipeline.LoadGooglePubsubConsumerConfigFromEnv()
	require.NoError(t, err)
	pubsubConsumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, pubsubConnection.ClientOptions, logger)
	require.NoError(t, err, "Failed to create Pub/Sub consumer")
	defer pubsubConsumer.Stop()

	// Device Metadata Fetcher (using Redis cache with Firestore fallback)
	redisCfg, err := LoadRedisConfigFromEnv() // Using local helper for RedisConfig
	require.NoError(t, err)

	firestoreFetcherCfg, err := device.LoadFirestoreFetcherConfigFromEnv() // Using device.LoadFirestoreFetcherConfigFromEnv
	require.NoError(t, err)

	// Create Firestore fallback fetcher directly
	firestoreFallbackFetcher, err := device.NewGoogleDeviceMetadataFetcher(ctx, firestoreClient, firestoreFetcherCfg, logger) // Using device.NewGoogleDeviceMetadataFetcher
	require.NoError(t, err, "Failed to create Firestore fallback fetcher")
	defer firestoreFallbackFetcher.Close()

	// Create Redis caching fetcher with the Firestore fallback
	redisCachingFetcher, err := device.NewRedisDeviceMetadataFetcher(ctx, redisCfg, firestoreFallbackFetcher.Fetch, logger) // Using device.NewRedisDeviceMetadataFetcher
	require.NoError(t, err, "Failed to create Redis caching fetcher")
	defer redisCachingFetcher.Close()

	deviceMetadataFetcher := redisCachingFetcher.Fetch // This is now your DeviceMetadataFetcher func

	// Message Enricher (Transformer) - now a function
	messageEnricherTransformer := enrichment.NewMessageEnricher(deviceMetadataFetcher, logger)

	// Pub/Sub Producer (Processor) - now implements new MessageProcessor interface
	producerCfg, err := messagepipeline.LoadGooglePubsubProducerConfigFromEnv()
	require.NoError(t, err)
	pubsubProducer, err := messagepipeline.NewGooglePubsubProducer[enrichment.EnrichedMessage](
		ctx,
		pubsubClient,
		producerCfg,
		logger,
	)
	require.NoError(t, err, "Failed to create Pub/Sub producer")

	// MessageProcessingService
	processingService, err := messagepipeline.NewProcessingService[enrichment.EnrichedMessage](
		1, // numWorkers
		pubsubConsumer,
		messageEnricherTransformer, // Pass the transformer function
		logger,
	)
	require.NoError(t, err, "Failed to create message processing service")

	// Start the service
	err = processingService.Start() // Call Start() without context
	require.NoError(t, err, "Failed to start message processing service")
	defer processingService.Stop() // Call Stop() on the service

	// Give services a moment to spin up
	time.Sleep(1 * time.Second)

	// --- 4. Publish a Test Message to the Input Topic ---
	originalPayload := []byte(`{"temperature": 25.5, "humidity": 60}`)
	inputMessage := &pubsub.Message{
		Data: originalPayload,
		Attributes: map[string]string{
			"uid":      deviceEUI,
			"location": "original-location-attr",
		},
	}

	inputTopic.Publish(ctx, inputMessage).Get(ctx) // Block until published
	t.Log("Published test message to input topic")

	// Give the system a moment to process (especially important for async Pub/Sub receives)
	time.Sleep(2 * time.Second) // Increased sleep to ensure processing and flushing

	// --- 5. Consume from the Output Topic and Assert Enrichment ---
	// Create a new subscription to the output topic for testing purposes
	testOutputSubscriptionID := "test-output-sub-" + fmt.Sprintf("%d", time.Now().UnixNano())
	testOutputSub, err := pubsubClient.CreateSubscription(ctx, testOutputSubscriptionID, pubsub.SubscriptionConfig{
		Topic:       outputTopic,
		AckDeadline: 10 * time.Second,
	})
	require.NoError(t, err, "Failed to create test output subscription")
	t.Cleanup(func() {
		testOutputSub.Delete(context.Background()) // Clean up the test subscription
	})

	received := make(chan enrichment.EnrichedMessage, 1)
	receiveCtx, receiveCancel := context.WithTimeout(ctx, 10*time.Second) // Timeout for receiving
	defer receiveCancel()

	go func() { // Run Receive in a goroutine
		err := testOutputSub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			var enrichedMsg enrichment.EnrichedMessage
			if unmarshalErr := json.Unmarshal(msg.Data, &enrichedMsg); unmarshalErr != nil {
				t.Errorf("Failed to unmarshal received message: %v", unmarshalErr)
				msg.Nack()
				return
			}
			received <- enrichedMsg
			msg.Ack()
		})
		if err != nil && err != context.Canceled {
			t.Logf("Output subscription Receive exited with error: %v", err)
		}
	}()

	select {
	case enriched := <-received:
		t.Log("Received enriched message from output topic")
		assert.Equal(t, string(originalPayload), string(enriched.Payload), "Original payload should match")
		assert.Equal(t, expectedClientID, enriched.ClientID, "Client ID should be enriched")
		assert.Equal(t, expectedLocationID, enriched.LocationID, "Location ID should be enriched")
		assert.Equal(t, expectedCategory, enriched.Category, "Category should be enriched")
		assert.Equal(t, deviceEUI, enriched.ConsumedMessage.DeviceInfo.UID, "Device UID should be present")
		assert.Equal(t, "original-location-attr", enriched.ConsumedMessage.DeviceInfo.Location, "Original Pub/Sub attribute location should persist if not explicitly overwritten")

	case <-receiveCtx.Done():
		t.Fatalf("Timeout waiting for enriched message: %v", receiveCtx.Err())
	}

	t.Log("Enrichment service integration test completed successfully.")
}
