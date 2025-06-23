package messagepipeline

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// =============================================================================
//  Test Helpers
// =============================================================================

// setupTestPubsub initializes a pstest.Server and configures the necessary
// client options to connect to it. It also creates a topic and subscription for testing.
// It returns the client options and a cleanup function to be deferred by the caller.
func setupTestPubsub(t *testing.T, projectID, topicID, subID string) []option.ClientOption {
	t.Helper()
	ctx := context.Background()

	// Create a fake Pub/Sub server.
	srv := pstest.NewServer()

	// **THE FIX IS HERE:** Instead of manually creating a gRPC connection and sharing it,
	// we use option.WithEndpoint. This tells the pubsub.Client to create and manage its
	// own connection to the test server's address. This avoids the race condition
	// caused by multiple components trying to close the same shared connection.
	iopt := grpc.WithTransportCredentials(insecure.NewCredentials())
	opts := []option.ClientOption{
		option.WithEndpoint(srv.Addr),
		option.WithGRPCDialOption(iopt),
		option.WithoutAuthentication(),
	}

	// Use the options to create a temporary client for setting up topics/subscriptions.
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)

	// Create a topic on the test server.
	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)

	// Create a subscription to that topic.
	_, err = client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err)

	// The cleanup function now only needs to close the setup client and the server.
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		require.NoError(t, srv.Close())
	})

	return opts
}

// =============================================================================
//  Test Cases
// =============================================================================

// TestLoadGooglePubsubConsumerConfigFromEnv verifies the configuration loading logic.
func TestLoadGooglePubsubConsumerConfigFromEnv(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "test-project")
		t.Setenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT", "test-sub")

		cfg, err := LoadGooglePubsubConsumerConfigFromEnv()

		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "test-project", cfg.ProjectID)
		assert.Equal(t, "test-sub", cfg.SubscriptionID)
	})

	t.Run("missing project id", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "")
		t.Setenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT", "test-sub")

		cfg, err := LoadGooglePubsubConsumerConfigFromEnv()

		require.Error(t, err)
		assert.Nil(t, cfg)
		assert.Contains(t, err.Error(), "GCP_PROJECT_ID environment variable not set")
	})
}

// TestNewGooglePubsubConsumer_Success verifies the constructor logic for a successful case.
func TestNewGooglePubsubConsumer_Success(t *testing.T) {
	projectID := "test-project-success"
	topicID := "test-topic-success"
	subID := "test-sub-success"
	opts := setupTestPubsub(t, projectID, topicID, subID)

	cfg := &GooglePubsubConsumerConfig{
		ProjectID:      projectID,
		SubscriptionID: subID,
	}

	consumer, err := NewGooglePubsubConsumer(context.Background(), cfg, opts, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// The consumer's client must be closed to clean up its own connection.
	require.NoError(t, consumer.client.Close())
}

// TestNewGooglePubsubConsumer_SubscriptionNotFound verifies the constructor fails
// when a subscription does not exist.
func TestNewGooglePubsubConsumer_SubscriptionNotFound(t *testing.T) {
	projectID := "test-project-fail"
	topicID := "test-topic-fail"
	subID := "test-sub-fail"
	opts := setupTestPubsub(t, projectID, topicID, subID)

	badCfg := &GooglePubsubConsumerConfig{
		ProjectID:      projectID,
		SubscriptionID: "non-existent-sub",
	}

	consumer, err := NewGooglePubsubConsumer(context.Background(), badCfg, opts, zerolog.Nop())
	require.Error(t, err)
	assert.Nil(t, consumer)
	assert.Contains(t, err.Error(), "does not exist")
}

// TestGooglePubsubConsumer_Lifecycle_And_MessageReception tests the full flow:
// Start -> Receive Message -> Process -> Stop.
func TestGooglePubsubConsumer_Lifecycle_And_MessageReception(t *testing.T) {
	projectID := "test-project-lifecycle"
	topicID := "test-topic-lifecycle"
	subID := "test-sub-lifecycle"
	opts := setupTestPubsub(t, projectID, topicID, subID)

	cfg := &GooglePubsubConsumerConfig{
		ProjectID:              projectID,
		SubscriptionID:         subID,
		MaxOutstandingMessages: 1,
		NumGoroutines:          1,
	}

	client, err := pubsub.NewClient(context.Background(), projectID, opts...)
	require.NoError(t, err)
	defer client.Close()

	consumer, err := NewGooglePubsubConsumer(context.Background(), cfg, opts, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Start the consumer with a dedicated context for its operations.
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()
	err = consumer.Start(consumerCtx)
	require.NoError(t, err)

	topic := client.Topic(topicID)
	defer topic.Stop()

	msgPayload := []byte("hello world")
	msgAttributes := map[string]string{
		"uid":      "device-123",
		"location": "garden",
	}

	result := topic.Publish(context.Background(), &pubsub.Message{
		Data:       msgPayload,
		Attributes: msgAttributes,
	})
	_, err = result.Get(context.Background())
	require.NoError(t, err)

	var receivedMsg types.ConsumedMessage
	select {
	case receivedMsg = <-consumer.Messages():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	assert.Equal(t, msgPayload, receivedMsg.Payload)
	require.NotNil(t, receivedMsg.DeviceInfo)
	assert.Equal(t, "device-123", receivedMsg.DeviceInfo.UID)
	assert.Equal(t, "garden", receivedMsg.DeviceInfo.Location)
	receivedMsg.Ack()

	err = consumer.Stop()
	require.NoError(t, err)

	select {
	case <-consumer.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer to stop")
	}
}
