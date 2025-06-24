package emulators

import (
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSetupPubsubEmulator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	projectID := "test-project-pubsub"
	topicName := "test-topic"
	subName := "test-subscription"
	cfg := GetDefaultPubsubConfig(projectID, map[string]string{topicName: subName})

	connInfo := SetupPubsubEmulator(t, ctx, cfg)

	// --- Verify EmulatorConnectionInfo ---
	if connInfo.HTTPEndpoint.Endpoint == "" {
		t.Error("HTTPEndpoint.Endpoint is empty")
	}
	if connInfo.HTTPEndpoint.Port == "" {
		t.Error("HTTPEndpoint.Port is empty")
	}
	if len(connInfo.ClientOptions) == 0 {
		t.Error("ClientOptions are empty")
	}

	// --- Test Connectivity ---
	client, err := pubsub.NewClient(ctx, projectID, connInfo.ClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer client.Close()

	// Verify topic and subscription exist (should be pre-created by SetupPubsubEmulator)
	topic := client.Topic(topicName)
	exists, err := topic.Exists(ctx)
	if err != nil {
		t.Fatalf("Failed to check if topic %q exists: %v", topicName, err)
	}
	if !exists {
		t.Errorf("Topic %q does not exist", topicName)
	}

	sub := client.Subscription(subName)
	exists, err = sub.Exists(ctx)
	if err != nil {
		t.Fatalf("Failed to check if subscription %q exists: %v", subName, err)
	}
	if !exists {
		t.Errorf("Subscription %q does not exist", subName)
	}

	t.Logf("Pub/Sub emulator test passed. Connected to: %s", connInfo.HTTPEndpoint.Endpoint)
}

func TestSetupFirestoreEmulator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	projectID := "test-project-firestore"
	cfg := GetDefaultFirestoreConfig(projectID)

	connInfo := SetupFirestoreEmulator(t, ctx, cfg)

	// --- Verify EmulatorConnectionInfo ---
	if connInfo.HTTPEndpoint.Endpoint == "" {
		t.Error("HTTPEndpoint.Endpoint is empty")
	}
	if connInfo.HTTPEndpoint.Port == "" {
		t.Error("HTTPEndpoint.Port is empty")
	}
	if len(connInfo.ClientOptions) == 0 {
		t.Error("ClientOptions are empty")
	}

	// --- Test Connectivity ---
	client, err := firestore.NewClient(ctx, projectID, connInfo.ClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create Firestore client: %v", err)
	}
	defer client.Close()

	// Try to add a dummy document
	_, _, err = client.Collection("testCollection").Add(ctx, map[string]interface{}{
		"field1": "value1",
		"field2": 123,
	})
	if err != nil {
		t.Fatalf("Failed to add document to Firestore: %v", err)
	}

	t.Logf("Firestore emulator test passed. Connected to: %s", connInfo.HTTPEndpoint.Endpoint)
}

func TestGetDefaultPubsubConfig(t *testing.T) {
	projectID := "default-pubsub-proj"
	topicSubs := map[string]string{"topicA": "subA"}
	cfg := GetDefaultPubsubConfig(projectID, topicSubs)

	if cfg.EmulatorImage != testEmulatorImage {
		t.Errorf("Expected image %q, got %q", testEmulatorImage, cfg.EmulatorImage)
	}
	if cfg.EmulatorPort != testPubsubEmulatorPort {
		t.Errorf("Expected HTTP port %q, got %q", testPubsubEmulatorPort, cfg.EmulatorPort)
	}
	if cfg.ProjectID != projectID {
		t.Errorf("Expected project ID %q, got %q", projectID, cfg.ProjectID)
	}
	if !reflect.DeepEqual(cfg.TopicSubs, topicSubs) {
		t.Errorf("TopicSubs mismatch: expected %v, got %v", topicSubs, cfg.TopicSubs)
	}
}

func TestGetDefaultFirestoreConfig(t *testing.T) {
	projectID := "default-firestore-proj"
	cfg := GetDefaultFirestoreConfig(projectID)

	if cfg.EmulatorImage != testEmulatorImage {
		t.Errorf("Expected image %q, got %q", testEmulatorImage, cfg.EmulatorImage)
	}
	if cfg.EmulatorPort != testFirestoreEmulatorPort {
		t.Errorf("Expected HTTP port %q, got %q", testFirestoreEmulatorPort, cfg.EmulatorPort)
	}
	if cfg.ProjectID != projectID {
		t.Errorf("Expected project ID %q, got %q", projectID, cfg.ProjectID)
	}
}
