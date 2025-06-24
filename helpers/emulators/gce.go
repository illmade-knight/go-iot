package emulators

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

const (
	// testEmulatorImage is the shared Google Cloud SDK emulator image for various services.
	testEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"

	// testPubsubEmulatorPort is the default port for the Pub/Sub emulator.
	testPubsubEmulatorPort = "8085"

	// testFirestoreEmulatorPort is the default port for the Firestore emulator.
	testFirestoreEmulatorPort = "8080"
)

// PubsubConfig holds configuration specific to the Pub/Sub emulator.
type PubsubConfig struct {
	GCImageContainer
	TopicSubs map[string]string // A map of topic names to subscription names to pre-create.
}

// FirestoreConfig holds configuration specific to the Firestore emulator.
type FirestoreConfig struct {
	GCImageContainer
	// Add any Firestore-specific configurations here if needed in the future.
	// For now, it primarily uses the common GCImageContainer properties.
}

// GetDefaultPubsubConfig provides a default configuration for the Pub/Sub emulator.
func GetDefaultPubsubConfig(projectID string, topicSubs map[string]string) PubsubConfig {
	return PubsubConfig{
		GCImageContainer: GCImageContainer{
			ImageContainer: ImageContainer{
				EmulatorImage: testEmulatorImage,
				EmulatorPort:  testPubsubEmulatorPort,
			},
			ProjectID: projectID,
		},
		TopicSubs: topicSubs,
	}
}

// GetDefaultFirestoreConfig provides a default configuration for the Firestore emulator.
func GetDefaultFirestoreConfig(projectID string) FirestoreConfig {
	return FirestoreConfig{
		GCImageContainer: GCImageContainer{
			ImageContainer: ImageContainer{
				EmulatorImage: testEmulatorImage,
				EmulatorPort:  testFirestoreEmulatorPort,
			},
			ProjectID: projectID,
		},
	}
}

// SetupPubsubEmulator starts a Pub/Sub emulator container and configures it.
// It returns client options for connecting to the emulator.
// CORRECTED: This function now uses t.Cleanup to manage the container lifecycle.
// Refactored to return EmulatorConnectionInfo.
func SetupPubsubEmulator(t *testing.T, ctx context.Context, cfg PubsubConfig) EmulatorConnectionInfo { // Changed return type
	t.Helper()

	httpPort := fmt.Sprintf("%s/tcp", cfg.EmulatorPort) // Use EmulatorPort for the exposed port
	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{httpPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", cfg.ProjectID), fmt.Sprintf("--host-port=0.0.0.0:%s", cfg.EmulatorPort)},
		WaitingFor:   wait.ForListeningPort(nat.Port(cfg.EmulatorPort)),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	// CORRECTED: Use t.Cleanup to ensure the container is terminated after the test and all its sub-tests complete.
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			log.Warn().Err(err).Msg("Failed to terminate Pub/Sub emulator container")
		}
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, nat.Port(cfg.EmulatorPort))
	require.NoError(t, err)
	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port()) //

	t.Logf("Pub/Sub emulator container started, listening on: %s", emulatorHost)
	// Removed t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost) as discussed

	clientOptions := getEmulatorOptions(emulatorHost)

	adminClient, err := pubsub.NewClient(ctx, cfg.ProjectID, clientOptions...)
	require.NoError(t, err)
	defer adminClient.Close()

	for topicName, subName := range cfg.TopicSubs {
		topic := adminClient.Topic(topicName)
		exists, err := topic.Exists(ctx)
		require.NoError(t, err)
		if !exists {
			_, err = adminClient.CreateTopic(ctx, topicName)
			require.NoError(t, err, "Failed to create Pub/Sub topic")
		}

		sub := adminClient.Subscription(subName)
		exists, err = sub.Exists(ctx)
		require.NoError(t, err)
		if !exists {
			_, err = adminClient.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{Topic: topic})
			require.NoError(t, err, "Failed to create Pub/Sub subscription")
		}
	}

	return EmulatorConnectionInfo{ // Populating the new struct
		HTTPEndpoint: Endpoint{
			Port:     cfg.EmulatorPort,
			Endpoint: emulatorHost,
		},
		ClientOptions: clientOptions,
	}
}

// SetupFirestoreEmulator starts a Firestore emulator container and configures it.
// It returns the client options for connecting to the emulator.
// CORRECTED: This function now uses t.Cleanup to manage the container lifecycle.
// Refactored to return EmulatorConnectionInfo.
func SetupFirestoreEmulator(t *testing.T, ctx context.Context, cfg FirestoreConfig) EmulatorConnectionInfo { // Changed return type
	t.Helper()

	httpPort := fmt.Sprintf("%s/tcp", cfg.EmulatorPort) // Use EmulatorPort for the exposed port
	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{httpPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "firestore", "start", fmt.Sprintf("--project=%s", cfg.ProjectID), fmt.Sprintf("--host-port=0.0.0.0:%s", cfg.EmulatorPort)},
		WaitingFor:   wait.ForListeningPort(nat.Port(cfg.EmulatorPort)),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	// CORRECTED: Use t.Cleanup to ensure the container is terminated after the test and all its sub-tests complete.
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			log.Warn().Err(err).Msg("Failed to terminate Firestore emulator container")
		}
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, nat.Port(cfg.EmulatorPort))
	require.NoError(t, err)
	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port()) //

	t.Logf("Firestore emulator container started, listening on: %s", emulatorHost)

	clientOptions := getEmulatorOptions(emulatorHost)

	return EmulatorConnectionInfo{ // Populating the new struct
		HTTPEndpoint: Endpoint{
			Port:     cfg.EmulatorPort,
			Endpoint: emulatorHost,
		},
		ClientOptions: clientOptions,
	}
}
