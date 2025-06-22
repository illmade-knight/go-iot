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
	"google.golang.org/api/option"
	"testing"
)

const (
	testPubsubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubsubEmulatorPort  = "8085"
)

type PubsubConfig struct {
	GCImageContainer
	TopicSubs map[string]string
}

func GetDefaultPubsubConfig(projectID string, topicSubs map[string]string) PubsubConfig {
	return PubsubConfig{
		GCImageContainer: GCImageContainer{
			ImageContainer: ImageContainer{
				EmulatorImage:    testPubsubEmulatorImage,
				EmulatorHTTPPort: testPubsubEmulatorPort,
			},
			ProjectID: projectID,
		},
		TopicSubs: topicSubs,
	}
}

/*
Update: prefer port startup to wait.ForLog - the default is 8085
*/
func SetupPubsubEmulator(t *testing.T, ctx context.Context, cfg PubsubConfig) (clientOptions []option.ClientOption, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", cfg.EmulatorHTTPPort)},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", cfg.ProjectID), fmt.Sprintf("--host-port=0.0.0.0:%s", cfg.EmulatorHTTPPort)},
		WaitingFor:   wait.ForListeningPort(nat.Port(cfg.EmulatorHTTPPort)),
		//WaitingFor: wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(20 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, nat.Port(cfg.EmulatorHTTPPort))
	require.NoError(t, err)
	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port())

	t.Logf("Pub/Sub emulator container started, listening on: %s", emulatorHost)
	if true {
		t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	}
	clientOptions = []option.ClientOption{option.WithEndpoint(emulatorHost), option.WithoutAuthentication()}

	adminClient, err := pubsub.NewClient(ctx, cfg.ProjectID, clientOptions...)
	require.NoError(t, err)
	defer adminClient.Close()

	for k, v := range cfg.TopicSubs {
		topic := adminClient.Topic(k)
		exists, err := topic.Exists(ctx)
		require.NoError(t, err)
		if !exists {
			_, err = adminClient.CreateTopic(ctx, k)
			require.NoError(t, err, "Failed to create Pub/Sub topic")
		}

		sub := adminClient.Subscription(v)
		exists, err = sub.Exists(ctx)
		require.NoError(t, err)
		if !exists {
			_, err = adminClient.CreateSubscription(ctx, v, pubsub.SubscriptionConfig{Topic: topic})
			require.NoError(t, err, "Failed to create Pub/Sub subscription")
		}
	}

	return clientOptions, func() {
		err := container.Terminate(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to terminate Pub/Sub emulator container")
		}
	}
}
