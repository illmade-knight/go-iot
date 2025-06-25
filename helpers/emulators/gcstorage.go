package emulators

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"testing"
	"time"
)

const (
	testGCSImage = "fsouza/fake-gcs-server:latest"
	testGCSPort  = "4443"
)

type GCSConfig struct {
	GCImageContainer
	BaseBucket  string
	BaseStorage string
}

func GetDefaultGCSConfig(projectID, baseBucket string) GCSConfig {
	return GCSConfig{
		GCImageContainer: GCImageContainer{
			ImageContainer: ImageContainer{
				EmulatorImage: testGCSImage,
				EmulatorPort:  testGCSPort,
			},
			ProjectID: projectID,
		},
		BaseBucket:  baseBucket,
		BaseStorage: "/storage/v1/b",
	}
}

func GetStorageClient(t *testing.T, ctx context.Context, cfg GCSConfig, opts []option.ClientOption) *storage.Client {

	gcsClient, err := storage.NewClient(ctx, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, gcsClient.Close())
	})

	if cfg.BaseBucket != "" {
		err = gcsClient.Bucket(cfg.BaseBucket).Create(ctx, cfg.ProjectID, nil)
		require.NoError(t, err)
	}

	return gcsClient
}

// SetupGCSEmulator starts a GCS emulator container and configures it.
// It returns the connection information for connecting to the emulator.
func SetupGCSEmulator(t *testing.T, ctx context.Context, cfg GCSConfig) EmulatorConnectionInfo {
	t.Helper()

	httpPort := fmt.Sprintf("%s/tcp", cfg.EmulatorPort)
	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{httpPort},
		Cmd:          []string{"-scheme", "http"}, // Explicitly tell fake-gcs-server to use http
		WaitingFor: wait.ForHTTP(cfg.BaseStorage).WithPort(nat.Port(httpPort)).WithStatusCodeMatcher(
			func(status int) bool {
				return status > 0
			}).WithStartupTimeout(20 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	emulatorEndpoint, err := container.Endpoint(ctx, "")

	t.Setenv("STORAGE_EMULATOR_HOST", emulatorEndpoint)

	opts := getEmulatorOptions(emulatorEndpoint)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	return EmulatorConnectionInfo{
		HTTPEndpoint: Endpoint{
			Port:     cfg.EmulatorPort,
			Endpoint: emulatorEndpoint,
		},
		ClientOptions: opts,
	}
}
