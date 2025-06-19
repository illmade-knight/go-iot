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
	"os"
	"testing"
	"time"
)

type GCSConfig struct {
	GCImageContainer
	BaseBucket  string
	BaseStorage string
}

func SetupGCSEmulator(t *testing.T, ctx context.Context, cfg GCSConfig) (*storage.Client, func()) {
	t.Helper()

	httpPort := fmt.Sprintf("%s/tcp", cfg.EmulatorHTTPPort)
	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{httpPort},
		Cmd:          []string{"-scheme", "http"},
		WaitingFor: wait.ForHTTP(cfg.BaseStorage).WithPort(nat.Port(httpPort)).WithStatusCodeMatcher(
			func(status int) bool {
				return status > 0
			}).WithStartupTimeout(20 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	t.Setenv("STORAGE_EMULATOR_HOST", endpoint)

	gcsClient, err := storage.NewClient(ctx, option.WithoutAuthentication(), option.WithEndpoint(os.Getenv("STORAGE_EMULATOR_HOST")))
	require.NoError(t, err)

	err = gcsClient.Bucket(cfg.BaseBucket).Create(ctx, cfg.ProjectID, nil)
	require.NoError(t, err)

	return gcsClient, func() {
		gcsClient.Close()
		require.NoError(t, container.Terminate(ctx))
	}
}
