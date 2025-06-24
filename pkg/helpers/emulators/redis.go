package emulators

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
	"time"
)

const (
	// Redis Configuration (using a local container)
	cloudTestRedisImage = "redis:8.0.2-alpine"
	cloudTestRedisPort  = "6379/tcp"
)

func GetDefaultRedisImageContainer() ImageContainer {
	return ImageContainer{
		EmulatorImage: cloudTestRedisImage,
		EmulatorPort:  cloudTestRedisPort, // This is the primary port for Redis
	}
}

// SetupRedisContainer starts a Redis container and returns its connection information.
func SetupRedisContainer(t *testing.T, ctx context.Context, imageContainer ImageContainer) EmulatorConnectionInfo {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        imageContainer.EmulatorImage,
		ExposedPorts: []string{imageContainer.EmulatorPort},
		WaitingFor:   wait.ForListeningPort(nat.Port(imageContainer.EmulatorPort)).WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Redis container")
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
		t.Log("CLOUD E2E: Redis container terminated.")
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, nat.Port(imageContainer.EmulatorPort))
	require.NoError(t, err)

	redisAddr := fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("CLOUD E2E: Redis container started at: %s", redisAddr)

	return EmulatorConnectionInfo{
		EmulatorAddress: redisAddr, // Now explicitly set this field
		// HTTPEndpoint, GRPCEndpoint, EmulatorAddress, and ClientOptions are not applicable for Redis and remain zero-valued.
	}
}
