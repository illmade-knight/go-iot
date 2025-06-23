package emulators

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSetupRedisContainer(t *testing.T) {
	t.Parallel() // Allow tests to run in parallel

	// Use a context with timeout for *test operations*, not container lifecycle.
	testCtx, testCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer testCancel()

	cfg := GetDefaultRedisImageContainer()
	// Pass context.Background() to SetupRedisContainer for container lifecycle
	// This ensures the container termination is not prematurely canceled by testCtx.
	connInfo := SetupRedisContainer(t, context.Background(), cfg)

	// --- Verify EmulatorConnectionInfo ---
	if connInfo.EmulatorAddress == "" { // Now check the dedicated RedisAddr field
		t.Error("RedisAddr is empty")
	}

	// --- Test Connectivity ---
	redisAddr := connInfo.EmulatorAddress // Retrieve the address from the new field
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0, // use default DB
	})

	defer func() {
		if err := rdb.Close(); err != nil {
			t.Logf("Error closing Redis client: %v", err)
		}
	}()

	pong, err := rdb.Ping(testCtx).Result() // Use testCtx for client operation
	require.NoError(t, err, "Failed to ping Redis")
	require.Equal(t, "PONG", pong, "Expected PONG from Redis")

	key := "testkey"
	value := "testvalue"
	err = rdb.Set(testCtx, key, value, 0).Err() // Use testCtx for client operation
	require.NoError(t, err, "Failed to set Redis key")

	retrievedValue, err := rdb.Get(testCtx, key).Result() // Use testCtx for client operation
	require.NoError(t, err, "Failed to get Redis key")
	require.Equal(t, value, retrievedValue, "Retrieved value mismatch")

	t.Logf("Redis emulator test passed. Connected to: %s", redisAddr)
}

func TestGetDefaultRedisImageContainer(t *testing.T) {
	cfg := GetDefaultRedisImageContainer()

	if cfg.EmulatorImage != cloudTestRedisImage {
		t.Errorf("Expected image %q, got %q", cloudTestRedisImage, cfg.EmulatorImage)
	}
	if cfg.EmulatorPort != cloudTestRedisPort {
		t.Errorf("Expected port %q, got %q", cloudTestRedisPort, cfg.EmulatorPort)
	}
}
