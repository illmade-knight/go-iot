package device

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisDeviceMetadataFetcher_Integration_RedisOnly(t *testing.T) {
	// --- Test Setup ---
	require.NotEmpty(t, "docker", "This test requires Docker to be running.")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)

	// --- Start Redis Emulator ---
	redisEmulatorCfg := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, redisEmulatorCfg)
	redisClient := redis.NewClient(&redis.Options{Addr: redisConn.EmulatorAddress})
	require.NoError(t, redisClient.Ping(ctx).Err(), "could not connect to redis emulator")
	defer redisClient.Close()

	// --- Define Test Data ---
	deviceEUI := "REDIS-INTEGRATION-TEST-001"
	expectedMetadata := deviceMetadataCache{
		ClientID:   "redis-client-1",
		LocationID: "redis-loc-1",
		Category:   "redis-cat-1",
	}
	expectedJSON, err := json.Marshal(expectedMetadata)
	require.NoError(t, err)

	// --- Create the Redis Fetcher ---
	// For this test, we use a simple fallback that does nothing but return an error.
	// This allows us to test the Redis caching behavior in isolation.
	noopFallback := func(eui string) (string, string, string, error) {
		t.Logf("Fallback called for %s, returning 'Not Found'.", eui)
		return "", "", "", ErrMetadataNotFound
	}

	redisConfig := &RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute}
	fetcher, err := NewRedisDeviceMetadataFetcher(ctx, redisConfig, noopFallback, logger)
	require.NoError(t, err)
	defer fetcher.Close()

	// --- Test Case 1: Data Found in Cache ---
	t.Run("Cache Hit", func(t *testing.T) {
		// --- Arrange ---
		// Manually place the data in the Redis cache.
		require.NoError(t, redisClient.Set(ctx, deviceEUI, expectedJSON, 0).Err())
		t.Logf("Arranged: Manually set key '%s' in Redis.", deviceEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a direct cache hit.")
		clientID, locationID, category, err := fetcher.Fetch(deviceEUI)
		require.NoError(t, err)

		// --- Assert ---
		// Check that the data from the fetcher is correct.
		assert.Equal(t, expectedMetadata.ClientID, clientID)
		assert.Equal(t, expectedMetadata.LocationID, locationID)
		assert.Equal(t, expectedMetadata.Category, category)
		t.Log("Assertion successful: Data was fetched correctly from cache.")
	})

	// --- Test Case 2: Data Not Found in Cache ---
	t.Run("Cache Miss", func(t *testing.T) {
		// --- Arrange ---
		// Ensure the key does not exist in Redis.
		require.NoError(t, redisClient.Del(ctx, deviceEUI).Err())
		t.Logf("Arranged: Ensured key '%s' is deleted from Redis.", deviceEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss and fallback failure.")
		_, _, _, err := fetcher.Fetch(deviceEUI)

		// --- Assert ---
		// Because the cache misses and our simple fallback returns an error,
		// the final result should be that error.
		require.Error(t, err, "Expected an error on a cache miss with a failing fallback")
		assert.ErrorIs(t, err, ErrMetadataNotFound, "Error should be the one from the no-op fallback")
		t.Log("Assertion successful: Correct 'Not Found' error received.")
	})
}
