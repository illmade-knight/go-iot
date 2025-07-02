//go:build integration

package device_test // Changed package name

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/device" // Import the main package now
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// localDeviceMetadataCache is a test-local helper struct to mirror the JSON
// structure stored in Redis. This allows direct JSON unmarshalling in the test
// without exposing the private deviceMetadataCache struct from the 'device' package.
type localDeviceMetadataCache struct {
	ClientID   string `json:"clientID"`
	LocationID string `json:"locationID"`
	Category   string `json:"category"`
}

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
	expectedMetadata := localDeviceMetadataCache{ // Use the local helper struct
		ClientID:   "redis-client-1",
		LocationID: "redis-loc-1",
		Category:   "redis-cat-1",
	}
	expectedJSON, err := json.Marshal(expectedMetadata)
	require.NoError(t, err)

	// --- Create the Redis Fetcher ---
	// NewRedisDeviceMetadataFetcher now only takes ctx, cfg, logger
	redisConfig := &device.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute}
	fetcher, err := device.NewRedisDeviceMetadataFetcher(ctx, redisConfig, logger)
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
		fetchContext, cancelContext := context.WithTimeout(context.Background(), time.Second)
		defer cancelContext()
		clientID, locationID, category, err := fetcher.FetchFromCache(fetchContext, deviceEUI) // Use FetchFromCache
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

		t.Log("Acting: Fetching data, expecting a direct cache hit.")
		fetchContext, cancelContext := context.WithTimeout(context.Background(), time.Second)
		defer cancelContext()
		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss.")
		_, _, _, err := fetcher.FetchFromCache(fetchContext, deviceEUI) // Use FetchFromCache

		// --- Assert ---
		// Expected a cache miss error.
		require.Error(t, err, "Expected an error on a cache miss")
		assert.ErrorIs(t, err, device.ErrCacheMiss{EUI: deviceEUI}, "Error should be ErrCacheMiss") // Use device.ErrCacheMiss
		t.Log("Assertion successful: Correct 'Cache Miss' error received.")
	})

	// --- Test Case 3: Write to Cache ---
	t.Run("Write to Cache", func(t *testing.T) {
		writeEUI := "REDIS-WRITE-TEST-002"
		writeClientID := "write-client"
		writeLocationID := "write-loc"
		writeCategory := "write-cat"

		t.Log("Acting: Fetching data, expecting a direct cache hit.")
		fetchContext, cancelContext := context.WithTimeout(context.Background(), time.Second)
		defer cancelContext()
		// --- Act ---
		t.Logf("Acting: Writing data for EUI %s to Redis cache.", writeEUI)
		err := fetcher.WriteToCache(fetchContext, writeEUI, writeClientID, writeLocationID, writeCategory)
		require.NoError(t, err, "Expected no error writing to cache")

		// --- Assert ---
		// Verify data was written by reading directly from Redis
		cachedVal, err := redisClient.Get(ctx, writeEUI).Result()
		require.NoError(t, err, "Data should be directly readable from Redis after write")

		var cachedMetadata localDeviceMetadataCache // Use the local helper struct
		err = json.Unmarshal([]byte(cachedVal), &cachedMetadata)
		require.NoError(t, err, "Failed to unmarshal data read directly from Redis")

		assert.Equal(t, writeClientID, cachedMetadata.ClientID)
		assert.Equal(t, writeLocationID, cachedMetadata.LocationID)
		assert.Equal(t, writeCategory, cachedMetadata.Category)
		t.Logf("Assertion successful: Data for EUI %s confirmed in Redis after write.", writeEUI)
	})
}
