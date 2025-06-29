//go:build integration

package device_test // Changed package to device_test

import (
	"context"
	"encoding/json"
	"errors" // Keep errors import for errors.New in mock
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/device" // Import the main package now
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFallbackFetcher is a controllable mock for the DeviceMetadataFetcher.
// It allows us to simulate different outcomes from the fallback (e.g., success, not found).
// This mock aligns with the original DeviceMetadataFetcher signature (func(string) (string,string,string,error))
type mockFallbackFetcher struct {
	// Function to execute when called. This allows for dynamic behavior per test.
	fetchFunc func(deviceEUI string) (string, string, string, error)
	// Tracks how many times the fallback was invoked.
	callCount int
}

// Fetch executes the mock's fetchFunc and increments the call count.
func (m *mockFallbackFetcher) Fetch(deviceEUI string) (string, string, string, error) {
	m.callCount++
	if m.fetchFunc != nil {
		return m.fetchFunc(deviceEUI)
	}
	return "", "", "", errors.New("fetchFunc not defined for mock")
}

// Close is implemented to satisfy the io.Closer interface required by SourceFetcher.
func (m *mockFallbackFetcher) Close() error {
	// No-op for a simple mock
	return nil
}

func (m *mockFallbackFetcher) Reset() {
	m.callCount = 0
}

func TestRedisDeviceMetadataFetcher_Integration(t *testing.T) { // This test is for the RedisCacheFetcher directly
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
	// Use the device.deviceMetadataCache struct from the 'device' package.
	// NOTE: deviceMetadataCache is unexported in device/redisdevicecache.go.
	// For testing, we need a local mirrored struct or device.deviceMetadataCache to be exported.
	// Let's create a local mirrored struct for the purpose of unmarshalling.
	type localDeviceMetadataCache struct {
		ClientID   string `json:"clientID"`
		LocationID string `json:"locationID"`
		Category   string `json:"category"`
	}
	expectedMetadata := localDeviceMetadataCache{
		ClientID:   "redis-client-1",
		LocationID: "redis-loc-1",
		Category:   "redis-cat-1",
	}
	expectedJSON, err := json.Marshal(expectedMetadata)
	require.NoError(t, err)

	// --- Create the Redis CachedFetcher ---
	// NewRedisDeviceMetadataFetcher now takes ctx, RedisConfig, and logger.
	redisConfig := &device.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute}
	redisCacheFetcher, err := device.NewRedisDeviceMetadataFetcher(ctx, redisConfig, logger)
	require.NoError(t, err)
	defer redisCacheFetcher.Close() // Ensure the Redis CachedFetcher is closed.

	// --- Test Case 1: Data Found in Cache (FetchFromCache) ---
	t.Run("Cache Hit (FetchFromCache)", func(t *testing.T) {
		// --- Arrange ---
		// Manually place the data in the Redis cache.
		require.NoError(t, redisClient.Set(ctx, deviceEUI, expectedJSON, 0).Err())
		t.Logf("Arranged: Manually set key '%s' in Redis.", deviceEUI)

		// --- Act ---
		t.Log("Acting: Fetching data directly from RedisDeviceMetadataFetcher's cache.")
		// Call FetchFromCache which now takes context.
		clientID, locationID, category, err := redisCacheFetcher.FetchFromCache(ctx, deviceEUI)
		require.NoError(t, err)

		// --- Assert ---
		// Check that the data from the fetcher is correct.
		assert.Equal(t, expectedMetadata.ClientID, clientID)
		assert.Equal(t, expectedMetadata.LocationID, locationID)
		assert.Equal(t, expectedMetadata.Category, category)
		t.Log("Assertion successful: Data was fetched correctly from Redis cache.")
	})

	// --- Test Case 2: Data Not Found in Cache (FetchFromCache) ---
	t.Run("Cache Miss (FetchFromCache)", func(t *testing.T) {
		unknownEUI := "REDIS-INTEGRATION-TEST-UNKNOWN"
		// --- Arrange ---
		// Ensure the key does not exist in Redis.
		require.NoError(t, redisClient.Del(ctx, unknownEUI).Err())
		t.Logf("Arranged: Ensured key '%s' is deleted from Redis.", unknownEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss.")
		// Call FetchFromCache which now takes context.
		_, _, _, err := redisCacheFetcher.FetchFromCache(ctx, unknownEUI)

		// --- Assert ---
		// Expected a cache miss error.
		require.Error(t, err, "Expected an error on a cache miss")
		assert.ErrorIs(t, err, device.ErrCacheMiss{EUI: unknownEUI}, "Error should be ErrCacheMiss") // Use device.ErrCacheMiss
		t.Log("Assertion successful: Correct 'Cache Miss' error received.")
	})

	// --- Test Case 3: Write to Cache (WriteToCache) ---
	t.Run("Write to Cache (WriteToCache)", func(t *testing.T) {
		writeEUI := "REDIS-WRITE-TEST-002"
		writeClientID := "write-client"
		writeLocationID := "write-loc"
		writeCategory := "write-cat"

		// --- Arrange ---
		require.NoError(t, redisClient.Del(ctx, writeEUI).Err()) // Ensure key is clean
		t.Logf("Arranged: Ensured key '%s' is deleted from Redis for write test.", writeEUI)

		// --- Act ---
		t.Logf("Acting: Writing data for EUI %s to Redis cache using WriteToCache.", writeEUI)
		// Call WriteToCache which now takes context.
		err := redisCacheFetcher.WriteToCache(ctx, writeEUI, writeClientID, writeLocationID, writeCategory)
		require.NoError(t, err, "Expected no error writing to cache")

		// --- Assert ---
		// Verify data was written by reading directly from Redis
		cachedVal, err := redisClient.Get(ctx, writeEUI).Result()
		require.NoError(t, err, "Data should be directly readable from Redis after write")

		var cachedMetadata localDeviceMetadataCache
		err = json.Unmarshal([]byte(cachedVal), &cachedMetadata)
		require.NoError(t, err, "Failed to unmarshal data read directly from Redis")

		assert.Equal(t, writeClientID, cachedMetadata.ClientID)
		assert.Equal(t, writeLocationID, cachedMetadata.LocationID)
		assert.Equal(t, writeCategory, cachedMetadata.Category)
		t.Logf("Assertion successful: Data for EUI %s confirmed in Redis after write.", writeEUI)
	})
}
