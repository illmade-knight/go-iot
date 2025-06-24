package device

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFallbackFetcher is a controllable mock for the DeviceMetadataFetcher.
// It allows us to simulate different outcomes from the fallback (e.g., success, not found).
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

func (m *mockFallbackFetcher) Reset() {
	m.callCount = 0
}

func TestRedisDeviceMetadataFetcher_Integration(t *testing.T) {
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
	// The mock fallback will be configured per test case.
	mockFallback := &mockFallbackFetcher{}
	redisConfig := &RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute}
	// Note: We are creating the fetcher with a real Redis client, not a mock.
	fetcher, err := NewRedisDeviceMetadataFetcher(ctx, redisConfig, mockFallback.Fetch, logger)
	require.NoError(t, err)
	defer fetcher.Close()

	// --- Test Case 1: Cache Miss, Successful Fallback, and Write-back ---
	t.Run("Cache Miss then Successful Fallback", func(t *testing.T) {
		// --- Arrange ---
		mockFallback.Reset()
		mockFallback.fetchFunc = func(eui string) (string, string, string, error) {
			assert.Equal(t, deviceEUI, eui)
			return expectedMetadata.ClientID, expectedMetadata.LocationID, expectedMetadata.Category, nil
		}
		// Ensure Redis is clean for this test
		require.NoError(t, redisClient.Del(ctx, deviceEUI).Err())
		t.Logf("Arranged: Ensured Redis is clean for EUI %s", deviceEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss followed by a fallback hit")
		clientID, locationID, category, err := fetcher.Fetch(deviceEUI)
		require.NoError(t, err)

		// --- Assert ---
		// 1. Check that the data from the fetcher is correct
		assert.Equal(t, expectedMetadata.ClientID, clientID)
		assert.Equal(t, expectedMetadata.LocationID, locationID)
		assert.Equal(t, expectedMetadata.Category, category)
		assert.Equal(t, 1, mockFallback.callCount, "Fallback should have been called once")

		// 2. Check that data was written back to the Redis cache
		cachedVal, err := redisClient.Get(ctx, deviceEUI).Result()
		require.NoError(t, err, "Data should now be in the Redis cache")
		assert.JSONEq(t, string(expectedJSON), cachedVal, "Cached data in Redis should match expected JSON")
		t.Log("Assertion successful: Data was correct and written to cache.")
	})

	// --- Test Case 2: Cache Hit (Fallback is not called) ---
	t.Run("Cache Hit", func(t *testing.T) {
		// --- Arrange ---
		mockFallback.Reset()
		// This fallback will cause a test failure if called, proving the cache was used.
		mockFallback.fetchFunc = func(eui string) (string, string, string, error) {
			t.Fatalf("Fallback should not have been called for a cache hit!")
			return "", "", "", errors.New("unreachable")
		}
		// Data should still be in the cache from the previous test.
		t.Log("Arranged: Data is pre-seeded in cache. Fallback is set to fail test if called.")

		// --- Act ---
		t.Log("Acting: Fetching data again, expecting a cache hit")
		clientID, locationID, category, err := fetcher.Fetch(deviceEUI)
		require.NoError(t, err)

		// --- Assert ---
		assert.Equal(t, expectedMetadata.ClientID, clientID)
		assert.Equal(t, expectedMetadata.LocationID, locationID)
		assert.Equal(t, expectedMetadata.Category, category)
		assert.Equal(t, 0, mockFallback.callCount, "Fallback should NOT be called on a cache hit")
		t.Log("Assertion successful: Data fetched from cache and fallback was not called.")
	})

	// --- Test Case 3: Cache Miss and Fallback Error ---
	t.Run("Cache Miss and Fallback Error", func(t *testing.T) {
		unknownEUI := "UNKNOWN-EUI-FOR-REDIS-TEST"
		// --- Arrange ---
		mockFallback.Reset()
		mockFallback.fetchFunc = func(eui string) (string, string, string, error) {
			assert.Equal(t, unknownEUI, eui)
			return "", "", "", ErrMetadataNotFound // Simulate the source not finding the data
		}
		// Ensure Redis is clean for this EUI
		require.NoError(t, redisClient.Del(ctx, unknownEUI).Err())

		// --- Act ---
		t.Logf("Acting: Fetching unknown EUI %s", unknownEUI)
		_, _, _, err := fetcher.Fetch(unknownEUI)

		// --- Assert ---
		require.Error(t, err, "Expected an error when fallback fails")
		assert.ErrorIs(t, err, ErrMetadataNotFound, "Error should be the one returned from fallback")
		assert.Equal(t, 1, mockFallback.callCount, "Fallback should have been called once")

		// Check that no negative cache entry was created
		_, err = redisClient.Get(ctx, unknownEUI).Result()
		assert.ErrorIs(t, err, redis.Nil, "A key should not be cached for a failed lookup")
		t.Log("Assertion successful: Correct 'Not Found' error received and nothing cached.")
	})
}
