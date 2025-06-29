//go:build integration

package device_test // Changed package to device_test

import (
	"context"
	"encoding/json" // Required for unmarshalling Redis direct reads in cleanup verification
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/go-redis/redis/v8" // Still needed for Redis client setup
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/device" // Import the main package now
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChainedFetcher_Integration tests the complete caching and fallback logic
// by using live, containerized Redis and Firestore emulators.
func TestChainedFetcher_Integration(t *testing.T) {
	// --- Test Setup ---
	require.NotEmpty(t, "docker", "This test requires Docker to be running.")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)

	// --- Start Emulators ---
	// 1. Firestore Emulator
	firestoreProjectID := "test-chained-fetcher-main" // Unique project ID
	firestoreCollection := "devices"
	firestoreEmulatorCfg := emulators.GetDefaultFirestoreConfig(firestoreProjectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, firestoreEmulatorCfg)
	firestoreClient, err := firestore.NewClient(ctx, firestoreProjectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	defer firestoreClient.Close()

	// 2. Redis Emulator
	redisEmulatorCfg := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, redisEmulatorCfg)
	redisClient := redis.NewClient(&redis.Options{Addr: redisConn.EmulatorAddress})
	require.NoError(t, redisClient.Ping(ctx).Err(), "could not connect to redis emulator")
	defer redisClient.Close()

	// --- Create the Concrete Source Fetcher (Firestore) ---
	firestoreConfig := &device.FirestoreFetcherConfig{ProjectID: firestoreProjectID, CollectionName: firestoreCollection}
	sourceFetcher, err := device.NewGoogleDeviceMetadataFetcher(firestoreClient, firestoreConfig, logger)
	require.NoError(t, err)
	// Note: The cleanup function from the chained fetcher will handle closing this.

	// --- Create the Redis Cache Fetcher (implementing CachedFetcher interface) ---
	redisConfig := &device.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute}
	// NewRedisDeviceMetadataFetcher now takes ctx, config, logger
	redisCacheFetcher, err := device.NewRedisDeviceMetadataFetcher(ctx, redisConfig, logger)
	require.NoError(t, err)
	// Note: The cleanup function from the chained fetcher will handle closing this.

	// --- Create the Chained Fetcher ---
	// Now pass the redisCacheFetcher (which implements device.CachedFetcher)
	fetcher, cleanup, err := device.NewCacheFallbackFetcher(ctx, redisCacheFetcher, sourceFetcher, logger)
	require.NoError(t, err)
	require.NotNil(t, fetcher)
	defer cleanup() // Ensure all resources are closed after the test

	// --- Define Test Data ---
	deviceEUI := "CHAINED-INTEGRATION-TEST-001"
	expectedClientID := "client-abc"
	expectedLocationID := "location-xyz"
	expectedCategory := "temp-sensor"
	expectedData := map[string]interface{}{
		"clientID":       expectedClientID,
		"locationID":     expectedLocationID,
		"deviceCategory": expectedCategory,
	}
	expectedCacheJSON, err := json.Marshal(localDeviceMetadataCache{
		ClientID:   expectedClientID,
		LocationID: expectedLocationID,
		Category:   expectedCategory,
	})
	require.NoError(t, err)

	// --- Test Case 1: Cache Miss, Firestore Hit, and Cache Write-back ---
	t.Run("Cache Miss then Firestore Hit with Writeback", func(t *testing.T) {
		subTestEUI := deviceEUI + "-CacheMiss"
		t.Logf("Running Cache Miss test with EUI: %s", subTestEUI)

		// --- Arrange ---
		// 1. Ensure Redis is empty for this EUI
		require.NoError(t, redisClient.Del(ctx, subTestEUI).Err())
		// 2. Put the data into Firestore (the source of truth)
		_, err := firestoreClient.Collection(firestoreCollection).Doc(subTestEUI).Set(ctx, expectedData)
		require.NoError(t, err)
		t.Logf("Arranged data in Firestore for EUI %s", subTestEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss followed by Firestore hit")
		clientID, locationID, category, err := fetcher(subTestEUI)
		require.NoError(t, err)

		// --- Assert ---
		// 1. Check that the data returned from the fetcher is correct
		assert.Equal(t, expectedClientID, clientID)
		assert.Equal(t, expectedLocationID, locationID)
		assert.Equal(t, expectedCategory, category)
		t.Log("Assertion successful: Data from fetcher is correct.")

		// 2. Check that the data was written back to the Redis cache
		// Wait briefly for the background goroutine to write to cache.
		time.Sleep(50 * time.Millisecond) // Give background goroutine time to write to cache
		cachedVal, err := redisClient.Get(ctx, subTestEUI).Result()
		require.NoError(t, err, "Data should now be in the Redis cache")
		assert.JSONEq(t, string(expectedCacheJSON), cachedVal, "Cached data in Redis should match expected JSON")
		t.Logf("Assertion successful: Data was written back to Redis cache for EUI %s.", subTestEUI)
	})

	// --- Test Case 2: Cache Hit (Firestore is not called) ---
	t.Run("Cache Hit", func(t *testing.T) {
		subTestEUI := deviceEUI + "-CacheHit"
		t.Logf("Running Cache Hit test with EUI: %s", subTestEUI)

		// --- Arrange ---
		// Manually put data into the Redis cache for this specific EUI.
		require.NoError(t, redisClient.Set(ctx, subTestEUI, expectedCacheJSON, 0).Err())
		t.Logf("Arranged: Manually put EUI %s into Redis cache.", subTestEUI)

		// To prove the cache is being used, we will *delete* the data from Firestore.
		// A successful fetch now can only come from the cache.
		_, err := firestoreClient.Collection(firestoreCollection).Doc(subTestEUI).Delete(ctx)
		require.NoError(t, err)
		t.Logf("Arranged: Deleted data from Firestore for EUI %s to force cache usage", subTestEUI)

		// --- Act ---
		t.Log("Acting: Fetching data again, expecting a cache hit")
		clientID, locationID, category, err := fetcher(subTestEUI)
		require.NoError(t, err)

		// --- Assert ---
		// Check that the data returned is still correct, even though it's gone from the source of truth.
		assert.Equal(t, expectedClientID, clientID)
		assert.Equal(t, expectedLocationID, locationID)
		assert.Equal(t, expectedCategory, category)
		t.Log("Assertion successful: Correct data fetched from cache.")
	})

	// --- Test Case 3: Cache Miss and Firestore Miss ---
	t.Run("Cache and Firestore Miss", func(t *testing.T) {
		unknownEUI := "UNKNOWN-EUI-999-CacheSourceMiss"
		t.Logf("Running Cache and Source Miss test with EUI: %s", unknownEUI)

		// --- Arrange ---
		// Ensure the key doesn't exist in either Redis or Firestore.
		require.NoError(t, redisClient.Del(ctx, unknownEUI).Err())
		_, err := firestoreClient.Collection(firestoreCollection).Doc(unknownEUI).Delete(ctx)
		require.NoError(t, err) // It's ok if this errors, we just want to ensure it's gone.
		t.Logf("Arranged: Ensured EUI %s is not in Redis or Firestore", unknownEUI)

		// --- Act ---
		t.Logf("Acting: Fetching unknown EUI %s", unknownEUI)
		_, _, _, err = fetcher(unknownEUI)

		// --- Assert ---
		require.Error(t, err, "Expected an error when data is in neither cache nor source")
		assert.ErrorIs(t, err, device.ErrMetadataNotFound, "Error should be ErrMetadataNotFound")
		t.Log("Assertion successful: Received correct 'Not Found' error.")
	})
}
