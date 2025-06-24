package device

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/go-redis/redis/v8"
	"github.com/illmade-knight/go-iot/helpers/emulators"
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
	firestoreProjectID := "test-chained-fetcher"
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
	// This creates the concrete implementation that will be passed into the chained fetcher.
	firestoreConfig := &FirestoreFetcherConfig{ProjectID: firestoreProjectID, CollectionName: firestoreCollection}
	sourceFetcher, err := NewGoogleDeviceMetadataFetcher(firestoreClient, firestoreConfig, logger)
	require.NoError(t, err)
	// Note: The cleanup function from the chained fetcher will handle closing this.

	// --- Create the Chained Fetcher ---
	// We pass the concrete `sourceFetcher` which satisfies the `SourceFetcher` interface.
	redisConfig := &RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute}
	fetcher, cleanup, err := NewChainedFetcher(ctx, redisConfig, sourceFetcher, logger)
	require.NoError(t, err)
	require.NotNil(t, fetcher)
	defer cleanup()

	// --- Define Test Data ---
	deviceEUI := "CHAINED-TEST-001"
	expectedData := map[string]interface{}{
		"clientID":       "client-abc",
		"locationID":     "location-xyz",
		"deviceCategory": "temp-sensor",
	}

	// --- Test Case 1: Cache Miss, Firestore Hit, and Cache Write-back ---
	t.Run("Cache Miss then Firestore Hit with Writeback", func(t *testing.T) {
		// --- Arrange ---
		// 1. Ensure Redis is empty for this EUI
		require.NoError(t, redisClient.Del(ctx, deviceEUI).Err())

		// 2. Put the data into Firestore (the source of truth)
		_, err := firestoreClient.Collection(firestoreCollection).Doc(deviceEUI).Set(ctx, expectedData)
		require.NoError(t, err)
		t.Logf("Arranged data in Firestore for EUI %s", deviceEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss followed by Firestore hit")
		clientID, locationID, category, err := fetcher(deviceEUI)
		require.NoError(t, err)

		// --- Assert ---
		// 1. Check that the data returned from the fetcher is correct
		assert.Equal(t, expectedData["clientID"], clientID)
		assert.Equal(t, expectedData["locationID"], locationID)
		assert.Equal(t, expectedData["deviceCategory"], category)
		t.Log("Assertion successful: Data from fetcher is correct.")

		// 2. Check that the data was written back to the Redis cache
		cachedVal, err := redisClient.Get(ctx, deviceEUI).Result()
		require.NoError(t, err, "Data should now be in the Redis cache")

		var cachedData deviceMetadataCache
		err = json.Unmarshal([]byte(cachedVal), &cachedData)
		require.NoError(t, err)

		assert.Equal(t, expectedData["clientID"], cachedData.ClientID)
		assert.Equal(t, expectedData["locationID"], cachedData.LocationID)
		assert.Equal(t, expectedData["deviceCategory"], cachedData.Category)
		t.Log("Assertion successful: Data was written back to Redis cache.")
	})

	// --- Test Case 2: Cache Hit (Firestore is not called) ---
	t.Run("Cache Hit", func(t *testing.T) {
		// --- Arrange ---
		// Data should still be in the cache from the previous test.
		// To prove the cache is being used, we will *delete* the data from Firestore.
		// A successful fetch now can only come from the cache.
		_, err := firestoreClient.Collection(firestoreCollection).Doc(deviceEUI).Delete(ctx)
		require.NoError(t, err)
		t.Logf("Arranged: Deleted data from Firestore for EUI %s to force cache usage", deviceEUI)

		// --- Act ---
		t.Log("Acting: Fetching data again, expecting a cache hit")
		clientID, locationID, category, err := fetcher(deviceEUI)
		require.NoError(t, err)

		// --- Assert ---
		// Check that the data returned is still correct, even though it's gone from the source of truth.
		assert.Equal(t, expectedData["clientID"], clientID)
		assert.Equal(t, expectedData["locationID"], locationID)
		assert.Equal(t, expectedData["deviceCategory"], category)
		t.Log("Assertion successful: Correct data fetched from cache.")
	})

	// --- Test Case 3: Cache Miss and Firestore Miss ---
	t.Run("Cache and Firestore Miss", func(t *testing.T) {
		unknownEUI := "UNKNOWN-EUI-999"

		// --- Arrange ---
		// Ensure the key doesn't exist in either Redis or Firestore.
		require.NoError(t, redisClient.Del(ctx, unknownEUI).Err())
		_, err := firestoreClient.Collection(firestoreCollection).Doc(unknownEUI).Delete(ctx)
		// It's ok if this errors (e.g., if it didn't exist), we just want to ensure it's gone.
		t.Logf("Arranged: Ensured EUI %s is not in Redis or Firestore (err: %v)", unknownEUI, err)

		// --- Act ---
		t.Logf("Acting: Fetching unknown EUI %s", unknownEUI)
		_, _, _, err = fetcher(unknownEUI)

		// --- Assert ---
		require.Error(t, err, "Expected an error when data is in neither cache nor source")
		assert.ErrorIs(t, err, ErrMetadataNotFound, "Error should be ErrMetadataNotFound")
		t.Log("Assertion successful: Received correct 'Not Found' error.")
	})
}
