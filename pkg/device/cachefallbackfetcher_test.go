package device_test // Changed package to device_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/device" // Import the main package now
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// Removed testcontainers imports as Redis is removed from this specific test
)

// TestNewChainedFetcher_Integration tests the full flow with an in-memory cache.
func TestNewChainedFetcher_Integration(t *testing.T) {
	// --- Test Setup ---
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)

	// --- Start Emulators ---
	// Firestore Emulator
	firestoreProjectID := "test-chained-fetcher"
	firestoreCollection := "devices"
	firestoreEmulatorCfg := emulators.GetDefaultFirestoreConfig(firestoreProjectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, firestoreEmulatorCfg)
	firestoreClient, err := firestore.NewClient(ctx, firestoreProjectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	defer firestoreClient.Close()

	// --- Create the Concrete Source Fetcher (Firestore) ---
	firestoreConfig := &device.FirestoreFetcherConfig{ProjectID: firestoreProjectID, CollectionName: firestoreCollection}
	sourceFetcher, err := device.NewGoogleDeviceMetadataFetcher(firestoreClient, firestoreConfig, logger)
	require.NoError(t, err)
	// Note: The cleanup function from the chained fetcher will handle closing this sourceFetcher.

	// --- Create the In-Memory Cache Fetcher ---
	// Instantiate our new in-memory cache implementation.
	// Using device.NewInMemoryCacheFetcher now.
	inMemoryCache := device.NewInMemoryCacheFetcher(ctx, 5*time.Minute, logger) // Context used for logger init, but not stored in InMemoryCacheFetcher.
	// Note: The cleanup function from the chained fetcher will handle closing this inMemoryCache.

	// --- Create the Chained Fetcher ---
	// Pass the in-memory cache and the source fetcher.
	fetcher, cleanup, err := device.NewCacheFallbackFetcher(ctx, inMemoryCache, sourceFetcher, logger) // Pass ctx to NewCacheFallbackFetcher
	require.NoError(t, err)
	require.NotNil(t, fetcher)
	defer cleanup() // Ensure all resources are closed after the test

	// --- Define Test Data ---
	deviceEUI := "CHAINED-TEST-001"
	expectedClientID := "client-abc"
	expectedLocationID := "location-xyz"
	expectedCategory := "temp-sensor"
	expectedData := map[string]interface{}{
		"clientID":       expectedClientID,
		"locationID":     expectedLocationID,
		"deviceCategory": expectedCategory,
	}

	// --- Test Case 1: Cache Miss, Source Hit, and Cache Write-back ---
	t.Run("Cache Miss then Source Hit with Writeback", func(t *testing.T) {
		subTestEUI := deviceEUI + "-CacheMiss"
		t.Logf("Running Cache Miss test with EUI: %s", subTestEUI)

		// --- Arrange ---
		// 1. Ensure cache is logically empty for this EUI (new EUI for this sub-test).

		// 2. Put the data into Firestore (the source of truth)
		_, err := firestoreClient.Collection(firestoreCollection).Doc(subTestEUI).Set(ctx, expectedData)
		require.NoError(t, err)
		t.Logf("Arranged data in Firestore for EUI %s", subTestEUI)

		// --- Act ---
		t.Log("Acting: Fetching data, expecting a cache miss followed by Source hit")
		clientID, locationID, category, err := fetcher(subTestEUI) // Context is implicitly available to fetcher func
		require.NoError(t, err)

		// --- Assert ---
		// 1. Check that the data returned from the fetcher is correct
		assert.Equal(t, expectedClientID, clientID)
		assert.Equal(t, expectedLocationID, locationID)
		assert.Equal(t, expectedCategory, category)
		t.Log("Assertion successful: Data from fetcher is correct.")

		// 2. The write-back to in-memory cache is asynchronous.
		// To verify write-back, the next test case ("Cache Hit") will try to read it.
		// However, for parallel tests, each test needs to be self-contained.
		// So, for this specific sub-test, we'll confirm cache write-back by attempting to fetch again
		// and expecting a cache hit (which proves it's in the cache).
		// Wait briefly for the goroutine to write to cache.
		time.Sleep(50 * time.Millisecond)                                                                           // Give background goroutine time to write to cache
		cachedClientID, cachedLocationID, cachedCategory, cacheErr := inMemoryCache.FetchFromCache(ctx, subTestEUI) // Pass ctx
		assert.NoError(t, cacheErr, "Data should have been written back to in-memory cache")
		assert.Equal(t, expectedClientID, cachedClientID)
		assert.Equal(t, expectedLocationID, cachedLocationID)
		assert.Equal(t, expectedCategory, cachedCategory)
		t.Logf("Assertion successful: Data was written back to in-memory cache for EUI %s.", subTestEUI)
	})

	// --- Test Case 2: Cache Hit (Source is not called) ---
	t.Run("Cache Hit", func(t *testing.T) {
		subTestEUI := deviceEUI + "-CacheHit"
		t.Logf("Running Cache Hit test with EUI: %s", subTestEUI)

		// --- Arrange ---
		// Manually put data into the in-memory cache for this specific EUI.
		require.NoError(t, inMemoryCache.WriteToCache(ctx, subTestEUI, expectedClientID, expectedLocationID, expectedCategory)) // Pass ctx
		t.Logf("Arranged: Manually put EUI %s into in-memory cache.", subTestEUI)

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

	// --- Test Case 3: Cache Miss and Source Miss ---
	t.Run("Cache and Source Miss", func(t *testing.T) {
		subTestEUI := deviceEUI + "-CacheSourceMiss"
		t.Logf("Running Cache and Source Miss test with EUI: %s", subTestEUI)

		// --- Arrange ---
		// Ensure the key doesn't exist in either in-memory cache or Firestore.
		// For in-memory cache, rely on Fetcher's logic not to find it if it wasn't written.
		// Or, to be explicit, you could add a 'Delete' method to InMemoryCacheFetcher.
		// For now, relying on unique EUI and empty state for a fresh InMemoryCacheFetcher implies no data.
		_, err := firestoreClient.Collection(firestoreCollection).Doc(subTestEUI).Delete(ctx)
		require.NoError(t, err)
		t.Logf("Arranged: Ensured EUI %s is not in Firestore", subTestEUI)

		// --- Act ---
		t.Logf("Acting: Fetching unknown EUI %s", subTestEUI)
		_, _, _, err = fetcher(subTestEUI)

		// --- Assert ---
		require.Error(t, err, "Expected an error when data is in neither cache nor source")
		// Using the IsMetadataNotFound from the device package.
		assert.ErrorIs(t, err, device.ErrMetadataNotFound, "Error should be ErrMetadataNotFound")
		t.Log("Assertion successful: Received correct 'Not Found' error.")
	})
}
