package device_test // Test package

import (
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/pkg/device" // Import the main package
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryCacheFetcher_Unit tests the basic functionality of InMemoryCacheFetcher.
func TestInMemoryCacheFetcher_Unit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled for cleanup

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)
	cacheTTL := 1 * time.Minute // TTL for cache entries

	// --- Create a new InMemoryCacheFetcher ---
	cacheFetcher := device.NewInMemoryCacheFetcher(ctx, cacheTTL, logger)
	require.NotNil(t, cacheFetcher, "Expected NewInMemoryCacheFetcher to return a non-nil fetcher")
	defer func() {
		assert.NoError(t, cacheFetcher.Close(), "Expected no error closing InMemoryCacheFetcher")
	}()

	// --- Define Test Data ---
	eui1 := "DEVICE-001"
	eui1ClientID := "client-A"
	eui1LocationID := "location-X"
	eui1Category := "temp-sensor"

	eui2 := "DEVICE-002" // For cache miss test
	eui2ClientID := "client-B"
	eui2LocationID := "location-Y"
	eui2Category := "pressure-sensor"

	// --- Test Case 1: Cache Miss ---
	t.Run("Cache Miss", func(t *testing.T) {
		t.Logf("Running Cache Miss test for EUI: %s", eui1)
		clientID, locationID, category, err := cacheFetcher.FetchFromCache(ctx, eui1)
		require.Error(t, err, "Expected an error for cache miss")
		assert.ErrorIs(t, err, device.ErrCacheMiss{EUI: eui1}, "Error should be ErrCacheMiss")
		assert.Empty(t, clientID, "Client ID should be empty on cache miss")
		assert.Empty(t, locationID, "Location ID should be empty on cache miss")
		assert.Empty(t, category, "Category should be empty on cache miss")
		t.Log("Assertion successful: Correctly handled cache miss.")
	})

	// --- Test Case 2: Write to Cache ---
	t.Run("Write to Cache", func(t *testing.T) {
		t.Logf("Running Write to Cache test for EUI: %s", eui1)
		err := cacheFetcher.WriteToCache(ctx, eui1, eui1ClientID, eui1LocationID, eui1Category)
		require.NoError(t, err, "Expected no error writing to cache")
		t.Log("Assertion successful: Data written to cache.")
	})

	// --- Test Case 3: Cache Hit ---
	t.Run("Cache Hit", func(t *testing.T) {
		t.Logf("Running Cache Hit test for EUI: %s", eui1)
		clientID, locationID, category, err := cacheFetcher.FetchFromCache(ctx, eui1)
		require.NoError(t, err, "Expected no error for cache hit")
		assert.Equal(t, eui1ClientID, clientID, "Client ID mismatch on cache hit")
		assert.Equal(t, eui1LocationID, locationID, "Location ID mismatch on cache hit")
		assert.Equal(t, eui1Category, category, "Category mismatch on cache hit")
		t.Log("Assertion successful: Correctly handled cache hit.")
	})

	// --- Test Case 4: Overwrite Cache Entry ---
	t.Run("Overwrite Cache Entry", func(t *testing.T) {
		newClientID := "client-A-new"
		newLocationID := "location-X-new"
		newCategory := "new-type"
		t.Logf("Running Overwrite Cache Entry test for EUI: %s", eui1)

		err := cacheFetcher.WriteToCache(ctx, eui1, newClientID, newLocationID, newCategory)
		require.NoError(t, err, "Expected no error overwriting cache")

		clientID, locationID, category, err := cacheFetcher.FetchFromCache(ctx, eui1)
		require.NoError(t, err, "Expected no error fetching overwritten entry")
		assert.Equal(t, newClientID, clientID, "Client ID mismatch after overwrite")
		assert.Equal(t, newLocationID, locationID, "Location ID mismatch after overwrite")
		assert.Equal(t, newCategory, category, "Category mismatch after overwrite")
		t.Log("Assertion successful: Cache entry overwritten correctly.")
	})

	// --- Test Case 5: Another Cache Miss (for a different EUI) ---
	t.Run("Another Cache Miss", func(t *testing.T) {
		t.Logf("Running another Cache Miss test for EUI: %s", eui2)
		clientID, _, _, err := cacheFetcher.FetchFromCache(ctx, eui2)
		require.Error(t, err, "Expected an error for cache miss on new EUI")
		assert.ErrorIs(t, err, device.ErrCacheMiss{EUI: eui2}, "Error should be ErrCacheMiss for new EUI")
		assert.Empty(t, clientID, "Client ID should be empty on cache miss for new EUI")
		t.Log("Assertion successful: Another cache miss handled correctly.")
	})

	// --- Test Case 6: Context Cancellation during Fetch ---
	t.Run("Context Cancellation during Fetch", func(t *testing.T) {
		cancelCtx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc() // Immediately cancel the context
		t.Log("Running Context Cancellation during Fetch test (expecting context.Canceled error)")

		_, _, _, err := cacheFetcher.FetchFromCache(cancelCtx, eui1)
		require.Error(t, err, "Expected an error due to context cancellation")
		assert.ErrorIs(t, err, context.Canceled, "Error should be context.Canceled")
		t.Log("Assertion successful: Fetch correctly handles context cancellation.")
	})

	// --- Test Case 7: Context Cancellation during Write ---
	t.Run("Context Cancellation during Write", func(t *testing.T) {
		cancelCtx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc() // Immediately cancel the context
		t.Logf("Running Context Cancellation during Write test for EUI: %s (expecting context.Canceled error)", eui2)

		err := cacheFetcher.WriteToCache(cancelCtx, eui2, eui2ClientID, eui2LocationID, eui2Category)
		require.Error(t, err, "Expected an error due to context cancellation")
		assert.ErrorIs(t, err, context.Canceled, "Error should be context.Canceled")
		t.Log("Assertion successful: Write correctly handles context cancellation.")
	})
}
