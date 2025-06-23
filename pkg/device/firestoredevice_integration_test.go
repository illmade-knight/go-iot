package device

import (
	"context"
	"errors"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// No need for "google.golang.org/api/option" here explicitly, as it's handled by emulators package
)

// TestGoogleDeviceMetadataFetcher_Integration_TableDriven tests the GoogleDeviceMetadataFetcher
// against a live (but emulated) Google Cloud Firestore instance using a table-driven approach.
func TestGoogleDeviceMetadataFetcher_Integration_TableDriven(t *testing.T) {
	// No t.Parallel() here, as we are setting environment variables via SetupFirestoreEmulator.
	ctx := context.Background()       // Test context for non-container operations
	logger := zerolog.New(io.Discard) // Mute logger for tests to keep output clean

	const (
		testProjectID      = "test-project-integration-table"
		testCollectionName = "devices"
	)

	// --- 1. Setup Firestore Emulator ---
	firestoreEmulatorCfg := emulators.GetDefaultFirestoreConfig(testProjectID)
	// Pass context.Background() for container lifecycle. SetupFirestoreEmulator will set FIRESTORE_EMULATOR_HOST.
	connection := emulators.SetupFirestoreEmulator(t, context.Background(), firestoreEmulatorCfg)

	// --- 2. Initialize Firestore Client for direct data manipulation AND for the fetcher ---
	// This single client instance will be passed to the fetcher.
	firestoreClient, err := firestore.NewClient(ctx, testProjectID, connection.ClientOptions...)
	require.NoError(t, err, "Failed to create direct Firestore client for test setup and fetcher")
	t.Cleanup(func() {
		// This client is now owned by the test and passed to the fetcher, so the test is responsible for closing it.
		firestoreClient.Close()
	})

	// --- 3. Define Test Cases ---
	type testCase struct {
		name      string
		deviceEUI string
		setupData map[string]interface{} // Data to write to Firestore for this test case
		expected  struct {
			clientID   string
			locationID string
			category   string
		}
		expectedErr error
	}

	testCases := []testCase{
		{
			name:      "Successful Fetch of Existing Device",
			deviceEUI: "integration-test-eui-001",
			setupData: map[string]interface{}{
				"clientID":       "integration-client-abc",
				"locationID":     "integration-location-xyz",
				"deviceCategory": "integration-sensor",
			},
			expected: struct {
				clientID   string
				locationID string
				category   string
			}{
				clientID:   "integration-client-abc",
				locationID: "integration-location-xyz",
				category:   "integration-sensor",
			},
			expectedErr: nil,
		},
		{
			name:      "Fetch of Non-Existent Device",
			deviceEUI: "integration-unknown-eui-999",
			setupData: nil,
			expected: struct {
				clientID   string
				locationID string
				category   string
			}{},
			expectedErr: ErrMetadataNotFound,
		},
		{
			name:      "Fetch with Incomplete Data",
			deviceEUI: "incomplete-eui-002",
			setupData: map[string]interface{}{
				"clientID":   "incomplete-client",
				"locationID": "incomplete-location",
			},
			expected: struct {
				clientID   string
				locationID string
				category   string
			}{},
			expectedErr: errors.New("incomplete metadata for"),
		},
		{
			name:      "Fetch with Corrupted Data (DataTo Error)",
			deviceEUI: "corrupted-eui-003",
			setupData: map[string]interface{}{
				"clientID":       "corrupted-client",
				"locationID":     "corrupted-location",
				"deviceCategory": 12345, // int where string is expected, causes DataTo error
			},
			expected: struct {
				clientID   string
				locationID string
				category   string
			}{},
			expectedErr: errors.New("firestore DataTo for"),
		},
	}

	// --- 4. Run Test Cases ---
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // Sub-tests can run in parallel, as the shared client is set up once.

			// --- Setup for individual test case ---
			if tc.setupData != nil {
				_, err = firestoreClient.Collection(testCollectionName).Doc(tc.deviceEUI).Set(ctx, tc.setupData)
				require.NoError(t, err, "Failed to set test data for %s", tc.name)
				t.Logf("Test data written for EUI %s: %+v", tc.deviceEUI, tc.setupData)
				time.Sleep(100 * time.Millisecond) // Give Firestore emulator a moment
			}

			// Initialize GoogleDeviceMetadataFetcher (SUT) with the *injected* client.
			fetcherConfig := &FirestoreFetcherConfig{
				ProjectID:      testProjectID,
				CollectionName: testCollectionName,
			}
			deviceFetcher, err := NewGoogleDeviceMetadataFetcher(ctx, firestoreClient, fetcherConfig, logger) // Pass firestoreClient directly
			require.NoError(t, err, "Failed to create GoogleDeviceMetadataFetcher for %s", tc.name)
			// No defer deviceFetcher.Close() needed here as the client is closed by the parent t.Cleanup.

			// --- Execute SUT ---
			clientID, locationID, category, fetchErr := deviceFetcher.Fetch(tc.deviceEUI)

			// --- Assertions ---
			if tc.expectedErr != nil {
				require.Error(t, fetchErr, "Expected an error for %s", tc.name)
				if errors.Is(tc.expectedErr, ErrMetadataNotFound) {
					assert.True(t, errors.Is(fetchErr, ErrMetadataNotFound), "Error for %s should be ErrMetadataNotFound, got %v", tc.name, fetchErr)
				} else {
					assert.Contains(t, fetchErr.Error(), tc.expectedErr.Error(), "Error message for %s should contain '%s', got '%s'", tc.name, tc.expectedErr.Error(), fetchErr.Error())
				}
			} else {
				require.NoError(t, fetchErr, "Did not expect an error for %s", tc.name)
				assert.Equal(t, tc.expected.clientID, clientID, "Client ID mismatch for %s", tc.name)
				assert.Equal(t, tc.expected.locationID, locationID, "Location ID mismatch for %s", tc.name)
				assert.Equal(t, tc.expected.category, category, "Category mismatch for %s", tc.name)
			}

			// Clean up data for the next parallel test run within the same collection.
			// This is critical for parallel table-driven tests sharing a collection.
			if tc.setupData != nil {
				_, err = firestoreClient.Collection(testCollectionName).Doc(tc.deviceEUI).Delete(ctx)
				require.NoError(t, err, "Failed to clean up test data for %s", tc.name)
			}
		})
	}
}
