package servicemanager_test

import (
	"context"
	"errors"
	servicemanager "github.com/illmade-knight/go-iot/servicemanager"
	"io"
	"testing"

	"github.com/illmade-knight/go-iot/pkg/types" // Assuming this is needed for schema registry
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mocks for underlying Clients and their sub-components ---
// These mocks are assumed to be defined elsewhere in the servicemanager_test package
// (e.g., in messagingmanager_test.go, storagemanager_test.go, bigquerymanager_test.go)
// and are imported/accessible here. Therefore, their definitions are omitted from this file.

// --- Test Helper Functions ---

func getTestDataflowResourceGroup() *servicemanager.ResourceGroup {
	return &servicemanager.ResourceGroup{
		Name: "test-dataflow",
		Resources: servicemanager.CloudResourcesSpec{
			Topics: []servicemanager.TopicConfig{
				{CloudResource: servicemanager.CloudResource{Name: "df-topic-1", TeardownProtection: false}}, // Not protected
				{CloudResource: servicemanager.CloudResource{Name: "df-topic-2", TeardownProtection: true}},  // Protected
			},
			GCSBuckets: []servicemanager.GCSBucket{
				{CloudResource: servicemanager.CloudResource{Name: "df-bucket-1", TeardownProtection: false}}, // Not protected
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{CloudResource: servicemanager.CloudResource{Name: "df-dataset-1", TeardownProtection: false}}, // Not protected
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{
					CloudResource:          servicemanager.CloudResource{Name: "df-table-1", TeardownProtection: false}, // Not protected
					Dataset:                "df-dataset-1",
					SchemaSourceIdentifier: "test-schema", // Assuming a schema source for BigQuery tables
				},
			},
			Subscriptions: []servicemanager.SubscriptionConfig{
				{CloudResource: servicemanager.CloudResource{Name: "df-sub-1", TeardownProtection: false}, Topic: "df-topic-1", AckDeadlineSeconds: 10}, // Not protected
				{CloudResource: servicemanager.CloudResource{Name: "df-sub-2", TeardownProtection: true}, Topic: "df-topic-2", AckDeadlineSeconds: 10},  // Protected
			},
		},
	}
}

// setupDataflowManagerTest creates a DataflowManager with real sub-managers
// injected with mock clients for testing.
func setupDataflowManagerTest(t *testing.T) (*servicemanager.DataflowManager, *MockMessagingClient, *MockStorageClient, *MockBQClient, context.Context, servicemanager.Environment, *servicemanager.ResourceGroup, zerolog.Logger) {
	logger := zerolog.New(io.Discard)
	ctx := context.Background()
	dataflowSpec := getTestDataflowResourceGroup()
	env := servicemanager.Environment{
		Name:               "test",
		ProjectID:          "test-project",
		Location:           "us-central1",
		TeardownProtection: false, // This environment-level flag might still exist, but resource-level takes precedence for specific resources
		Labels:             map[string]string{"env": "test"},
	}
	schemaRegistry := map[string]interface{}{
		"test-schema": types.GardenMonitorReadings{}, // Example schema, ensure pkg/types is correct
	}

	// Create Mock Clients (these types are assumed to be defined elsewhere)
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	// Create REAL Managers with the MOCK Clients
	msgManager, err := servicemanager.NewMessagingManager(mockMsgClient, logger)
	require.NoError(t, err)
	storeManager, err := servicemanager.NewStorageManager(mockStoreClient, logger)
	require.NoError(t, err)
	bqManager, err := servicemanager.NewBigQueryManager(mockBqClient, logger, schemaRegistry, env)
	require.NoError(t, err)

	// Create the DataflowManager using the REAL managers
	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		msgManager, storeManager, bqManager,
		env, logger,
	)
	require.NoError(t, err)
	require.NotNil(t, dfm)

	return dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowSpec, logger
}

// --- Test Cases ---

// TestNewDataflowManager tests the constructor for DataflowManager.
func TestNewDataflowManager(t *testing.T) {
	logger := zerolog.New(io.Discard)
	ctx := context.Background()
	env := servicemanager.Environment{ProjectID: "test-project"}

	schemaRegistry := map[string]interface{}{
		"test-schema": types.GardenMonitorReadings{}, // Example schema
	}

	t.Run("Success", func(t *testing.T) {
		// This test primarily checks that the constructor doesn't panic and returns a DataflowManager.
		// Detailed failure paths for internal client/manager creation are difficult to test
		// without refactoring the main code to allow injecting client/manager factories.
		dfm, err := servicemanager.NewDataflowManager(ctx, env, schemaRegistry, logger)
		require.NoError(t, err)
		assert.NotNil(t, dfm)
	})

	// Add test cases for NewDataflowManager failure paths if the constructor were refactored
	// to accept injectable client/manager factories.
}

// TestDataflowManager_Setup_Success tests the successful setup of all resources.
func TestDataflowManager_Setup_Success(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Manager Client Mocks for Setup ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Twice() // Once for topic creation, once for sub safeguard
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()             // For initial topic creation check
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic1, nil).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once() // For subscription's topic existence check

	mockTopic2 := new(MockMessagingTopic) // For df-topic-2 (protected, but setup should still attempt if it doesn't exist)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()

	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub1, nil).Once()

	mockSub2 := new(MockMessagingSubscription) // For df-sub-2 (protected)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(false, nil).Once()
	// Note: df-sub-2 is on df-topic-2, which is also protected.
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once() // For subscription's topic existence check
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockMsgClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub2, nil).Once()

	// --- Storage Manager Client Mocks for Setup ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(nil, errors.New("bucket not found")).Once() // Simulate bucket not existing
	mockBucket1.On("Create", ctx, env.ProjectID, mock.AnythingOfType("*servicemanager.GCSBucket")).Return(nil).Once()
	// Define expected provisioned resources for Storage (will be nil as sub-manager doesn't return them yet)
	var expectedProvisionedGCS []servicemanager.ProvisionedGCSBucket = nil

	// --- BigQuery Manager Client Mocks for Setup ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(nil, errors.New("dataset not found")).Once() // Simulate dataset not existing
	mockDataset1.On("Create", ctx, mock.AnythingOfType("*servicemanager.BigQueryDataset")).Return(nil).Once()
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "df-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, errors.New("table not found")).Once() // Simulate table not existing
	mockTable1.On("Create", ctx, mock.AnythingOfType("*servicemanager.BigQueryTable")).Return(nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe() // Project might be called multiple times
	// Define expected provisioned resources for BigQuery (will be nil as sub-manager doesn't return them yet)
	var expectedProvisionedBQ []servicemanager.ProvisionedBigQueryTable = nil

	// Act
	provResources, err := dfm.SetupResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup

	// Assert
	require.NoError(t, err)
	assert.NotNil(t, provResources)
	assert.Equal(t, expectedProvisionedGCS, provResources.GCSBuckets)    // Now expecting nil
	assert.Equal(t, expectedProvisionedBQ, provResources.BigQueryTables) // Now expecting nil

	// Verify all mock expectations were met on the clients and their sub-mocks
	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}

// TestDataflowManager_Setup_MessagingManagerFails tests setup failure due to MessagingManager.
func TestDataflowManager_Setup_MessagingManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, _, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Manager Client Mocks for Failure ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	expectedErr := errors.New("messaging client create topic failed")
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(nil, expectedErr).Once()

	// Ensure other managers' clients are not called as Setup short-circuits
	mockStoreClient.AssertNotCalled(t, "Validate", mock.Anything)
	mockStoreClient.AssertNotCalled(t, "Bucket", mock.Anything)
	mockBqClient.AssertNotCalled(t, "Validate", mock.Anything)
	mockBqClient.AssertNotCalled(t, "Dataset", mock.Anything)

	provResources, err := dfm.SetupResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Nil(t, provResources) // Should return nil provisioned resources on failure

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
}

// TestDataflowManager_Setup_StorageManagerFails tests setup failure due to StorageManager.
func TestDataflowManager_Setup_StorageManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Manager Client Mocks (Success) ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Twice()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic1, nil).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub1, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockMsgClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub2, nil).Once()

	// --- Storage Manager Client Mocks for Failure ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(nil, errors.New("bucket not found")).Once()
	expectedErr := errors.New("storage client create bucket failed")
	mockBucket1.On("Create", ctx, env.ProjectID, mock.AnythingOfType("*servicemanager.GCSBucket")).Return(expectedErr).Once()

	// Ensure BigQuery manager's clients are not called as Setup short-circuits
	mockBqClient.AssertNotCalled(t, "Validate", mock.Anything)
	mockBqClient.AssertNotCalled(t, "Dataset", mock.Anything)

	provResources, err := dfm.SetupResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Nil(t, provResources) // Should return nil provisioned resources on failure

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
}

// TestDataflowManager_Setup_BigQueryManagerFails tests setup failure due to BigQueryManager.
func TestDataflowManager_Setup_BigQueryManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Manager Client Mocks (Success) ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Twice()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic1, nil).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub1, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockMsgClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub2, nil).Once()

	// --- Storage Manager Client Mocks (Success) ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(nil, errors.New("bucket not found")).Once()
	mockBucket1.On("Create", ctx, env.ProjectID, mock.AnythingOfType("*servicemanager.GCSBucket")).Return(nil).Once()

	// --- BigQuery Manager Client Mocks for Failure ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(nil, errors.New("dataset not found")).Once()
	expectedErr := errors.New("bigquery client create dataset failed")
	mockDataset1.On("Create", ctx, mock.AnythingOfType("*servicemanager.BigQueryDataset")).Return(expectedErr).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe() // Project might be called before failure

	provResources, err := dfm.SetupResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Nil(t, provResources) // Should return nil provisioned resources on failure

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Setup_MultipleManagersFail tests setup failure with multiple managers failing.
func TestDataflowManager_Setup_MultipleManagersFail(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	msgErr := errors.New("messaging setup failed")
	storeErr := errors.New("storage setup failed")
	bqErr := errors.New("bigquery setup failed")

	// --- Messaging Manager Client Mocks (Failure) ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	mockMsgClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(nil, msgErr).Once()

	// --- Storage Manager Client Mocks (Failure) ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(nil, errors.New("bucket not found")).Once()
	mockBucket1.On("Create", ctx, env.ProjectID, mock.AnythingOfType("*servicemanager.GCSBucket")).Return(storeErr).Once()

	// --- BigQuery Manager Client Mocks (Failure) ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(nil, errors.New("dataset not found")).Once()
	mockDataset1.On("Create", ctx, mock.AnythingOfType("*servicemanager.BigQueryDataset")).Return(bqErr).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	provResources, err := dfm.SetupResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), msgErr.Error())
	assert.Contains(t, err.Error(), storeErr.Error())
	assert.Contains(t, err.Error(), bqErr.Error())
	assert.Nil(t, provResources)

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Teardown_Success tests successful teardown.
func TestDataflowManager_Teardown_Success(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- BigQuery Client Mocks for Teardown ---
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("DeleteWithContents", ctx).Return(nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe() // Project might be called for logging/internal use

	// --- Storage Client Mocks for Teardown ---
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Delete", ctx).Return(nil).Once()

	// --- Messaging Client Mocks for Teardown ---
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Delete", ctx).Return(nil).Once()
	// df-sub-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Subscription", "df-sub-2")

	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Delete", ctx).Return(nil).Once()
	// df-topic-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Topic", "df-topic-2")

	err := dfm.TeardownResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.NoError(t, err)

	mockMsgClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Teardown_BigQueryManagerFails tests teardown failure due to BigQueryManager.
func TestDataflowManager_Teardown_BigQueryManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- BigQuery Client Mocks for Failure ---
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	expectedErr := errors.New("BigQuery dataset delete failed")
	mockDataset1.On("DeleteWithContents", ctx).Return(expectedErr).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	// --- Storage Client Mocks (Success - still attempted) ---
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Delete", ctx).Return(nil).Once()

	// --- Messaging Client Mocks (Success - still attempted) ---
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Delete", ctx).Return(nil).Once()
	// df-sub-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Subscription", "df-sub-2")

	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Delete", ctx).Return(nil).Once()
	// df-topic-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Topic", "df-topic-2")

	err := dfm.TeardownResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Teardown_StorageManagerFails tests teardown failure due to StorageManager.
func TestDataflowManager_Teardown_StorageManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- BigQuery Client Mocks (Success) ---
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("DeleteWithContents", ctx).Return(nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	// --- Storage Client Mocks for Failure ---
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	expectedErr := errors.New("GCS bucket delete failed")
	mockBucket1.On("Delete", ctx).Return(expectedErr).Once()

	// --- Messaging Client Mocks (Success - still attempted) ---
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Delete", ctx).Return(nil).Once()
	// df-sub-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Subscription", "df-sub-2")

	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Delete", ctx).Return(nil).Once()
	// df-topic-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Topic", "df-topic-2")

	err := dfm.TeardownResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Teardown_MessagingManagerFails tests teardown failure due to MessagingManager.
func TestDataflowManager_Teardown_MessagingManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- BigQuery Client Mocks (Success) ---
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("DeleteWithContents", ctx).Return(nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	// --- Storage Client Mocks (Success) ---
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Delete", ctx).Return(nil).Once()

	// --- Messaging Client Mocks for Failure ---
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	expectedErr := errors.New("messaging subscription delete failed")
	mockSub1.On("Delete", ctx).Return(expectedErr).Once()
	// Topic 1 and 2 still attempted
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Delete", ctx).Return(nil).Once()
	// df-topic-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Topic", "df-topic-2")

	err := dfm.TeardownResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Teardown_MultipleManagersFail tests teardown failure with multiple managers failing.
func TestDataflowManager_Teardown_MultipleManagersFail(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	bqErr := errors.New("bigquery teardown failed")
	storeErr := errors.New("storage teardown failed")
	msgErr := errors.New("messaging teardown failed")

	// --- BigQuery Client Mocks (Failure) ---
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("DeleteWithContents", ctx).Return(bqErr).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	// --- Storage Client Mocks (Failure) ---
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Delete", ctx).Return(storeErr).Once()

	// --- Messaging Client Mocks (Failure) ---
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Delete", ctx).Return(msgErr).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Delete", ctx).Return(nil).Once() // This one might succeed
	// df-topic-2 is protected, so its delete should NOT be called
	mockMsgClient.AssertNotCalled(t, "Topic", "df-topic-2")

	err := dfm.TeardownResources(ctx, dataflowGroup) // Renamed function, passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), bqErr.Error())
	assert.Contains(t, err.Error(), storeErr.Error())
	assert.Contains(t, err.Error(), msgErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
}

// TestDataflowManager_Verify_Success tests successful verification.
func TestDataflowManager_Verify_Success(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Client Mocks for Verify ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(true, nil).Once()

	// --- Storage Client Mocks for Verify ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(&servicemanager.GCSBucket{
		CloudResource: servicemanager.CloudResource{Name: "df-bucket-1"},
		Location:      env.Location,
	}, nil).Once()

	// --- BigQuery Client Mocks for Verify ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(&servicemanager.BigQueryDataset{
		CloudResource: servicemanager.CloudResource{Name: "df-dataset-1"},
	}, nil).Once()
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "df-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(&servicemanager.BigQueryTable{
		CloudResource: servicemanager.CloudResource{Name: "df-table-1"},
		Dataset:       "df-dataset-1",
	}, nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	err := dfm.Verify(ctx, dataflowGroup) // Passed dataflowGroup
	require.NoError(t, err)

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}

// TestDataflowManager_Verify_MessagingManagerFails tests verify failure due to MessagingManager.
func TestDataflowManager_Verify_MessagingManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Client Mocks for Failure ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	expectedErr := errors.New("messaging topic verify failed")
	mockTopic1.On("Exists", ctx).Return(false, expectedErr).Once() // Make this fail

	// Other verify calls are still attempted in DataflowManager.Verify
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(true, nil).Once()

	// --- Storage Client Mocks (Success) ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(&servicemanager.GCSBucket{
		CloudResource: servicemanager.CloudResource{Name: "df-bucket-1"},
		Location:      env.Location,
	}, nil).Once()

	// --- BigQuery Client Mocks (Success) ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(&servicemanager.BigQueryDataset{
		CloudResource: servicemanager.CloudResource{Name: "df-dataset-1"},
	}, nil).Once()
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "df-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(&servicemanager.BigQueryTable{
		CloudResource: servicemanager.CloudResource{Name: "df-table-1"},
		Dataset:       "df-dataset-1",
	}, nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	err := dfm.Verify(ctx, dataflowGroup) // Passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}

// TestDataflowManager_Verify_StorageManagerFails tests verify failure due to StorageManager.
func TestDataflowManager_Verify_StorageManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Client Mocks (Success) ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(true, nil).Once()

	// --- Storage Client Mocks for Failure ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	expectedErr := errors.New("storage bucket verify failed")
	mockBucket1.On("Attrs", ctx).Return(nil, expectedErr).Once() // Make this fail

	// --- BigQuery Client Mocks (Success) ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(&servicemanager.BigQueryDataset{
		CloudResource: servicemanager.CloudResource{Name: "df-dataset-1"},
	}, nil).Once()
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "df-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(&servicemanager.BigQueryTable{
		CloudResource: servicemanager.CloudResource{Name: "df-table-1"},
		Dataset:       "df-dataset-1",
	}, nil).Once()
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	err := dfm.Verify(ctx, dataflowGroup) // Passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}

// TestDataflowManager_Verify_BigQueryManagerFails tests verify failure due to BigQueryManager.
func TestDataflowManager_Verify_BigQueryManagerFails(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	// --- Messaging Client Mocks (Success) ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(true, nil).Once()

	// --- Storage Client Mocks (Success) ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(&servicemanager.GCSBucket{
		CloudResource: servicemanager.CloudResource{Name: "df-bucket-1"},
		Location:      env.Location,
	}, nil).Once()

	// --- BigQuery Client Mocks for Failure ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(&servicemanager.BigQueryDataset{
		CloudResource: servicemanager.CloudResource{Name: "df-dataset-1"},
	}, nil).Once()
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "df-table-1").Return(mockTable1).Once()
	expectedErr := errors.New("bigquery table verify failed")
	mockTable1.On("Metadata", ctx).Return(nil, expectedErr).Once() // Make this fail
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	err := dfm.Verify(ctx, dataflowGroup) // Passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}

// TestDataflowManager_Verify_MultipleManagersFail tests verify failure with multiple managers failing.
func TestDataflowManager_Verify_MultipleManagersFail(t *testing.T) {
	dfm, mockMsgClient, mockStoreClient, mockBqClient, ctx, env, dataflowGroup, _ := setupDataflowManagerTest(t)

	msgErr := errors.New("messaging verify failed")
	storeErr := errors.New("storage verify failed")
	bqErr := errors.New("bigquery verify failed")

	// --- Messaging Client Mocks (Failure) ---
	mockMsgClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockTopic1 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(false, msgErr).Once() // Fail here
	mockTopic2 := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "df-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockSub1 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub2 := new(MockMessagingSubscription)
	mockMsgClient.On("Subscription", "df-sub-2").Return(mockSub2).Once()
	mockSub2.On("Exists", ctx).Return(true, nil).Once()

	// --- Storage Client Mocks (Failure) ---
	mockStoreClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockBucket1 := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket1).Once()
	mockBucket1.On("Attrs", ctx).Return(nil, storeErr).Once() // Fail here

	// --- BigQuery Client Mocks (Failure) ---
	mockBqClient.On("Validate", dataflowGroup.Resources).Return(nil).Once()
	mockDataset1 := new(MockBQDataset)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset1).Once()
	mockDataset1.On("Metadata", ctx).Return(&servicemanager.BigQueryDataset{
		CloudResource: servicemanager.CloudResource{Name: "df-dataset-1"},
	}, nil).Once()
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "df-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, bqErr).Once() // Fail here
	mockBqClient.On("Project").Return(env.ProjectID).Maybe()

	err := dfm.Verify(ctx, dataflowGroup) // Passed dataflowGroup
	require.Error(t, err)
	assert.Contains(t, err.Error(), msgErr.Error())
	assert.Contains(t, err.Error(), storeErr.Error())
	assert.Contains(t, err.Error(), bqErr.Error())

	mockMsgClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockSub2.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBucket1.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}
