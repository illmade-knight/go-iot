package servicemanager_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// This test file assumes the mock definitions for BQClient, MessagingClient, StorageClient
// and their sub-components (MockBQDataset, MockMessagingTopic, etc.) exist in other
// test files within the `servicemanager_test` package.

// --- Test Helper ---

func getTestDataflowResourceGroup() *servicemanager.ResourceGroup {
	return &servicemanager.ResourceGroup{
		Name: "test-dataflow",
		Resources: servicemanager.ResourcesSpec{
			Topics:     []servicemanager.TopicConfig{{Name: "df-topic-1"}},
			GCSBuckets: []servicemanager.GCSBucket{{Name: "df-bucket-1"}},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{Name: "df-dataset-1"},
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{
					Name:                   "df-table-1",
					Dataset:                "df-dataset-1",
					SchemaSourceType:       "go_struct",
					SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings",
				},
			},
		},
	}
}

// --- Test Cases ---

func TestDataflowManager_Setup_WithMockClients(t *testing.T) {
	// Arrange
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	spec := getTestDataflowResourceGroup()
	projectID := "test-project-df"
	location := "us-east1"
	labels := map[string]string{"env": "test"}

	// 1. Create Mock Clients
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	// 2. Set expectations on the Mock Clients, using the explicit `ctx` from the test.
	mockTopic := new(MockMessagingTopic)
	// The Validate method is called within the MessagingManager's Setup.
	// We mock it to return nil, indicating success. Using mock.Anything
	// makes the test less brittle to changes in the exact resource spec.
	mockMsgClient.On("Validate", mock.Anything).Return(nil)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic)
	mockTopic.On("Exists", ctx).Return(false, nil)
	mockMsgClient.On("CreateTopic", ctx, "df-topic-1").Return(mockTopic, nil)
	mockTopic.On("ID").Return("df-topic-1")

	mockBucket := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket)
	mockBucket.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist"))
	mockBucket.On("Create", ctx, projectID, mock.Anything).Return(nil)

	mockTable := new(MockBQTable)
	mockTable.On("Metadata", ctx).Return(nil, errors.New("table notFound"))
	mockTable.On("Create", ctx, mock.Anything).Return(nil)
	mockDataset := new(MockBQDataset)
	mockDataset.On("Metadata", ctx).Return(nil, errors.New("dataset notFound"))
	mockDataset.On("Create", ctx, mock.Anything).Return(nil)
	mockDataset.On("Table", "df-table-1").Return(mockTable)
	mockBqClient.On("Project").Return(projectID)
	mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset)

	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings": types.GardenMonitorReadings{},
	}

	// 3. Create REAL Managers with the MOCK Clients
	msgManager, err := servicemanager.NewMessagingManager(mockMsgClient, logger)
	require.NoError(t, err)
	storeManager, err := servicemanager.NewStorageManager(mockStoreClient, logger)
	require.NoError(t, err)
	bqManager, err := servicemanager.NewBigQueryManager(mockBqClient, logger, schemaRegistry)
	require.NoError(t, err)

	// 4. Create the DataflowManager using the REAL managers
	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		msgManager, storeManager, bqManager, spec, projectID, location, labels, "test", logger,
	)
	require.NoError(t, err)

	// Act
	provResources, err := dfm.Setup(ctx)

	// Assert
	require.NoError(t, err)
	assert.NotNil(t, provResources)
	assert.Len(t, provResources.Topics, 1)
	assert.Len(t, provResources.GCSBuckets, 1)
	assert.Len(t, provResources.BigQueryDatasets, 1)
	assert.Len(t, provResources.BigQueryTables, 1)

	// Verify that all expected mock calls were made on the clients
	mockMsgClient.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockDataset.AssertExpectations(t)
	mockTable.AssertExpectations(t)
}

// TestDataflowManager_Teardown_WithMockClients is the new test suite to validate the Teardown method.
func TestDataflowManager_Teardown_WithMockClients(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	spec := getTestDataflowResourceGroup()
	projectID := "test-project-df"

	t.Run("Teardown Succeeds", func(t *testing.T) {
		// Arrange
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)

		// Mock successful deletions
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic)
		mockTopic.On("Delete", ctx).Return(nil)

		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket)
		mockBucket.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil) // Exists
		mockBucket.On("Delete", ctx).Return(nil)

		mockTable := new(MockBQTable)
		mockTable.On("Delete", ctx).Return(nil)
		mockDataset := new(MockBQDataset)
		mockDataset.On("Delete", ctx).Return(nil)
		mockDataset.On("Table", "df-table-1").Return(mockTable)
		mockBqClient.On("Project").Return(projectID)
		mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset)

		msgManager, _ := servicemanager.NewMessagingManager(mockMsgClient, logger)
		storeManager, _ := servicemanager.NewStorageManager(mockStoreClient, logger)
		bqManager, _ := servicemanager.NewBigQueryManager(mockBqClient, logger, nil)

		dfm, err := servicemanager.NewDataflowManagerFromManagers(
			msgManager, storeManager, bqManager, spec, projectID, "", nil, "test", logger,
		)
		require.NoError(t, err)

		// Act
		err = dfm.Teardown(ctx, false)

		// Assert
		require.NoError(t, err)
		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
	})

	t.Run("Teardown Returns Aggregated Error on Partial Failure", func(t *testing.T) {
		// Arrange
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)

		// Mock GCS & Messaging deletions to SUCCEED
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket)
		mockBucket.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil)
		mockBucket.On("Delete", ctx).Return(nil)

		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic)
		mockTopic.On("Delete", ctx).Return(nil)

		// Mock BigQuery table deletion to FAIL
		mockTable := new(MockBQTable)
		mockTable.On("Delete", ctx).Return(errors.New("mock BQ table delete error"))

		mockDataset := new(MockBQDataset)
		// The dataset deletion should still be attempted and succeed
		mockDataset.On("Delete", ctx).Return(nil)
		mockDataset.On("Table", "df-table-1").Return(mockTable)

		mockBqClient.On("Project").Return(projectID)
		mockBqClient.On("Dataset", "df-dataset-1").Return(mockDataset)

		msgManager, _ := servicemanager.NewMessagingManager(mockMsgClient, logger)
		storeManager, _ := servicemanager.NewStorageManager(mockStoreClient, logger)
		// The nil for schema registry is fine for this test as we aren't calling setup.
		bqManager, _ := servicemanager.NewBigQueryManager(mockBqClient, logger, nil)

		dfm, err := servicemanager.NewDataflowManagerFromManagers(
			msgManager, storeManager, bqManager, spec, projectID, "", nil, "test", logger,
		)
		require.NoError(t, err)

		// Act
		err = dfm.Teardown(ctx, false)

		// Assert
		require.Error(t, err, "Teardown should return an error when BigQuery fails")
		assert.Contains(t, err.Error(), "BigQuery teardown failed", "Error message should identify the failing manager")
		assert.Contains(t, err.Error(), "mock BQ table delete error", "Error message should contain the root cause from the sub-manager")

		// Verify that all teardown operations were still attempted
		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})
}
