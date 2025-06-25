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
	mockMsgClient.On("Validate", spec.Resources).Return(nil)
	mockMsgClient.On("Topic", "df-topic-1").Return(mockTopic)
	mockTopic.On("Exists", ctx).Return(false, nil)
	mockMsgClient.On("CreateTopic", ctx, "df-topic-1").Return(mockTopic, nil)
	mockTopic.On("ID").Return("df-topic-1")

	mockBucket := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "df-bucket-1").Return(mockBucket)
	mockBucket.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist"))
	mockBucket.On("Create", ctx, projectID, mock.Anything).Return(nil)

	// FIX: Return an error string that the BQManager's notFound check will recognize.
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
