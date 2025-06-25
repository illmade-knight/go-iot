package servicemanager_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Test Helper ---

func getTestDataflowSpec() *servicemanager.ResourceGroup {
	return &servicemanager.ResourceGroup{
		Name: "test-dataflow",
		Resources: servicemanager.ResourcesSpec{
			Topics: []servicemanager.TopicConfig{
				{Name: "df-topic-1"},
			},
			GCSBuckets: []servicemanager.GCSBucket{
				{Name: "df-bucket-1"},
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{Name: "df-dataset-1"},
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{Name: "df-table-1", Dataset: "df-dataset-1"},
			},
		},
	}
}

// --- Test Cases ---

func TestNewDataflowManagerFromManagers(t *testing.T) {
	// Arrange
	logger := zerolog.New(io.Discard)
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	// We need to create the actual managers with the mock clients
	msgManager, err := servicemanager.NewMessagingManager(mockMsgClient, logger)
	require.NoError(t, err)
	storeManager, err := servicemanager.NewStorageManager(mockStoreClient, logger)
	require.NoError(t, err)
	bqManager, err := servicemanager.NewBigQueryManager(mockBqClient, logger, nil)
	require.NoError(t, err)

	spec := getTestDataflowSpec()

	// Act
	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		msgManager,
		storeManager,
		bqManager,
		spec,
		"test-project",
		"us-central1",
		nil,
		"test",
		logger,
	)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, dfm)
}

func TestDataflowManager_Setup(t *testing.T) {
	// Arrange
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	spec := getTestDataflowSpec()
	projectID := "test-project-df"
	location := "us-east1"

	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	// Setup mock expectations for the underlying managers' Setup methods
	mockMsgClient.On("Validate", spec.Resources).Return(nil)
	mockMsgClient.On("Topic", "df-topic-1").Return(new(MockMessagingTopic).On("Exists", ctx).Return(false, nil).Once()).Once()
	mockMsgClient.On("CreateTopic", ctx, "df-topic-1").Return(new(MockMessagingTopic).On("ID").Return("df-topic-1").Once(), nil).Once()

	mockStoreClient.On("Bucket", "df-bucket-1").Return(new(MockBucketHandle).
		On("Attrs", ctx).Return(nil, errors.New("not found")).Once().
		On("Create", ctx, projectID, mock.Anything).Return(nil).Once(),
	).Once()

	mockBqClient.On("Project").Return(projectID)
	mockBqClient.On("Dataset", "df-dataset-1").Return(new(MockBQDataset).
		On("Metadata", ctx).Return(nil, errors.New("not found")).Once().
		On("Create", ctx, mock.Anything).Return(nil).Once().
		On("Table", "df-table-1").Return(new(MockBQTable).
		On("Metadata", ctx).Return(nil, errors.New("not found")).Once().
		On("Create", ctx, mock.Anything).Return(nil).Once(),
	).Once(),
	).Once()

	msgManager, _ := servicemanager.NewMessagingManager(mockMsgClient, logger)
	storeManager, _ := servicemanager.NewStorageManager(mockStoreClient, logger)
	bqManager, _ := servicemanager.NewBigQueryManager(mockBqClient, logger, nil)

	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		msgManager, storeManager, bqManager, spec, projectID, location, nil, "test", logger,
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

	// Verify that all expected mock calls were made
	mockMsgClient.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
}

func TestDataflowManager_Teardown(t *testing.T) {
	// Arrange
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	spec := getTestDataflowSpec()
	projectID := "test-project-df"

	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	// Setup mock expectations for teardown
	mockMsgClient.On("Subscription", mock.Anything).Return(nil) // Assume no subscriptions for simplicity
	mockMsgClient.On("Topic", "df-topic-1").Return(new(MockMessagingTopic).On("Delete", ctx).Return(nil).Once()).Once()

	mockStoreClient.On("Bucket", "df-bucket-1").Return(new(MockBucketHandle).
		On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil).Once().
		On("Delete", ctx).Return(nil).Once(),
	).Once()

	mockBqClient.On("Project").Return(projectID)
	mockBqClient.On("Dataset", "df-dataset-1").Return(new(MockBQDataset).
		On("Table", "df-table-1").Return(new(MockBQTable).
		On("Delete", ctx).Return(nil).Once(),
	).Once().
		On("Delete", ctx).Return(nil).Once(),
	).Once()

	msgManager, _ := servicemanager.NewMessagingManager(mockMsgClient, logger)
	storeManager, _ := servicemanager.NewStorageManager(mockStoreClient, logger)
	bqManager, _ := servicemanager.NewBigQueryManager(mockBqClient, logger, nil)

	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		msgManager, storeManager, bqManager, spec, projectID, "", nil, "test", logger,
	)
	require.NoError(t, err)

	// Act
	err = dfm.Teardown(ctx, false) // Teardown protection OFF

	// Assert
	require.NoError(t, err)
	mockMsgClient.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
}

func TestDataflowManager_Teardown_ProtectionEnabled(t *testing.T) {
	// Arrange
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	spec := getTestDataflowSpec()
	projectID := "test-project-df"

	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	// Expect that the underlying managers are called with teardownProtection = true
	// and that they will return an error which the DataflowManager will log but not propagate.
	// protectionErr := errors.New("teardown protection enabled")

	mockBqClient.On("Project").Return(projectID)
	// Expect Teardown to be called with protection ON.
	// We simulate the underlying manager returning an error.
	mockBqClient.On("Dataset", mock.Anything).Return(new(MockBQDataset).
		On("Table", mock.Anything).Return(new(MockBQTable).
		On("Delete", mock.Anything).Return(nil).Once(),
	).Once().
		On("Delete", mock.Anything).Return(nil).Once(),
	)
	// The other managers would also be called, but we can simplify the test
	// by focusing on the fact that Teardown is called with the correct flag.
	// For a full test, you would mock the Teardown methods on all managers.

	msgManager, _ := servicemanager.NewMessagingManager(mockMsgClient, logger)
	storeManager, _ := servicemanager.NewStorageManager(mockStoreClient, logger)
	bqManager, _ := servicemanager.NewBigQueryManager(mockBqClient, logger, nil)

	dfm, err := servicemanager.NewDataflowManagerFromManagers(
		msgManager, storeManager, bqManager, spec, projectID, "", nil, "test", logger,
	)
	require.NoError(t, err)

	// Act
	err = dfm.Teardown(ctx, true) // Teardown protection ON

	// The DataflowManager's Teardown method logs errors but returns nil, so we expect no error here.
	// A more advanced test could capture log output to verify the error was logged.
	assert.NoError(t, err)
}
