// servicemanager_test.go
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

// --- Helper function to get a consistent test config ---

func getTestConfig() *servicemanager.TopLevelConfig {
	return &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-project",
		DefaultLocation:  "us-central1",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "test-project"},
		},
		Resources: servicemanager.ResourcesSpec{
			MessagingTopics: []servicemanager.MessagingTopicConfig{
				{Name: "topic1", ProducerService: "service-a"},
			},
			MessagingSubscriptions: []servicemanager.MessagingSubscriptionConfig{}, // Ensure it's not nil
			GCSBuckets: []servicemanager.GCSBucket{
				{Name: "bucket1", AccessingServices: []string{"service-a"}},
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{Name: "dataset1", Description: "Test Dataset 1"},
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{
					Name:                   "table1",
					Dataset:                "dataset1",
					AccessingServices:      []string{"service-b"},
					Description:            "Test Table for Service B",
					SchemaSourceType:       "go_struct",
					SchemaSourceIdentifier: "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading",
					TimePartitioningField:  "original_mqtt_time",
					TimePartitioningType:   "DAY",
				},
			},
		},
		Dataflows: []servicemanager.DataflowSpec{
			{
				Name:     "dataflow-ephemeral",
				Services: []string{"service-a"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
			},
			{
				Name:     "dataflow-permanent",
				Services: []string{"service-b"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyPermanent,
				},
			},
		},
	}
}

// NOTE: Mock implementations for MessagingClient, StorageClient, BQClient, and error helpers
// are assumed to exist in other files within this `servicemanager_test` package
// (e.g., bigquery_test.go, messaging_test.go).

// --- Test Cases for ServiceManager ---

func TestNewServiceManagerFromClients(t *testing.T) {
	logger := zerolog.New(io.Discard)
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(getTestConfig())
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		require.NotNil(t, sm)
	})
}

func TestServiceManager_SetupAll(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"

	t.Run("Success", func(t *testing.T) {
		// Arrange: Mock all clients and their expected calls
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// Messaging expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Exists", mock.Anything).Return(false, nil)
		mockTopic.On("ID").Return("topic1")
		mockMsgClient.On("CreateTopic", mock.Anything, "topic1").Return(mockTopic, nil)

		// Storage expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		mockBucket.On("Attrs", mock.Anything).Return(nil, errors.New("storage: bucket doesn't exist"))
		mockBucket.On("Create", mock.Anything, "test-project", mock.AnythingOfType("*servicemanager.BucketAttributes")).Return(nil)

		// BigQuery expectations for ALL resources
		mockDataset := new(MockBQDataset)
		mockBqClient.On("Project").Return("test-project")
		mockBqClient.On("Dataset", "dataset1").Return(mockDataset)
		mockDataset.On("Metadata", mock.Anything).Return(nil, newNotFoundError("dataset", "dataset1"))
		mockDataset.On("Create", mock.Anything, mock.Anything).Return(nil)

		mockTable := new(MockBQTable)
		mockDataset.On("Table", "table1").Return(mockTable)
		mockTable.On("Metadata", mock.Anything).Return(nil, newNotFoundError("table", "table1"))
		mockTable.On("Create", mock.Anything, mock.Anything).Return(nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)

		provResources, err := sm.SetupAll(ctx, testCfg, env)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, provResources)
		assert.Len(t, provResources.PubSubTopics, 1)
		assert.Len(t, provResources.GCSBuckets, 1)
		assert.Len(t, provResources.BigQueryDatasets, 1)
		assert.Len(t, provResources.BigQueryTables, 1)

		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})
}

func TestServiceManager_TeardownAll(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"

	t.Run("Success", func(t *testing.T) {
		// Arrange
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// Messaging expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Delete", mock.Anything).Return(nil)

		// Storage expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		// FIX: Added missing mock expectation for the Attrs() call.
		// The teardown logic checks for existence before deleting.
		mockBucket.On("Attrs", mock.Anything).Return(&servicemanager.BucketAttributes{}, nil)
		mockBucket.On("Delete", mock.Anything).Return(nil)

		// BigQuery teardown expectations
		mockDataset := new(MockBQDataset)
		mockBqClient.On("Project").Return("test-project")
		mockBqClient.On("Dataset", "dataset1").Return(mockDataset)
		mockTable := new(MockBQTable)
		mockDataset.On("Table", "table1").Return(mockTable)
		mockTable.On("Delete", mock.Anything).Return(nil)
		mockDataset.On("Delete", mock.Anything).Return(nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownAll(ctx, testCfg, env)

		// Assert
		assert.NoError(t, err)
		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
	})
}

func TestServiceManager_SetupDataflow(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"

	t.Run("Setup dataflow for service-a (messaging and storage)", func(t *testing.T) {
		dataflowName := "dataflow-ephemeral"
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// Expectations for service-a resources ONLY
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Exists", mock.Anything).Return(false, nil)
		mockTopic.On("ID").Return("topic1")
		mockMsgClient.On("CreateTopic", mock.Anything, "topic1").Return(mockTopic, nil)

		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		mockBucket.On("Attrs", mock.Anything).Return(nil, errors.New("storage: bucket doesn't exist"))
		mockBucket.On("Create", mock.Anything, "test-project", mock.AnythingOfType("*servicemanager.BucketAttributes")).Return(nil)

		// The BQ Manager is always called, so we must expect the Project() check.
		mockBqClient.On("Project").Return("test-project")

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		provResources, err := sm.SetupDataflow(ctx, env, dataflowName)

		// Assert
		require.NoError(t, err)
		assert.Len(t, provResources.PubSubTopics, 1, "Should set up topic for service-a")
		assert.Len(t, provResources.GCSBuckets, 1, "Should set up bucket for service-a")
		assert.Empty(t, provResources.BigQueryTables, "Should NOT set up BQ table for service-a")
		assert.Empty(t, provResources.BigQueryDatasets, "Should NOT set up BQ dataset for service-a")

		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
		// Verify that Dataset() was NOT called, since there are no BQ resources for this dataflow
		mockBqClient.AssertNotCalled(t, "Dataset", mock.Anything)
	})

	t.Run("Setup dataflow for service-b (bigquery)", func(t *testing.T) {
		dataflowName := "dataflow-permanent"
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// Expectations for service-b (BigQuery)
		mockDataset := new(MockBQDataset)
		mockBqClient.On("Project").Return("test-project")
		mockBqClient.On("Dataset", "dataset1").Return(mockDataset)
		mockDataset.On("Metadata", mock.Anything).Return(nil, newNotFoundError("dataset", "dataset1"))
		mockDataset.On("Create", mock.Anything, mock.Anything).Return(nil)

		mockTable := new(MockBQTable)
		mockDataset.On("Table", "table1").Return(mockTable)
		mockTable.On("Metadata", mock.Anything).Return(nil, newNotFoundError("table", "table1"))
		mockTable.On("Create", mock.Anything, mock.Anything).Return(nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		provResources, err := sm.SetupDataflow(ctx, env, dataflowName)

		// Assert
		require.NoError(t, err)
		assert.Empty(t, provResources.PubSubTopics, "Should NOT set up topics for service-b")
		assert.Empty(t, provResources.GCSBuckets, "Should NOT set up buckets for service-b")
		assert.Len(t, provResources.BigQueryDatasets, 1, "Should set up dataset for service-b")
		assert.Len(t, provResources.BigQueryTables, 1, "Should set up table for service-b")

		mockBqClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
		mockMsgClient.AssertNotCalled(t, "Topic", mock.Anything)
		mockStoreClient.AssertNotCalled(t, "Bucket", mock.Anything)
	})
}

func TestServiceManager_TeardownDataflow(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"

	t.Run("Success for Ephemeral", func(t *testing.T) {
		// Arrange
		dataflowName := "dataflow-ephemeral"
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// Messaging teardown expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Delete", mock.Anything).Return(nil)

		// Storage teardown expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		// FIX: Added missing mock expectation for the Attrs() call.
		mockBucket.On("Attrs", mock.Anything).Return(&servicemanager.BucketAttributes{}, nil)
		mockBucket.On("Delete", mock.Anything).Return(nil)

		// The BQ Manager is always called, so we must expect the Project() check.
		mockBqClient.On("Project").Return("test-project")

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownDataflow(ctx, env, dataflowName)

		// Assert
		assert.NoError(t, err)
		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
		mockBqClient.AssertNotCalled(t, "Dataset", mock.Anything)
	})

	t.Run("Skipped for Permanent", func(t *testing.T) {
		// Arrange
		dataflowName := "dataflow-permanent"
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownDataflow(ctx, env, dataflowName)

		// Assert
		assert.NoError(t, err)
		mockMsgClient.AssertNotCalled(t, "Topic", mock.Anything)
		mockStoreClient.AssertNotCalled(t, "Bucket", mock.Anything)
	})
}
