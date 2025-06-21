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

// --- MockServicesDefinition (re-usable mock) ---

type MockServicesDefinition struct {
	mock.Mock
}

func (m *MockServicesDefinition) GetTopLevelConfig() (*servicemanager.TopLevelConfig, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.TopLevelConfig), args.Error(1)
}

func (m *MockServicesDefinition) GetService(name string) (*servicemanager.ServiceSpec, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.ServiceSpec), args.Error(1)
}

func (m *MockServicesDefinition) GetDataflow(name string) (*servicemanager.DataflowSpec, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.DataflowSpec), args.Error(1)
}

func (m *MockServicesDefinition) GetProjectID(environment string) (string, error) {
	args := m.Called(environment)
	return args.String(0), args.Error(1)
}

// --- Helper function to get a consistent test config ---

func getTestConfig() *servicemanager.TopLevelConfig {
	return &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-project",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "test-project"},
		},
		Resources: servicemanager.ResourcesSpec{
			PubSubTopics: []servicemanager.MessagingTopicConfig{
				{Name: "topic1", ProducerService: "service-a"},
			},
			GCSBuckets: []servicemanager.GCSBucket{
				{Name: "bucket1", AccessingServices: []string{"service-a"}},
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{Name: "dataset1"},
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{Name: "table1", Dataset: "dataset1", AccessingServices: []string{"service-b"}},
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

// --- Test Cases for ServiceManager ---

func TestNewServiceManagerFromClients(t *testing.T) {
	logger := zerolog.New(io.Discard)
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)
	mockSvcsDef := new(MockServicesDefinition)

	t.Run("Success", func(t *testing.T) {
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
		require.NoError(t, err)
		require.NotNil(t, sm)
	})

	t.Run("Nil MessagingClient", func(t *testing.T) {
		_, err := servicemanager.NewServiceManagerFromClients(nil, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
		assert.Error(t, err)
	})

	t.Run("Nil StorageClient", func(t *testing.T) {
		_, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, nil, mockBqClient, mockSvcsDef, nil, logger)
		assert.Error(t, err)
	})

	t.Run("Nil BQClient", func(t *testing.T) {
		_, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, nil, mockSvcsDef, nil, logger)
		assert.Error(t, err)
	})

	t.Run("Nil ServicesDefinition", func(t *testing.T) {
		_, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, nil, nil, logger)
		assert.Error(t, err)
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
		mockBqClient := new(MockBQClient) // Setup for BQ is not implemented yet in ServiceManager
		mockSvcsDef := new(MockServicesDefinition)

		// Messaging expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Exists", mock.Anything).Return(false, nil)
		mockMsgClient.On("CreateTopic", mock.Anything, "topic1").Return(mockTopic, nil)
		mockTopic.On("ID").Return("topic1") // Needed for logging

		// Storage expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		mockBucket.On("Attrs", mock.Anything).Return(nil, errors.New("storage: bucket doesn't exist"))
		mockBucket.On("Create", mock.Anything, "test-project", mock.AnythingOfType("*servicemanager.BucketAttributes")).Return(nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
		require.NoError(t, err)

		provResources, err := sm.SetupAll(ctx, testCfg, env)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, provResources)
		assert.Len(t, provResources.PubSubTopics, 1)
		assert.Len(t, provResources.GCSBuckets, 1)

		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
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
		mockSvcsDef := new(MockServicesDefinition)

		// Messaging expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Delete", mock.Anything).Return(nil)

		// Storage expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		mockBucket.On("Attrs", mock.Anything).Return(&servicemanager.BucketAttributes{}, nil)
		mockBucket.On("Delete", mock.Anything).Return(nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
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
	dataflowName := "dataflow-ephemeral"

	// Arrange: Mocks
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)
	mockSvcsDef := new(MockServicesDefinition)

	// Arrange: Get dataflow spec and set expectations
	dataflowProvider, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
	require.NoError(t, err)
	dfSpec, err := dataflowProvider.GetDataflow(dataflowName)
	require.NoError(t, err)

	mockSvcsDef.On("GetDataflow", dataflowName).Return(dfSpec, nil)
	mockSvcsDef.On("GetTopLevelConfig").Return(testCfg, nil)

	// Arrange: Set expectations on clients for ONLY the dataflow's resources
	// Messaging: service-a uses topic1
	mockTopic := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "topic1").Return(mockTopic)
	mockTopic.On("Exists", mock.Anything).Return(false, nil)
	mockMsgClient.On("CreateTopic", mock.Anything, "topic1").Return(mockTopic, nil)
	mockTopic.On("ID").Return("topic1")

	// Storage: service-a uses bucket1
	mockBucket := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
	mockBucket.On("Attrs", mock.Anything).Return(nil, errors.New("storage: bucket doesn't exist"))
	mockBucket.On("Create", mock.Anything, "test-project", mock.AnythingOfType("*servicemanager.BucketAttributes")).Return(nil)

	// BigQuery: service-a uses no BQ resources, so no expectations on mockBqClient

	// Act
	sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
	require.NoError(t, err)
	_, err = sm.SetupDataflow(ctx, env, dataflowName)

	// Assert
	assert.NoError(t, err)
	mockSvcsDef.AssertExpectations(t)
	mockMsgClient.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t) // Should have no calls
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
		mockSvcsDef := new(MockServicesDefinition)

		dataflowProvider, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)
		dfSpec, err := dataflowProvider.GetDataflow(dataflowName)
		require.NoError(t, err)

		mockSvcsDef.On("GetDataflow", dataflowName).Return(dfSpec, nil)
		mockSvcsDef.On("GetTopLevelConfig").Return(testCfg, nil)

		// Messaging teardown expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Delete", mock.Anything).Return(nil)

		// Storage teardown expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		mockBucket.On("Attrs", mock.Anything).Return(&servicemanager.BucketAttributes{}, nil)
		mockBucket.On("Delete", mock.Anything).Return(nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownDataflow(ctx, env, dataflowName)

		// Assert
		assert.NoError(t, err)
		mockSvcsDef.AssertExpectations(t)
		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
	})

	t.Run("Skipped for Permanent", func(t *testing.T) {
		// Arrange
		dataflowName := "dataflow-permanent"
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		mockSvcsDef := new(MockServicesDefinition)

		dataflowProvider, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)
		dfSpec, err := dataflowProvider.GetDataflow(dataflowName)
		require.NoError(t, err)

		mockSvcsDef.On("GetDataflow", dataflowName).Return(dfSpec, nil)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, mockSvcsDef, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownDataflow(ctx, env, dataflowName)

		// Assert
		assert.NoError(t, err)
		mockSvcsDef.AssertExpectations(t)
		mockMsgClient.AssertNotCalled(t, "Topic", mock.Anything)
		mockStoreClient.AssertNotCalled(t, "Bucket", mock.Anything)
	})
}
