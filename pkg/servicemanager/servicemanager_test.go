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

// This function now returns a config where resources are scoped within each dataflow.
func getTestConfig() *servicemanager.TopLevelConfig {
	return &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-project",
		DefaultLocation:  "us-central1",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {
				ProjectID:          "test-project",
				TeardownProtection: false,
			},
			"prod": {
				ProjectID:          "prod-project",
				TeardownProtection: true,
			},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:         "dataflow-ephemeral",
				ServiceNames: []string{"service-a"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{Name: "topic1", ProducerService: "service-a"},
					},
					GCSBuckets: []servicemanager.GCSBucket{
						{Name: "bucket1", AccessingServices: []string{"service-a"}},
					},
				},
			},
			{
				Name:         "dataflow-permanent",
				ServiceNames: []string{"service-b"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyPermanent,
				},
				Resources: servicemanager.ResourcesSpec{
					BigQueryDatasets: []servicemanager.BigQueryDataset{
						{Name: "dataset1", Description: "Test Dataset 1"},
					},
					BigQueryTables: []servicemanager.BigQueryTable{
						{Name: "table1", Dataset: "dataset1", AccessingServices: []string{"service-b"}},
					},
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
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(getTestConfig())
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		require.NotNil(t, sm)
	})
}

func TestServiceManager_SetupDataflow(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"
	dataflowName := "dataflow-ephemeral"

	// Arrange
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
	require.NoError(t, err)

	// Since we are setting up a specific dataflow, we expect calls to the underlying
	// managers with the resources ONLY from that dataflow.
	dataflowSpec, err := servicesDef.GetDataflow(dataflowName)
	require.NoError(t, err)

	// Messaging expectations
	mockMsgClient.On("Validate", dataflowSpec.Resources).Return(nil).Once()
	mockTopic := new(MockMessagingTopic)
	mockMsgClient.On("Topic", "topic1").Return(mockTopic).Once()
	mockTopic.On("Exists", ctx).Return(false, nil).Once()
	mockTopic.On("ID").Return("topic1").Once()
	mockMsgClient.On("CreateTopic", ctx, "topic1").Return(mockTopic, nil).Once()

	// Storage expectations
	mockBucket := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "bucket1").Return(mockBucket).Once()
	mockBucket.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist")).Once()
	mockBucket.On("Create", ctx, "test-project", mock.Anything).Return(nil).Once()

	// BigQuery client will be created but no setup methods called
	mockBqClient.On("Project").Return("test-project").Maybe()

	// Act
	sm, err := servicemanager.NewServiceManager(ctx, servicesDef, env, nil, logger)
	require.NoError(t, err)
	provResources, err := sm.SetupDataflow(ctx, env, dataflowName)

	// Assert
	require.NoError(t, err)
	assert.Len(t, provResources.Topics, 1)
	assert.Len(t, provResources.GCSBuckets, 1)
	assert.Empty(t, provResources.BigQueryTables)

	mockMsgClient.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	// Verify that the BQ manager was NOT asked to set up any datasets.
	mockBqClient.AssertNotCalled(t, "Dataset", mock.Anything)
}

func TestServiceManager_TeardownDataflow(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"

	t.Run("Success for Ephemeral", func(t *testing.T) {
		dataflowName := "dataflow-ephemeral"

		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		_, err = servicesDef.GetDataflow(dataflowName)
		require.NoError(t, err)

		// Messaging teardown expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic).Once()
		mockTopic.On("Delete", ctx).Return(nil).Once()

		// Storage teardown expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket).Once()
		mockBucket.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil).Once()
		mockBucket.On("Delete", ctx).Return(nil).Once()

		mockBqClient.On("Project").Return("test-project").Maybe()

		// Act
		sm, err := servicemanager.NewServiceManager(ctx, servicesDef, env, nil, logger)
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
		dataflowName := "dataflow-permanent"

		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		mockBqClient.On("Project").Return("test-project").Maybe()

		// Act
		sm, err := servicemanager.NewServiceManager(ctx, servicesDef, env, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownDataflow(ctx, env, dataflowName)

		// Assert
		assert.NoError(t, err)
		// Verify no teardown methods were called
		mockMsgClient.AssertNotCalled(t, "Topic", mock.Anything)
		mockStoreClient.AssertNotCalled(t, "Bucket", mock.Anything)
	})
}
