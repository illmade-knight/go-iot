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
		Services: []servicemanager.ServiceSpec{
			{Name: "service-a"},
			{Name: "service-b"},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:         "dataflow-ephemeral",
				ServiceNames: []string{"service-a"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager.ResourcesSpec{
					Topics:     []servicemanager.TopicConfig{{Name: "topic1"}},
					GCSBuckets: []servicemanager.GCSBucket{{Name: "bucket1"}},
				},
			},
			{
				Name:         "dataflow-permanent",
				ServiceNames: []string{"service-b"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyPermanent,
				},
				Resources: servicemanager.ResourcesSpec{
					BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: "dataset1"}},
				},
			},
		},
	}
}

// --- Test Cases ---

func TestServiceManager_SetupDataflow(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	testCfg := getTestConfig()
	env := "test"
	dataflowName := "dataflow-ephemeral"

	// Arrange: Create MOCK CLIENTS and define expectations on them.
	mockMsgClient := new(MockMessagingClient)
	mockStoreClient := new(MockStorageClient)
	mockBqClient := new(MockBQClient)

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
	require.NoError(t, err)

	dataflowSpec, err := servicesDef.GetDataflow(dataflowName)
	require.NoError(t, err)

	// Messaging client expectations
	mockTopic := new(MockMessagingTopic)
	mockMsgClient.On("Validate", dataflowSpec.Resources).Return(nil)
	mockMsgClient.On("Topic", "topic1").Return(mockTopic)
	mockTopic.On("Exists", ctx).Return(false, nil)
	mockMsgClient.On("CreateTopic", ctx, "topic1").Return(mockTopic, nil)
	mockTopic.On("ID").Return("topic1")

	// Storage client expectations
	mockBucket := new(MockBucketHandle)
	mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
	mockBucket.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist"))
	mockBucket.On("Create", ctx, "test-project", mock.Anything).Return(nil)

	// BigQuery client will be used, but since this dataflow has no BQ resources,
	// we only expect the Project() check from the BQ Manager's Setup method.
	mockBqClient.On("Project").Return("test-project").Maybe()

	// Act: Create the ServiceManager with the MOCK CLIENTS (via the correct constructor)
	// and call the method under test.
	sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
	require.NoError(t, err)

	provResources, err := sm.SetupDataflow(ctx, env, dataflowName)

	// Assert
	require.NoError(t, err)
	assert.Len(t, provResources.Topics, 1)
	assert.Len(t, provResources.GCSBuckets, 1)
	assert.Empty(t, provResources.BigQueryDatasets) // No BQ resources in this dataflow

	// Verify that the mock clients were called as expected.
	mockMsgClient.AssertExpectations(t)
	mockStoreClient.AssertExpectations(t)
	mockBqClient.AssertExpectations(t)
	mockTopic.AssertExpectations(t)
	mockBucket.AssertExpectations(t)
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

		// Messaging teardown expectations
		mockTopic := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic)
		mockTopic.On("Delete", ctx).Return(nil)

		// Storage teardown expectations
		mockBucket := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket1").Return(mockBucket)
		mockBucket.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil)
		mockBucket.On("Delete", ctx).Return(nil)

		mockBqClient.On("Project").Return("test-project")

		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)

		err = sm.TeardownDataflow(ctx, env, dataflowName)
		assert.NoError(t, err)

		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
		mockTopic.AssertExpectations(t)
		mockBucket.AssertExpectations(t)
	})

	t.Run("Skipped for Permanent", func(t *testing.T) {
		dataflowName := "dataflow-permanent"

		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)

		err = sm.TeardownDataflow(ctx, env, dataflowName)
		assert.NoError(t, err)

		// Assert that NO teardown methods were called on the clients.
		mockMsgClient.AssertNotCalled(t, "Topic", mock.Anything)
		mockStoreClient.AssertNotCalled(t, "Bucket", mock.Anything)
		mockBqClient.AssertNotCalled(t, "Dataset", mock.Anything)
	})
}
