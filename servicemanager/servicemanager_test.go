package servicemanager_test

import (
	"context"
	"errors"
	servicemanager2 "github.com/illmade-knight/go-iot/servicemanager"
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Helper ---

// getTestConfigWithBQ provides a more complex configuration for testing teardown scenarios,
// including dataflows with different resource types to ensure managers are called correctly.
func getTestConfigWithBQ() *servicemanager2.MicroserviceArchitecture {
	return &servicemanager2.MicroserviceArchitecture{
		Environment: servicemanager2.Environment{
			Name:      "default",
			ProjectID: "ignore-project",
			Location:  "eu-west1",
		},
		DeploymentEnvironments: map[string]servicemanager2.Environment{
			"test": {
				Name:               "test",
				ProjectID:          "test-project",
				TeardownProtection: false,
			},
		},
		Dataflows: map[string]servicemanager2.ResourceGroup{
			"ephemeral-topics": {
				Name: "dataflow-ephemeral-topics",
				Lifecycle: &servicemanager2.LifecyclePolicy{
					Strategy: servicemanager2.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager2.CloudResourcesSpec{
					Topics: []servicemanager2.TopicConfig{{
						CloudResource: servicemanager2.CloudResource{
							Name:               "topic1",
							TeardownProtection: false,
						},
					}},
				},
			},
			"ephemeral-gcs": {
				Name: "dataflow-ephemeral-gcs",
				Lifecycle: &servicemanager2.LifecyclePolicy{
					Strategy: servicemanager2.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager2.CloudResourcesSpec{
					GCSBuckets: []servicemanager2.GCSBucket{{
						CloudResource: servicemanager2.CloudResource{
							Name:               "bucket2",
							TeardownProtection: false,
						}},
					},
				},
			},
			// ADDED a dataflow with BQ resources to specifically test BQ failure
			"ephemeral-bq": {
				Name: "dataflow-ephemeral-bq",
				Lifecycle: &servicemanager2.LifecyclePolicy{
					Strategy: servicemanager2.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager2.CloudResourcesSpec{
					BigQueryDatasets: []servicemanager2.BigQueryDataset{{CloudResource: servicemanager2.CloudResource{Name: "dataset-to-fail"}}},
					BigQueryTables:   []servicemanager2.BigQueryTable{{CloudResource: servicemanager2.CloudResource{Name: "table-to-fail"}, Dataset: "dataset-to-fail"}},
				},
			},
			"permanent": {
				Name: "dataflow-permanent",
				Lifecycle: &servicemanager2.LifecyclePolicy{
					Strategy: servicemanager2.LifecycleStrategyPermanent,
				},
				Resources: servicemanager2.CloudResourcesSpec{
					BigQueryDatasets: []servicemanager2.BigQueryDataset{{CloudResource: servicemanager2.CloudResource{
						Name:               "dataset-permanent",
						TeardownProtection: true,
					}}},
				},
			},
		},
	}
}

// --- Test Cases ---

func TestServiceManager_TeardownAll_Failure(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	// Use the new config with a dataflow that has BigQuery resources
	testCfg := getTestConfigWithBQ()
	projectID := "test-project"

	t.Run("TeardownAll returns aggregated error on BigQuery failure", func(t *testing.T) {
		// Arrange
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager2.NewInMemoryServicesDefinition(testCfg)
		require.NoError(t, err)

		// The teardown loop runs in reverse. We expect it to process:
		// 1. dataflow-ephemeral-bq (FAIL BQ)
		// 2. dataflow-ephemeral-gcs (SUCCEED GCS)
		// 3. dataflow-ephemeral-topics (SUCCEED TOPICS)
		// Permanent dataflows are skipped.

		// --- MOCK SETUP FOR dataflow-ephemeral-bq (This will FAIL) ---
		mockBqDatasetFail := new(MockBQDataset)
		mockBqTableFail := new(MockBQTable)
		// This is the root cause of the failure.
		mockBqTableFail.On("Delete", ctx).Return(errors.New("mock bq table deletion error")).Once()
		// Dataset deletion still proceeds and succeeds after table deletion fails.
		mockBqDatasetFail.On("Delete", ctx).Return(nil).Once()
		mockBqDatasetFail.On("Table", "table-to-fail").Return(mockBqTableFail).Once()
		// CRITICAL FIX: The BQClient.Dataset method is called TWICE for this dataflow:
		// Once in `teardownTables` and once in `teardownDatasets`.
		mockBqClient.On("Dataset", "dataset-to-fail").Return(mockBqDatasetFail).Twice()

		// --- MOCK SETUP FOR dataflow-ephemeral-gcs (This will SUCCEED) ---
		mockBucket2 := new(MockBucketHandle)
		mockStoreClient.On("Bucket", "bucket2").Return(mockBucket2).Once()
		mockBucket2.On("Attrs", ctx).Return(&servicemanager2.BucketAttributes{}, nil).Once()
		mockBucket2.On("Delete", ctx).Return(nil).Once()

		// --- MOCK SETUP FOR dataflow-ephemeral-topics (This will SUCCEED) ---
		mockTopic1 := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic1).Once()
		mockTopic1.On("Delete", ctx).Return(nil).Once()

		// We expect Project() to be called by the BQ Manager for EACH of the 3 ephemeral dataflows.
		mockBqClient.On("Project").Return(projectID).Times(3)

		architecture, err := servicesDef.GetMicroserviceArchitecture()
		require.NoError(t, err)

		// Act
		sm, err := servicemanager2.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, architecture, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownAll(ctx)

		// Assert
		require.Error(t, err, "Expected an error to be returned from TeardownAll")
		// Check that the error message contains details from the failed dataflow.
		assert.Contains(t, err.Error(), "failed to teardown dataflow 'dataflow-ephemeral-bq'")
		// Check that it identifies the correct sub-manager.
		assert.Contains(t, err.Error(), "BigQuery teardown failed")
		// Check that it contains the root cause.
		assert.Contains(t, err.Error(), "mock bq table deletion error")
		// Importantly, ensure it does NOT contain the names of the successful dataflows.
		assert.NotContains(t, err.Error(), "dataflow-ephemeral-gcs")
		assert.NotContains(t, err.Error(), "dataflow-ephemeral-topics")

		// Verify that all expected mock calls were made across the entire loop.
		mockMsgClient.AssertExpectations(t)
		mockStoreClient.AssertExpectations(t)
		mockBqClient.AssertExpectations(t)
		mockBqDatasetFail.AssertExpectations(t)
		mockBqTableFail.AssertExpectations(t)
		mockBucket2.AssertExpectations(t)
		mockTopic1.AssertExpectations(t)
	})
}
