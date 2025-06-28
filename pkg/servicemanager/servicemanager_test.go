package servicemanager_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Helper ---

// getTestConfigWithBQ provides a more complex configuration for testing teardown scenarios,
// including dataflows with different resource types to ensure managers are called correctly.
func getTestConfigWithBQ() *servicemanager.TopLevelConfig {
	return &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-project",
		DefaultLocation:  "us-central1",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {
				ProjectID:          "test-project",
				TeardownProtection: false,
			},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name: "dataflow-ephemeral-topics",
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{{Name: "topic1"}},
				},
			},
			{
				Name: "dataflow-ephemeral-gcs",
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager.ResourcesSpec{
					GCSBuckets: []servicemanager.GCSBucket{{Name: "bucket2"}},
				},
			},
			// ADDED a dataflow with BQ resources to specifically test BQ failure
			{
				Name: "dataflow-ephemeral-bq",
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyEphemeral,
				},
				Resources: servicemanager.ResourcesSpec{
					BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: "dataset-to-fail"}},
					BigQueryTables:   []servicemanager.BigQueryTable{{Name: "table-to-fail", Dataset: "dataset-to-fail"}},
				},
			},
			{
				Name: "dataflow-permanent",
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy: servicemanager.LifecycleStrategyPermanent,
				},
				Resources: servicemanager.ResourcesSpec{
					BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: "dataset-permanent"}},
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
	env := "test"
	projectID := "test-project"

	t.Run("TeardownAll returns aggregated error on BigQuery failure", func(t *testing.T) {
		// Arrange
		mockMsgClient := new(MockMessagingClient)
		mockStoreClient := new(MockStorageClient)
		mockBqClient := new(MockBQClient)
		servicesDef, err := servicemanager.NewInMemoryServicesDefinition(testCfg)
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
		mockBucket2.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil).Once()
		mockBucket2.On("Delete", ctx).Return(nil).Once()

		// --- MOCK SETUP FOR dataflow-ephemeral-topics (This will SUCCEED) ---
		mockTopic1 := new(MockMessagingTopic)
		mockMsgClient.On("Topic", "topic1").Return(mockTopic1).Once()
		mockTopic1.On("Delete", ctx).Return(nil).Once()

		// We expect Project() to be called by the BQ Manager for EACH of the 3 ephemeral dataflows.
		mockBqClient.On("Project").Return(projectID).Times(3)

		// Act
		sm, err := servicemanager.NewServiceManagerFromClients(mockMsgClient, mockStoreClient, mockBqClient, servicesDef, nil, logger)
		require.NoError(t, err)
		err = sm.TeardownAll(ctx, env)

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
