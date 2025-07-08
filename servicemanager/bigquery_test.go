package servicemanager_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-iot/servicemanager"
	"io"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	telemetry "github.com/illmade-knight/go-iot/gen/go/protos/telemetry" // Assuming this path is correct for your schema
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Implementations for BQClient, BQDataset, BQTable interfaces ---
// (These mocks are assumed to be defined elsewhere in the servicemanager_test package
// and are imported/accessible here. Their definitions are omitted for brevity.)

type MockBQTable struct {
	mock.Mock
}

func (m *MockBQTable) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bigquery.TableMetadata), args.Error(1)
}

func (m *MockBQTable) Create(ctx context.Context, meta *bigquery.TableMetadata) error {
	args := m.Called(ctx, meta)
	return args.Error(0)
}

func (m *MockBQTable) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockBQDataset struct {
	mock.Mock
}

func (m *MockBQDataset) Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bigquery.DatasetMetadata), args.Error(1)
}

func (m *MockBQDataset) Create(ctx context.Context, meta *bigquery.DatasetMetadata) error {
	args := m.Called(ctx, meta)
	return args.Error(0)
}

func (m *MockBQDataset) Update(ctx context.Context, metaToUpdate bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error) {
	args := m.Called(ctx, metaToUpdate, etag)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bigquery.DatasetMetadata), args.Error(1)
}

func (m *MockBQDataset) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBQDataset) Table(tableID string) servicemanager.BQTable {
	args := m.Called(tableID)
	return args.Get(0).(servicemanager.BQTable)
}

func (m *MockBQDataset) DeleteWithContents(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockBQClient struct {
	mock.Mock
}

func (m *MockBQClient) Dataset(datasetID string) servicemanager.BQDataset {
	args := m.Called(datasetID)
	return args.Get(0).(servicemanager.BQDataset)
}

func (m *MockBQClient) Project() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockBQClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- Test Helper Functions ---

// newNotFoundError creates an error that simulates a "not found" error from the BigQuery client.
func newNotFoundError(resourceType, resourceName string) error {
	return errors.New(fmt.Sprintf("%s not found: %s", resourceType, resourceName))
}

func getTestBigQueryResources() servicemanager.CloudResourcesSpec {
	return servicemanager.CloudResourcesSpec{
		BigQueryDatasets: []servicemanager.BigQueryDataset{
			{
				CloudResource: servicemanager.CloudResource{Name: "test-dataset-1", TeardownProtection: false},
			},
			{
				CloudResource: servicemanager.CloudResource{Name: "test-dataset-2", TeardownProtection: true}, // Protected dataset
			},
		},
		BigQueryTables: []servicemanager.BigQueryTable{
			{
				CloudResource:          servicemanager.CloudResource{Name: "test-table-1", TeardownProtection: false},
				Dataset:                "test-dataset-1",
				SchemaSourceIdentifier: "meter_reading_schema",
				TimePartitioningField:  "timestamp",
				TimePartitioningType:   "DAY",
				ClusteringFields:       []string{"meter_id"},
				Expiration:             servicemanager.Duration(24 * time.Hour),
			},
			{
				CloudResource:          servicemanager.CloudResource{Name: "test-table-2", TeardownProtection: true}, // Protected table
				Dataset:                "test-dataset-1",
				SchemaSourceIdentifier: "meter_reading_schema",
				TimePartitioningField:  "timestamp",
				TimePartitioningType:   "DAY",
				ClusteringFields:       []string{"meter_id"},
				Expiration:             servicemanager.Duration(24 * time.Hour),
			},
		},
	}
}

// setupBigQueryManagerTest creates a BigQueryManager with a mock client for testing.
func setupBigQueryManagerTest(t *testing.T) (*servicemanager.BigQueryManager, *MockBQClient, context.Context, servicemanager.Environment, map[string]interface{}, zerolog.Logger) {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	ctx := context.Background()
	env := servicemanager.Environment{ProjectID: "test-project", Name: "test-env", Location: "us-central1"}
	schemaRegistry := map[string]interface{}{
		"meter_reading_schema": telemetry.MeterReading{}, // Assuming this struct exists and can be inferred
	}

	mockClient := new(MockBQClient)
	manager, err := servicemanager.NewBigQueryManager(mockClient, logger, schemaRegistry, env) // Pass environment
	require.NoError(t, err)
	return manager, mockClient, ctx, env, schemaRegistry, logger
}

// --- Test Cases ---

func TestNewBigQueryManager(t *testing.T) {
	logger := zerolog.New(io.Discard)
	env := servicemanager.Environment{ProjectID: "test-project"}
	schemaRegistry := make(map[string]interface{})

	t.Run("Success", func(t *testing.T) {
		mockClient := new(MockBQClient)
		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, schemaRegistry, env)
		require.NoError(t, err)
		assert.NotNil(t, manager)
	})

	t.Run("Nil Client", func(t *testing.T) {
		manager, err := servicemanager.NewBigQueryManager(nil, logger, schemaRegistry, env)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
		assert.Nil(t, manager)
	})

	t.Run("Nil Schema Registry", func(t *testing.T) {
		mockClient := new(MockBQClient)
		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil, env)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema registry cannot be nil")
		assert.Nil(t, manager)
	})
}

func TestBigQueryManager_CreateResources_Success(t *testing.T) {
	manager, mockClient, ctx, env, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Mocks for Dataset 1 (not protected)
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "test-dataset-1")).Once()  // Dataset doesn't exist
	mockDataset1.On("Create", ctx, mock.AnythingOfType("*bigquery.DatasetMetadata")).Return(nil).Once() // Corrected type

	// Mocks for Dataset 2 (protected)
	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Twice()
	mockDataset2.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "test-dataset-2")).Twice() // Dataset doesn't exist
	mockDataset2.On("Create", ctx, mock.AnythingOfType("*bigquery.DatasetMetadata")).Return(nil).Once() // Corrected type

	// Mocks for Table 1 (not protected)
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-1")).Once() // Table doesn't exist
	mockTable1.On("Create", ctx, mock.AnythingOfType("*bigquery.TableMetadata")).Return(nil).Once()

	// Mocks for Table 2 (protected)
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-2")).Once() // Table doesn't exist
	mockTable2.On("Create", ctx, mock.AnythingOfType("*bigquery.TableMetadata")).Return(nil).Once()

	mockClient.On("Project").Return(env.ProjectID).Maybe() // Called during table creation for ProvisionedBigQueryTable

	// Act
	provTables, provDatasets, err := manager.CreateResources(ctx, resources) // Renamed function

	// Assert
	require.NoError(t, err)
	assert.Len(t, provTables, 2)
	assert.Len(t, provDatasets, 2)

	// Verify provisioned tables
	assert.Contains(t, provTables, servicemanager.ProvisionedBigQueryTable{Dataset: "test-dataset-1", Name: "test-table-1"})
	assert.Contains(t, provTables, servicemanager.ProvisionedBigQueryTable{Dataset: "test-dataset-1", Name: "test-table-2"})

	// Verify provisioned datasets
	assert.Contains(t, provDatasets, servicemanager.ProvisionedBigQueryDataset{Name: "test-dataset-1"})
	assert.Contains(t, provDatasets, servicemanager.ProvisionedBigQueryDataset{Name: "test-dataset-2"})

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}

func TestBigQueryManager_CreateResources_ExistingResources(t *testing.T) {
	manager, mockClient, ctx, env, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Dataset 1 exists
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once() // Dataset exists

	// Dataset 2 exists
	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Once()
	mockDataset2.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once() // Dataset exists

	// Table 1 exists
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(&bigquery.TableMetadata{}, nil).Once() // Table exists

	// Table 2 exists
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(&bigquery.TableMetadata{}, nil).Once() // Table exists

	mockClient.On("Project").Return(env.ProjectID).Maybe()

	// Act
	provTables, provDatasets, err := manager.CreateResources(ctx, resources)

	// Assert
	require.NoError(t, err)
	assert.Len(t, provTables, 2)
	assert.Len(t, provDatasets, 2)

	assert.Contains(t, provTables, servicemanager.ProvisionedBigQueryTable{Dataset: "test-dataset-1", Name: "test-table-1"})
	assert.Contains(t, provTables, servicemanager.ProvisionedBigQueryTable{Dataset: "test-dataset-1", Name: "test-table-2"})

	assert.Contains(t, provDatasets, servicemanager.ProvisionedBigQueryDataset{Name: "test-dataset-1"})
	assert.Contains(t, provDatasets, servicemanager.ProvisionedBigQueryDataset{Name: "test-dataset-2"})

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}

func TestBigQueryManager_CreateResources_PartialFailure(t *testing.T) {
	manager, mockClient, ctx, env, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Dataset 1 fails to create
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "test-dataset-1")).Once()
	mockDataset1.On("Create", ctx, mock.AnythingOfType("*bigquery.DatasetMetadata")).Return(errors.New("failed to create dataset")).Once() // Corrected type

	// Dataset 2 succeeds
	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Once()
	mockDataset2.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "test-dataset-2")).Once()
	mockDataset2.On("Create", ctx, mock.AnythingOfType("*bigquery.DatasetMetadata")).Return(nil).Once() // Corrected type

	// Table 1 fails to create (depends on dataset 1, but still attempted)
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-1")).Once()
	mockTable1.On("Create", ctx, mock.AnythingOfType("*bigquery.TableMetadata")).Return(errors.New("failed to create table")).Once()

	// Table 2 succeeds (depends on dataset 1, but still attempted)
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-2")).Once()
	mockTable2.On("Create", ctx, mock.AnythingOfType("*bigquery.TableMetadata")).Return(nil).Once()

	mockClient.On("Project").Return(env.ProjectID).Maybe()

	// Act
	provTables, provDatasets, err := manager.CreateResources(ctx, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create dataset")
	assert.Contains(t, err.Error(), "failed to create table")

	// Only successfully provisioned resources should be returned
	assert.Len(t, provTables, 1) // Only table 2 should be provisioned
	assert.Contains(t, provTables, servicemanager.ProvisionedBigQueryTable{Dataset: "test-dataset-1", Name: "test-table-2"})
	assert.Len(t, provDatasets, 1) // Only dataset 2 should be provisioned
	assert.Contains(t, provDatasets, servicemanager.ProvisionedBigQueryDataset{Name: "test-dataset-2"})

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}

func TestBigQueryManager_Teardown_Success(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Mocks for Table 1 (not protected)
	mockTable1 := new(MockBQTable)
	mockDataset1 := new(MockBQDataset) // Need a mock dataset for the table
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Twice()
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Delete", ctx).Return(nil).Once()

	// Table 2 is protected, so its delete should NOT be called
	// mockDataset1.AssertNotCalled(t, "Table", "test-table-2") // This assertion is better placed after the call to Teardown

	// Mocks for Dataset 1 (not protected)
	mockDataset1.On("DeleteWithContents", ctx).Return(nil).Once()

	// Dataset 2 is protected, so its delete should NOT be called
	// mockClient.AssertNotCalled(t, "Dataset", "test-dataset-2") // This assertion is better placed after the call to Teardown

	// Act
	err := manager.Teardown(ctx, resources) // Removed env parameter

	// Assert
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockDataset1.AssertNotCalled(t, "Table", "test-table-2")   // Now asserted after the call
	mockClient.AssertNotCalled(t, "Dataset", "test-dataset-2") // Now asserted after the call
}

func TestBigQueryManager_Teardown_PartialFailure(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Mocks for Table 1 (fails to delete)
	mockTable1 := new(MockBQTable)
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Twice()
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Delete", ctx).Return(errors.New("table delete error")).Once()

	// Table 2 is protected, so its delete should NOT be called
	// mockDataset1.AssertNotCalled(t, "Table", "test-table-2")

	// Mocks for Dataset 1 (succeeds)
	mockDataset1.On("DeleteWithContents", ctx).Return(nil).Once()

	// Dataset 2 is protected, so its delete should NOT be called
	// mockClient.AssertNotCalled(t, "Dataset", "test-dataset-2")

	// Act
	err := manager.Teardown(ctx, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table delete error")

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockDataset1.AssertNotCalled(t, "Table", "test-table-2")
	mockClient.AssertNotCalled(t, "Dataset", "test-dataset-2")
}

func TestBigQueryManager_Teardown_ProtectedResources(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// For non-protected resources, mock success
	mockTable1 := new(MockBQTable)
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Twice()
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Delete", ctx).Return(nil).Once()
	mockDataset1.On("DeleteWithContents", ctx).Return(nil).Once()

	err := manager.Teardown(ctx, resources)
	require.NoError(t, err)

	// Ensure delete is NOT called for protected resources
	mockClient.AssertNotCalled(t, "Dataset", "test-dataset-2") // Protected dataset
	mockDataset1.AssertNotCalled(t, "Table", "test-table-2")   // Protected table (assuming it's on dataset1)

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
}

func TestBigQueryManager_Verify_Success(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Mocks for Dataset 1
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once()

	// Mocks for Dataset 2
	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Once()
	mockDataset2.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once()

	// Mocks for Table 1
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(&bigquery.TableMetadata{}, nil).Once()

	// Mocks for Table 2
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(&bigquery.TableMetadata{}, nil).Once()

	// Act
	err := manager.Verify(ctx, resources) // Removed env parameter

	// Assert
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}

func TestBigQueryManager_Verify_DatasetMissing(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Dataset 1 is missing
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "test-dataset-1")).Once()

	// Dataset 2 exists
	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Once()
	mockDataset2.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once()

	// Mocks for Table 1 (even if dataset 1 is missing, table verification is attempted)
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-1")).Once() // Table 1 will also be missing

	// Mocks for Table 2
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(&bigquery.TableMetadata{}, nil).Once()

	// Act
	err := manager.Verify(ctx, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset 'test-dataset-1' not found")
	assert.Contains(t, err.Error(), "table 'test-table-1' in dataset 'test-dataset-1' not found")

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}

func TestBigQueryManager_Verify_TableMissing(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Datasets exist
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once()

	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Once()
	mockDataset2.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{}, nil).Once()

	// Table 1 is missing
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-1")).Once()

	// Table 2 exists
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(&bigquery.TableMetadata{}, nil).Once()

	// Act
	err := manager.Verify(ctx, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table 'test-table-1' in dataset 'test-dataset-1' not found")

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}

func TestBigQueryManager_Verify_MultipleFailures(t *testing.T) {
	manager, mockClient, ctx, _, _, _ := setupBigQueryManagerTest(t)
	resources := getTestBigQueryResources()

	// Dataset 1 fails to check existence
	mockDataset1 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-1").Return(mockDataset1).Times(3)
	mockDataset1.On("Metadata", ctx).Return(nil, errors.New("dataset check error")).Once()

	// Dataset 2 is missing
	mockDataset2 := new(MockBQDataset)
	mockClient.On("Dataset", "test-dataset-2").Return(mockDataset2).Once()
	mockDataset2.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "test-dataset-2")).Once()

	// Table 1 fails to check existence
	mockTable1 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-1").Return(mockTable1).Once()
	mockTable1.On("Metadata", ctx).Return(nil, errors.New("table check error")).Once()

	// Table 2 is missing
	mockTable2 := new(MockBQTable)
	mockDataset1.On("Table", "test-table-2").Return(mockTable2).Once()
	mockTable2.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "test-table-2")).Once()

	// Act
	err := manager.Verify(ctx, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset check error")
	assert.Contains(t, err.Error(), "dataset 'test-dataset-2' not found")
	assert.Contains(t, err.Error(), "table check error")
	assert.Contains(t, err.Error(), "table 'test-table-2' in dataset 'test-dataset-1' not found")

	mockClient.AssertExpectations(t)
	mockDataset1.AssertExpectations(t)
	mockDataset2.AssertExpectations(t)
	mockTable1.AssertExpectations(t)
	mockTable2.AssertExpectations(t)
}
