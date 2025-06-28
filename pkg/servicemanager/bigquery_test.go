package servicemanager_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	telemetry "github.com/illmade-knight/go-iot/gen/go/protos/telemetry"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Implementations for BQClient, BQDataset, BQTable interfaces ---

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
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(servicemanager.BQTable)
}

type MockBQClient struct {
	mock.Mock
}

func (m *MockBQClient) Dataset(datasetID string) servicemanager.BQDataset {
	args := m.Called(datasetID)
	if args.Get(0) == nil {
		return nil
	}
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

var _ servicemanager.BQTable = &MockBQTable{}
var _ servicemanager.BQDataset = &MockBQDataset{}
var _ servicemanager.BQClient = &MockBQClient{}

// --- Test Helpers ---

type notFoundError struct{ msg string }

func (e *notFoundError) Error() string { return e.msg }
func newNotFoundError(resourceType, resourceID string) error {
	return &notFoundError{msg: fmt.Sprintf("%s %s notFound", resourceType, resourceID)}
}

func getMeterReadingSchema() bigquery.Schema {
	schema, err := bigquery.InferSchema(&telemetry.MeterReadingBQWrapper{})
	if err != nil {
		panic(fmt.Sprintf("Failed to infer MeterReadingBQWrapper schema: %v", err))
	}
	return schema
}

func getTestBigQueryResources() servicemanager.ResourcesSpec {
	return servicemanager.ResourcesSpec{
		BigQueryDatasets: []servicemanager.BigQueryDataset{
			{Name: "dataset1", Description: "Test Dataset 1", Labels: map[string]string{"env": "test"}},
		},
		BigQueryTables: []servicemanager.BigQueryTable{
			{
				Name:                   "table1",
				Dataset:                "dataset1",
				Description:            "Test Table 1",
				SchemaSourceType:       "go_struct",
				SchemaSourceIdentifier: "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading",
				TimePartitioningField:  "original_mqtt_time",
				TimePartitioningType:   "DAY",
			},
		},
	}
}

// --- Test Cases for BigQueryManager ---

func TestBigQueryManager_Setup(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()
	testProjectID := "test-project-bq-manager"
	testLocation := "us-central1"
	testResources := getTestBigQueryResources()

	t.Run("Create new dataset and table", func(t *testing.T) {
		mockClient := new(MockBQClient)
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)

		mockClient.On("Project").Return(testProjectID)
		mockClient.On("Dataset", "dataset1").Return(mockDataset)
		mockDataset.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "dataset1"))
		mockDataset.On("Create", ctx, mock.Anything).Return(nil)
		mockDataset.On("Table", "table1").Return(mockTable)
		mockTable.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "table1"))
		mockTable.On("Create", ctx, mock.Anything).Return(nil)

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Setup(ctx, testProjectID, testLocation, testResources)
		require.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})
}

func TestBigQueryManager_Teardown(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()
	testProjectID := "test-project-bq-manager"
	testResources := getTestBigQueryResources()

	t.Run("Success", func(t *testing.T) {
		mockClient := new(MockBQClient)
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)

		mockClient.On("Project").Return(testProjectID)
		mockClient.On("Dataset", "dataset1").Return(mockDataset)
		mockDataset.On("Table", "table1").Return(mockTable)
		mockTable.On("Delete", ctx).Return(nil)
		mockDataset.On("Delete", ctx).Return(nil)

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Teardown(ctx, testProjectID, testResources, false)
		require.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})

	t.Run("Protection Enabled", func(t *testing.T) {
		mockClient := new(MockBQClient)
		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		mockClient.On("Project").Return(testProjectID)

		err = manager.Teardown(ctx, testProjectID, testResources, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "teardown protection enabled")

		mockClient.AssertCalled(t, "Project")
		mockClient.AssertNotCalled(t, "Dataset", mock.Anything)
	})

	t.Run("Partial Failure returns aggregated error", func(t *testing.T) {
		// Arrange: Define resources with multiple tables to test the loop
		multiTableResources := servicemanager.ResourcesSpec{
			BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: "dataset1"}},
			BigQueryTables: []servicemanager.BigQueryTable{
				{Name: "table1", Dataset: "dataset1"},
				{Name: "table2", Dataset: "dataset1"},
			},
		}

		mockClient := new(MockBQClient)
		mockDataset := new(MockBQDataset)
		mockTable1 := new(MockBQTable)
		mockTable2 := new(MockBQTable)

		mockClient.On("Project").Return(testProjectID)
		mockClient.On("Dataset", "dataset1").Return(mockDataset)

		// Link tables to the dataset mock
		mockDataset.On("Table", "table1").Return(mockTable1)
		mockDataset.On("Table", "table2").Return(mockTable2)

		// Mock one table deletion to FAIL, the other to SUCCEED
		mockTable1.On("Delete", ctx).Return(errors.New("mock permission error on table1"))
		mockTable2.On("Delete", ctx).Return(nil)

		// Mock dataset deletion to succeed
		mockDataset.On("Delete", ctx).Return(nil)

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		// Act
		err = manager.Teardown(ctx, testProjectID, multiTableResources, false)

		// Assert
		require.Error(t, err, "Teardown should return an error if any sub-operation fails")
		assert.Contains(t, err.Error(), "mock permission error on table1", "The aggregated error should contain the specific failure message")

		// Verify that all teardown operations were still attempted
		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable1.AssertExpectations(t)
		mockTable2.AssertExpectations(t)
	})
}

// --- Test Cases for Verify (No change needed as signatures were already correct) ---

func TestBigQueryManager_VerifyDatasets(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()

	t.Run("All Datasets Exist and Match Config", func(t *testing.T) {
		mockClient := new(MockBQClient)
		mockDataset1 := new(MockBQDataset)
		datasetsToVerify := []servicemanager.BigQueryDataset{
			{Name: "existing-ds1", Description: "Desc1", Location: "us-central1", Labels: map[string]string{"env": "test"}},
		}

		mockClient.On("Dataset", "existing-ds1").Return(mockDataset1).Once()
		mockDataset1.On("Metadata", ctx).Return(&bigquery.DatasetMetadata{
			Description: "Desc1", Location: "US-CENTRAL1", Labels: map[string]string{"env": "test", "owner": "team-a"},
		}, nil).Once()

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.VerifyDatasets(ctx, datasetsToVerify)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockDataset1.AssertExpectations(t)
	})

	t.Run("Dataset Missing", func(t *testing.T) {
		mockClient := new(MockBQClient)
		mockDataset := new(MockBQDataset)
		datasetsToVerify := []servicemanager.BigQueryDataset{{Name: "missing-ds"}}

		mockClient.On("Dataset", "missing-ds").Return(mockDataset).Once()
		mockDataset.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "missing-ds")).Once()

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.VerifyDatasets(ctx, datasetsToVerify)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dataset 'missing-ds' not found during verification")
		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
	})
}

func TestBigQueryManager_VerifyTables(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()
	schemaRegistry := map[string]interface{}{
		"github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading": &telemetry.MeterReadingBQWrapper{},
	}
	expectedMeterReadingSchema := getMeterReadingSchema()

	t.Run("All Tables Exist and Match Config", func(t *testing.T) {
		mockClient := new(MockBQClient)
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)
		tablesToVerify := []servicemanager.BigQueryTable{
			{
				Name:                   "event_log",
				Dataset:                "my_dataset",
				Description:            "Event log table",
				SchemaSourceType:       "go_struct",
				SchemaSourceIdentifier: "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading",
				TimePartitioningField:  "timestamp",
				TimePartitioningType:   "DAY",
				ClusteringFields:       []string{"meter_id", "event_type"},
			},
		}

		mockClient.On("Dataset", "my_dataset").Return(mockDataset).Once()
		mockDataset.On("Table", "event_log").Return(mockTable).Once()
		mockTable.On("Metadata", ctx).Return(&bigquery.TableMetadata{
			Description:      "Event log table",
			Schema:           expectedMeterReadingSchema,
			TimePartitioning: &bigquery.TimePartitioning{Field: "timestamp", Type: bigquery.DayPartitioningType},
			Clustering:       &bigquery.Clustering{Fields: []string{"meter_id", "event_type"}},
		}, nil).Once()

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, schemaRegistry)
		require.NoError(t, err)

		err = manager.VerifyTables(ctx, tablesToVerify)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})

	t.Run("Table Missing", func(t *testing.T) {
		mockClient := new(MockBQClient)
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)
		tablesToVerify := []servicemanager.BigQueryTable{{Name: "missing_table", Dataset: "some_dataset"}}

		mockClient.On("Dataset", "some_dataset").Return(mockDataset).Once()
		mockDataset.On("Table", "missing_table").Return(mockTable).Once()
		mockTable.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "missing_table")).Once()

		manager, err := servicemanager.NewBigQueryManager(mockClient, logger, schemaRegistry)
		require.NoError(t, err)

		err = manager.VerifyTables(ctx, tablesToVerify)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "table 'some_dataset.missing_table' not found during verification")
		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})
}
