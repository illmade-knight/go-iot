package servicemanager_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	telemetry "github.com/illmade-knight/go-iot/gen/go/protos/telemetry"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Implementations for BQClient, BQDataset, BQTable interfaces ---
// These mocks are part of the servicemanager_test package and can be used by other tests in this package.

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

// Ensure mocks implement the interfaces from the servicemanager package
var _ servicemanager.BQTable = &MockBQTable{}
var _ servicemanager.BQDataset = &MockBQDataset{}
var _ servicemanager.BQClient = &MockBQClient{}

// Helper to simulate "notFound" error
type notFoundError struct{ msg string }

func (e *notFoundError) Error() string { return e.msg }
func newNotFoundError(resourceType, resourceID string) error {
	return &notFoundError{msg: fmt.Sprintf("%s %s notFound", resourceType, resourceID)}
}

func TestBigQueryManager_Setup(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()
	testProjectID := "test-project-bq-manager"

	_, errInfer := bigquery.InferSchema((*telemetry.MeterReading)(nil))
	require.NoError(t, errInfer)

	testConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: testProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: testProjectID, TeardownProtection: false},
		},
		Resources: servicemanager.ResourcesSpec{
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
		},
	}

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

		err = manager.Setup(ctx, testConfig, "test")
		require.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})
}
