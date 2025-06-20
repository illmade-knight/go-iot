package servicemanager_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/iam"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mocks for Storage Interfaces ---

type MockStorageClient struct {
	mock.Mock
}

func (m *MockStorageClient) Bucket(name string) servicemanager.StorageBucketHandle {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(servicemanager.StorageBucketHandle)
}

func (m *MockStorageClient) Buckets(ctx context.Context, projectID string) servicemanager.BucketIterator {
	args := m.Called(ctx, projectID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(servicemanager.BucketIterator)
}

func (m *MockStorageClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockBucketHandle struct {
	mock.Mock
}

func (m *MockBucketHandle) Attrs(ctx context.Context) (*servicemanager.BucketAttributes, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.BucketAttributes), args.Error(1)
}

func (m *MockBucketHandle) Create(ctx context.Context, projectID string, attrs *servicemanager.BucketAttributes) error {
	args := m.Called(ctx, projectID, attrs)
	return args.Error(0)
}

func (m *MockBucketHandle) Update(ctx context.Context, attrs servicemanager.BucketAttributesToUpdate) (*servicemanager.BucketAttributes, error) {
	args := m.Called(ctx, attrs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.BucketAttributes), args.Error(1)
}

func (m *MockBucketHandle) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBucketHandle) IAM() *iam.Handle {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*iam.Handle)
}

// --- Test Cases for StorageManager ---

func TestStorageManager_Setup_CreateNewBucket(t *testing.T) {
	// Arrange
	mockClient := new(MockStorageClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewStorageManager(mockClient, logger)
	require.NoError(t, err)

	ctx := context.Background()
	cfg := &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-proj",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "test-proj"},
		},
		Resources: servicemanager.ResourcesSpec{
			GCSBuckets: []servicemanager.GCSBucket{{Name: "new-bucket"}},
		},
	}

	mockBucketHandle := new(MockBucketHandle)
	mockClient.On("Bucket", "new-bucket").Return(mockBucketHandle)
	mockBucketHandle.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist"))
	mockBucketHandle.On("Create", ctx, "test-proj", mock.AnythingOfType("*servicemanager.BucketAttributes")).Return(nil)

	// Act
	err = manager.Setup(ctx, cfg, "test")

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockBucketHandle.AssertExpectations(t)
}

func TestStorageManager_Teardown_Success(t *testing.T) {
	// Arrange
	mockClient := new(MockStorageClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewStorageManager(mockClient, logger)
	require.NoError(t, err)

	ctx := context.Background()
	cfg := &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-proj",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "test-proj"},
		},
		Resources: servicemanager.ResourcesSpec{
			GCSBuckets: []servicemanager.GCSBucket{{Name: "bucket-to-delete"}},
		},
	}

	mockBucketHandle := new(MockBucketHandle)
	mockClient.On("Bucket", "bucket-to-delete").Return(mockBucketHandle)
	mockBucketHandle.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil) // Bucket exists
	mockBucketHandle.On("Delete", ctx).Return(nil)

	// Act
	err = manager.Teardown(ctx, cfg, "test")

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockBucketHandle.AssertExpectations(t)
}
