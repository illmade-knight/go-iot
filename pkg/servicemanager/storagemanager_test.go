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

// --- Test Helper ---
func getTestStorageResources() servicemanager.ResourcesSpec {
	return servicemanager.ResourcesSpec{
		GCSBuckets: []servicemanager.GCSBucket{{Name: "test-bucket"}},
	}
}

// --- Test Cases for StorageManager ---

func TestStorageManager_Setup_CreateNewBucket(t *testing.T) {
	// Arrange
	mockClient := new(MockStorageClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewStorageManager(mockClient, logger)
	require.NoError(t, err)

	ctx := context.Background()
	projectID := "test-proj"
	location := "us-central1"
	labels := map[string]string{"env": "test"}
	resources := getTestStorageResources()

	mockBucketHandle := new(MockBucketHandle)
	mockClient.On("Bucket", "test-bucket").Return(mockBucketHandle)
	mockBucketHandle.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist"))
	mockBucketHandle.On("Create", ctx, projectID, mock.AnythingOfType("*servicemanager.BucketAttributes")).Return(nil)

	// Act
	err = manager.Setup(ctx, projectID, location, labels, resources)

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
	resources := servicemanager.ResourcesSpec{
		GCSBuckets: []servicemanager.GCSBucket{{Name: "bucket-to-delete"}},
	}

	mockBucketHandle := new(MockBucketHandle)
	mockClient.On("Bucket", "bucket-to-delete").Return(mockBucketHandle)
	mockBucketHandle.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{}, nil) // Bucket exists
	mockBucketHandle.On("Delete", ctx).Return(nil)

	// Act
	err = manager.Teardown(ctx, resources, false) // teardownProtection is false

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockBucketHandle.AssertExpectations(t)
}

func TestStorageManager_Teardown_ProtectionEnabled(t *testing.T) {
	// Arrange
	mockClient := new(MockStorageClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewStorageManager(mockClient, logger)
	require.NoError(t, err)

	ctx := context.Background()
	resources := getTestStorageResources()

	// Act
	err = manager.Teardown(ctx, resources, true) // teardownProtection is true

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "teardown protection enabled")
	mockClient.AssertNotCalled(t, "Bucket", mock.Anything)
}

// --- Test Cases for VerifyBuckets (No change needed as signature was already correct) ---

func TestStorageManager_VerifyBuckets(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	mockClient := new(MockStorageClient)
	manager, err := servicemanager.NewStorageManager(mockClient, logger)
	require.NoError(t, err)

	t.Run("All Buckets Exist and Match Config", func(t *testing.T) {
		bucketsToVerify := []servicemanager.GCSBucket{
			{Name: "bucket-exists", Location: "us-central1", StorageClass: "STANDARD", VersioningEnabled: true, Labels: map[string]string{"env": "dev"}},
		}

		mockBucket1 := new(MockBucketHandle)
		mockClient.On("Bucket", "bucket-exists").Return(mockBucket1).Once()
		mockBucket1.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{
			Name: "bucket-exists", Location: "US-CENTRAL1", StorageClass: "STANDARD", VersioningEnabled: true, Labels: map[string]string{"env": "dev", "project": "xyz"},
		}, nil).Once()

		err := manager.VerifyBuckets(ctx, bucketsToVerify)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockBucket1.AssertExpectations(t)
	})

	t.Run("Bucket Missing", func(t *testing.T) {
		bucketsToVerify := []servicemanager.GCSBucket{
			{Name: "missing-bucket"},
		}

		mockBucket := new(MockBucketHandle)
		mockClient.On("Bucket", "missing-bucket").Return(mockBucket).Once()
		mockBucket.On("Attrs", ctx).Return(nil, errors.New("storage: bucket doesn't exist")).Once()

		err := manager.VerifyBuckets(ctx, bucketsToVerify)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bucket 'missing-bucket' not found during verification")
		mockClient.AssertExpectations(t)
		mockBucket.AssertExpectations(t)
	})

	t.Run("Location Mismatch", func(t *testing.T) {
		bucketsToVerify := []servicemanager.GCSBucket{
			{Name: "location-mismatch-bucket", Location: "us-east1"},
		}

		mockBucket := new(MockBucketHandle)
		mockClient.On("Bucket", "location-mismatch-bucket").Return(mockBucket).Once()
		mockBucket.On("Attrs", ctx).Return(&servicemanager.BucketAttributes{
			Name: "location-mismatch-bucket", Location: "US-WEST1",
		}, nil).Once()

		err := manager.VerifyBuckets(ctx, bucketsToVerify)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "location mismatch: expected 'US-EAST1' (configured) vs 'US-WEST1' (actual)")
		mockClient.AssertExpectations(t)
		mockBucket.AssertExpectations(t)
	})
}
