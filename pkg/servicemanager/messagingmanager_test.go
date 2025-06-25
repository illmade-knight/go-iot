package servicemanager_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mocks for Messaging Interfaces ---

type MockMessagingTopic struct {
	mock.Mock
}

func (m *MockMessagingTopic) ID() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockMessagingTopic) Exists(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}
func (m *MockMessagingTopic) Update(ctx context.Context, cfg servicemanager.TopicConfig) (*servicemanager.TopicConfig, error) {
	args := m.Called(ctx, cfg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.TopicConfig), args.Error(1)
}
func (m *MockMessagingTopic) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockMessagingSubscription struct {
	mock.Mock
}

func (m *MockMessagingSubscription) ID() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockMessagingSubscription) Exists(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}
func (m *MockMessagingSubscription) Update(ctx context.Context, cfg servicemanager.SubscriptionConfig) (*servicemanager.SubscriptionConfig, error) {
	args := m.Called(ctx, cfg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.SubscriptionConfig), args.Error(1)
}
func (m *MockMessagingSubscription) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockMessagingClient struct {
	mock.Mock
}

func (m *MockMessagingClient) Topic(id string) servicemanager.MessagingTopic {
	args := m.Called(id)
	return args.Get(0).(servicemanager.MessagingTopic)
}
func (m *MockMessagingClient) Subscription(id string) servicemanager.MessagingSubscription {
	args := m.Called(id)
	return args.Get(0).(servicemanager.MessagingSubscription)
}
func (m *MockMessagingClient) CreateTopic(ctx context.Context, topicID string) (servicemanager.MessagingTopic, error) {
	args := m.Called(ctx, topicID)
	return args.Get(0).(servicemanager.MessagingTopic), args.Error(1)
}
func (m *MockMessagingClient) CreateTopicWithConfig(ctx context.Context, topicSpec servicemanager.TopicConfig) (servicemanager.MessagingTopic, error) {
	args := m.Called(ctx, topicSpec)
	return args.Get(0).(servicemanager.MessagingTopic), args.Error(1)
}
func (m *MockMessagingClient) CreateSubscription(ctx context.Context, subSpec servicemanager.SubscriptionConfig) (servicemanager.MessagingSubscription, error) {
	args := m.Called(ctx, subSpec)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(servicemanager.MessagingSubscription), args.Error(1)
}
func (m *MockMessagingClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockMessagingClient) Validate(resources servicemanager.ResourcesSpec) error {
	args := m.Called(resources)
	return args.Error(0)
}

// --- Helper Function ---

func getTestMessagingResources() servicemanager.ResourcesSpec {
	return servicemanager.ResourcesSpec{
		Topics: []servicemanager.TopicConfig{
			{Name: "test-topic-1"},
			{Name: "test-topic-2", Labels: map[string]string{"env": "test"}},
		},
		Subscriptions: []servicemanager.SubscriptionConfig{
			{Name: "test-sub-1", Topic: "test-topic-1", AckDeadlineSeconds: 30, MessageRetention: servicemanager.Duration(10 * time.Minute)},
		},
	}
}

// --- Test Cases for Setup ---

func TestMessagingManager_Setup_CreateNewResources(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	projectID := "test-project"
	ctx := context.Background()

	mockClient.On("Validate", resources).Return(nil)

	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateTopic", ctx, "test-topic-1").Return(mockTopic1, nil)
	mockTopic1.On("ID").Return("test-topic-1")

	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateTopicWithConfig", ctx, resources.Topics[1]).Return(mockTopic2, nil)
	mockTopic2.On("ID").Return("test-topic-2")

	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateSubscription", ctx, resources.Subscriptions[0]).Return(mockSub1, nil)
	mockSub1.On("ID").Return("test-sub-1")

	// Act - Call Setup with the new signature
	err = manager.Setup(ctx, projectID, resources)

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestMessagingManager_Setup_ValidationFails(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	projectID := "test-project"
	ctx := context.Background()

	expectedErr := errors.New("invalid subscription config")
	mockClient.On("Validate", resources).Return(expectedErr)

	// Act - Call Setup with the new signature
	err = manager.Setup(ctx, projectID, resources)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockClient.AssertExpectations(t)
	// Ensure no other calls were made
	mockClient.AssertNotCalled(t, "Topic", mock.Anything)
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)
}

// --- Test Cases for Verify (No change needed as signature was already correct) ---

func TestMessagingManager_VerifyTopics(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	t.Run("All Topics Exist", func(t *testing.T) {
		topicsToVerify := []servicemanager.TopicConfig{
			{Name: "existing-topic-1"},
			{Name: "existing-topic-2"},
		}

		mockTopic1 := new(MockMessagingTopic)
		mockClient.On("Topic", "existing-topic-1").Return(mockTopic1).Once()
		mockTopic1.On("Exists", ctx).Return(true, nil).Once()

		mockTopic2 := new(MockMessagingTopic)
		mockClient.On("Topic", "existing-topic-2").Return(mockTopic2).Once()
		mockTopic2.On("Exists", ctx).Return(true, nil).Once()

		err := manager.VerifyTopics(ctx, topicsToVerify)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Some Topics Missing", func(t *testing.T) {
		topicsToVerify := []servicemanager.TopicConfig{
			{Name: "existing-topic"},
			{Name: "missing-topic"},
		}

		mockTopicExisting := new(MockMessagingTopic)
		mockClient.On("Topic", "existing-topic").Return(mockTopicExisting).Once()
		mockTopicExisting.On("Exists", ctx).Return(true, nil).Once()

		mockTopicMissing := new(MockMessagingTopic)
		mockClient.On("Topic", "missing-topic").Return(mockTopicMissing).Once()
		mockTopicMissing.On("Exists", ctx).Return(false, nil).Once()

		err := manager.VerifyTopics(ctx, topicsToVerify)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topic 'missing-topic' not found during verification")
		mockClient.AssertExpectations(t)
	})
}

func TestMessagingManager_VerifySubscriptions(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	t.Run("All Subscriptions Exist", func(t *testing.T) {
		subsToVerify := []servicemanager.SubscriptionConfig{
			{Name: "existing-sub-1", Topic: "topic-a"},
			{Name: "existing-sub-2", Topic: "topic-b"},
		}

		mockSub1 := new(MockMessagingSubscription)
		mockClient.On("Subscription", "existing-sub-1").Return(mockSub1).Once()
		mockSub1.On("Exists", ctx).Return(true, nil).Once()

		mockSub2 := new(MockMessagingSubscription)
		mockClient.On("Subscription", "existing-sub-2").Return(mockSub2).Once()
		mockSub2.On("Exists", ctx).Return(true, nil).Once()

		err := manager.VerifySubscriptions(ctx, subsToVerify)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Some Subscriptions Missing", func(t *testing.T) {
		subsToVerify := []servicemanager.SubscriptionConfig{
			{Name: "existing-sub", Topic: "topic-x"},
			{Name: "missing-sub", Topic: "topic-y"},
		}

		mockSubExisting := new(MockMessagingSubscription)
		mockClient.On("Subscription", "existing-sub").Return(mockSubExisting).Once()
		mockSubExisting.On("Exists", ctx).Return(true, nil).Once()

		mockSubMissing := new(MockMessagingSubscription)
		mockClient.On("Subscription", "missing-sub").Return(mockSubMissing).Once()
		mockSubMissing.On("Exists", ctx).Return(false, nil).Once()

		err := manager.VerifySubscriptions(ctx, subsToVerify)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "subscription 'missing-sub' not found during verification")
		mockClient.AssertExpectations(t)
	})
}

// --- Test Cases for Teardown ---

func TestMessagingManager_Teardown_Success(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	projectID := "test-project"
	ctx := context.Background()

	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Delete", ctx).Return(nil)

	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Delete", ctx).Return(nil)

	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Delete", ctx).Return(nil)

	// Act - Call Teardown with the new signature
	err = manager.Teardown(ctx, projectID, resources, false) // teardownProtection is false

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestMessagingManager_Teardown_ProtectionEnabled(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	projectID := "prod-project"
	ctx := context.Background()

	// Act - Call Teardown with the new signature and teardownProtection set to true
	err = manager.Teardown(ctx, projectID, resources, true)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, "teardown protection enabled for this operation")
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)
	mockClient.AssertNotCalled(t, "Topic", mock.Anything)
}
