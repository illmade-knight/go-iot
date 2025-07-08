package servicemanager_test

import (
	"context"
	"errors"
	servicemanager "github.com/illmade-knight/go-iot/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(servicemanager.MessagingTopic)
}
func (m *MockMessagingClient) Subscription(id string) servicemanager.MessagingSubscription {
	args := m.Called(id)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(servicemanager.MessagingSubscription)
}
func (m *MockMessagingClient) CreateTopic(ctx context.Context, topicID string) (servicemanager.MessagingTopic, error) {
	args := m.Called(ctx, topicID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(servicemanager.MessagingTopic), args.Error(1)
}
func (m *MockMessagingClient) CreateTopicWithConfig(ctx context.Context, topicSpec servicemanager.TopicConfig) (servicemanager.MessagingTopic, error) {
	args := m.Called(ctx, topicSpec)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
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
func (m *MockMessagingClient) Validate(resources servicemanager.CloudResourcesSpec) error {
	args := m.Called(resources)
	return args.Error(0)
}

// --- Test Helper ---

func getTestMessagingResources() servicemanager.CloudResourcesSpec {
	return servicemanager.CloudResourcesSpec{
		Topics: []servicemanager.TopicConfig{
			{CloudResource: servicemanager.CloudResource{Name: "test-topic-1"}},
			{CloudResource: servicemanager.CloudResource{Name: "test-topic-2"}},
		},
		Subscriptions: []servicemanager.SubscriptionConfig{
			{CloudResource: servicemanager.CloudResource{Name: "test-sub-1"}, Topic: "test-topic-1", AckDeadlineSeconds: 10},
		},
	}
}

// --- Test Cases ---

func TestMessagingManager_NewMessagingManager(t *testing.T) {
	logger := zerolog.New(io.Discard) // Discard logs in tests

	t.Run("Success", func(t *testing.T) {
		mockClient := new(MockMessagingClient)
		manager, err := servicemanager.NewMessagingManager(mockClient, logger)
		require.NoError(t, err)
		assert.NotNil(t, manager)
	})

	t.Run("Nil Client", func(t *testing.T) {
		manager, err := servicemanager.NewMessagingManager(nil, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PubSub client (MessagingClient interface) cannot be nil")
		assert.Nil(t, manager)
	})
}

func TestMessagingManager_Setup_Success_Improved(t *testing.T) {
	logger := zerolog.New(io.Discard)
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources() // Contains topic1, topic2, sub1 (on topic1)
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	// 1. Initial Validation: Manager validates the resources config
	mockClient.On("Validate", resources).Return(nil).Once()

	// --- Mocks for Topic 1 (new topic) ---
	mockTopic1 := new(MockMessagingTopic)
	// Expect the manager to get the topic interface for "test-topic-1" (for setupTopics)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	// Expect the manager to check if topic1 exists (first call from setupTopics)
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	// Expect the manager to create topic1
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic1, nil).Once()

	// --- Mocks for Topic 2 (new topic) ---
	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once() // From setupTopics
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()

	// --- Mocks for Subscription 1 (new subscription on test-topic-1) ---
	mockSub1 := new(MockMessagingSubscription)
	// Expect the manager to get the subscription interface for "test-sub-1"
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once() // From setupSubscriptions
	// Expect the manager to check if sub1 exists
	mockSub1.On("Exists", ctx).Return(false, nil).Once()

	// This is the additional call for the subscription's topic's existence.
	// The `setupSubscriptions` logic explicitly calls `topic.Exists()` as a safeguard.
	// Since the topic should have been created by `setupTopics` by this point,
	// we now mock `Exists` to return `true` for this second call.
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once() // Second call to Topic() for the safeguard
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()            // <--- CRITICAL CHANGE: This is the second Exists call, and it MUST return true

	// Expect the manager to create sub1
	mockClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub1, nil).Once()

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.NoError(t, err) // Manager should report no error
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
}

func TestMessagingManager_Setup_TopicExistsUpdate(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	mockClient.On("Validate", resources).Return(nil).Once()

	// --- Topic 1 (Exists and Updates) ---
	mockTopic1 := new(MockMessagingTopic)
	// First call to Topic() for setupTopics
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	// First call to Exists() for setupTopics (topic exists)
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	// Expect update to be called
	mockTopic1.On("Update", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(&resources.Topics[0], nil).Once()

	// --- Topic 2 (Does not exist, created) ---
	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once() // From setupTopics
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()           // Topic does not exist
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()

	// --- Subscription 1 (Does not exist, created) ---
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once() // From setupSubscriptions
	mockSub1.On("Exists", ctx).Return(false, nil).Once()                // Subscription does not exist

	// Second call to Topic() for the safeguard in setupSubscriptions
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	// Second call to Exists() for the safeguard in setupSubscriptions (topic should exist by now)
	mockTopic1.On("Exists", ctx).Return(true, nil).Once() // <--- CRITICAL CHANGE: This is the second Exists call, and it MUST return true

	// Expect CreateSubscription to be called for test-sub-1
	mockClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(mockSub1, nil).Once()

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
}

func TestMessagingManager_Teardown_Success(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	ctx := context.Background()

	// --- Subscription 1 mocks (to be deleted) ---
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once() // Subscription exists
	mockSub1.On("Delete", ctx).Return(nil).Once()
	mockSub1.On("ID").Return("test-sub-1").Maybe() // Allow ID to be called multiple times for logging/internal use

	// --- Topic 1 mocks (to be deleted) ---
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once() // Topic exists
	mockTopic1.On("Delete", ctx).Return(nil).Once()
	mockTopic1.On("ID").Return("test-topic-1").Maybe() // Allow ID to be called multiple times

	// --- Topic 2 mocks (to be deleted) ---
	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once() // Topic exists
	mockTopic2.On("Delete", ctx).Return(nil).Once()
	mockTopic2.On("ID").Return("test-topic-2").Maybe() // Allow ID to be called multiple times

	// Act
	err = manager.Teardown(ctx, servicemanager.Environment{}, resources)

	// Assert
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
}

func TestMessagingManager_Teardown_ProtectionEnabled(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	// Create resources with teardown protection enabled for topic 1 and sub 1
	resources := servicemanager.CloudResourcesSpec{
		Topics: []servicemanager.TopicConfig{
			{CloudResource: servicemanager.CloudResource{Name: "test-topic-1", TeardownProtection: true}},  // Protected
			{CloudResource: servicemanager.CloudResource{Name: "test-topic-2", TeardownProtection: false}}, // Not protected
		},
		Subscriptions: []servicemanager.SubscriptionConfig{
			{CloudResource: servicemanager.CloudResource{Name: "test-sub-1", TeardownProtection: true}, Topic: "test-topic-1", AckDeadlineSeconds: 10}, // Protected
		},
	}
	ctx := context.Background()

	// Mock for protected subscription (should not be deleted)
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	// Assert that Delete is NOT called for protected subscription
	mockSub1.AssertNotCalled(t, "Delete", ctx)
	mockSub1.On("ID").Return("test-sub-1").Maybe()

	// Mock for protected topic (should not be deleted)
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	// Assert that Delete is NOT called for protected topic
	mockTopic1.AssertNotCalled(t, "Delete", ctx)
	mockTopic1.On("ID").Return("test-topic-1").Maybe()

	// Mock for unprotected topic (should be deleted)
	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2.On("Delete", ctx).Return(nil).Once()
	mockTopic2.On("ID").Return("test-topic-2").Maybe()

	// Act
	err = manager.Teardown(ctx, servicemanager.Environment{}, resources)

	// Assert
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
}

func TestMessagingManager_Setup_ClientValidationFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	expectedErr := errors.New("validation failed: invalid resource config")
	mockClient.On("Validate", resources).Return(expectedErr).Once()

	// Assert that no other methods on client are called
	mockClient.AssertNotCalled(t, "Topic", mock.Anything)
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)
	mockClient.AssertNotCalled(t, "CreateTopicWithConfig", mock.Anything, mock.Anything)
	mockClient.AssertNotCalled(t, "CreateSubscription", mock.Anything, mock.Anything)

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
}

func TestMessagingManager_Setup_TopicCreationFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	mockClient.On("Validate", resources).Return(nil).Once()

	// Topic 1 exists but creation fails
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	expectedErr := errors.New("failed to create topic: permission denied")
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(nil, expectedErr).Once()

	// Assert that no other topics or subscriptions are attempted
	mockClient.AssertNotCalled(t, "Topic", "test-topic-2")
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
}

func TestMessagingManager_Setup_SubscriptionCreationFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	mockClient.On("Validate", resources).Return(nil).Once()

	// Topic 1 and 2 succeed
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic1, nil).Once()

	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()

	// Subscription 1 exists but creation fails
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(false, nil).Once()

	// This is the additional call for the subscription's topic's existence.
	// It should return true as the topic would have been created.
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once() // Topic should exist here

	expectedErr := errors.New("failed to create subscription: invalid ack deadline")
	mockClient.On("CreateSubscription", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(nil, expectedErr).Once()

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
}

func TestMessagingManager_Teardown_TopicDeletionFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	ctx := context.Background()

	// Subscription 1 succeeds deletion
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub1.On("Delete", ctx).Return(nil).Once()
	mockSub1.On("ID").Return("test-sub-1").Maybe()

	// Topic 1 fails deletion
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	expectedErr := errors.New("failed to delete topic: resource in use")
	mockTopic1.On("Delete", ctx).Return(expectedErr).Once()
	mockTopic1.On("ID").Return("test-topic-1").Maybe()

	// Topic 2 succeeds deletion
	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2.On("Delete", ctx).Return(nil).Once()
	mockTopic2.On("ID").Return("test-topic-2").Maybe()

	// Act
	err = manager.Teardown(ctx, servicemanager.Environment{}, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
}

func TestMessagingManager_Teardown_TopicStillHasSubscriptions(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	ctx := context.Background()

	// Subscription 1 succeeds deletion
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	mockSub1.On("Delete", ctx).Return(nil).Once()
	mockSub1.On("ID").Return("test-sub-1").Maybe()

	// Topic 1 fails deletion because it still has subscriptions
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	expectedErr := errors.New("topic cannot be deleted: still has subscriptions")
	mockTopic1.On("Delete", ctx).Return(expectedErr).Once()
	mockTopic1.On("ID").Return("test-topic-1").Maybe()

	// Topic 2 succeeds deletion
	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2.On("Delete", ctx).Return(nil).Once()
	mockTopic2.On("ID").Return("test-topic-2").Maybe()

	// Act
	err = manager.Teardown(ctx, servicemanager.Environment{}, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
}

func TestMessagingManager_Teardown_SubscriptionDeletionFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	ctx := context.Background()

	// Subscription 1 fails deletion
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once()
	expectedErr := errors.New("failed to delete subscription: permission denied")
	mockSub1.On("Delete", ctx).Return(expectedErr).Once()
	mockSub1.On("ID").Return("test-sub-1").Maybe()

	// Topic 1 and 2 succeed deletion
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	mockTopic1.On("Delete", ctx).Return(nil).Once()
	mockTopic1.On("ID").Return("test-topic-1").Maybe()

	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(true, nil).Once()
	mockTopic2.On("Delete", ctx).Return(nil).Once()
	mockTopic2.On("ID").Return("test-topic-2").Maybe()

	// Act
	err = manager.Teardown(ctx, servicemanager.Environment{}, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
}

func TestMessagingManager_Setup_TopicExistsUpdateFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	mockClient.On("Validate", resources).Return(nil).Once()

	// Topic 1 exists but update fails
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once()
	expectedErr := errors.New("failed to update topic: invalid config")
	mockTopic1.On("Update", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(nil, expectedErr).Once()

	// Assert that no other topics or subscriptions are attempted
	mockClient.AssertNotCalled(t, "Topic", "test-topic-2")
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
}

func TestMessagingManager_Setup_SubscriptionExistsUpdateFails(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Arrange
	mockClient := new(MockMessagingClient)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	resources := getTestMessagingResources()
	env := servicemanager.Environment{ProjectID: "test-project"}
	ctx := context.Background()

	mockClient.On("Validate", resources).Return(nil).Once()

	// Topic 1 and 2 succeed
	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(false, nil).Once()
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic1, nil).Once()

	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2).Once()
	mockTopic2.On("Exists", ctx).Return(false, nil).Once()
	mockClient.On("CreateTopicWithConfig", ctx, mock.AnythingOfType("servicemanager.TopicConfig")).Return(mockTopic2, nil).Once()

	// Subscription 1 exists but update fails
	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1).Once()
	mockSub1.On("Exists", ctx).Return(true, nil).Once() // Subscription exists

	// Safeguard topic check
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1).Once()
	mockTopic1.On("Exists", ctx).Return(true, nil).Once() // Topic should exist here

	expectedErr := errors.New("failed to update subscription: invalid ack deadline")
	mockSub1.On("Update", ctx, mock.AnythingOfType("servicemanager.SubscriptionConfig")).Return(nil, expectedErr).Once()

	// Act
	err = manager.Setup(ctx, env, resources)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
}
