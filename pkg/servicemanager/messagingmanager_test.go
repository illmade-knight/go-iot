package servicemanager_test

import (
	"context"
	"errors"
	"fmt"
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
func (m *MockMessagingTopic) Update(ctx context.Context, cfg servicemanager.MessagingTopicConfig) (*servicemanager.MessagingTopicConfig, error) {
	args := m.Called(ctx, cfg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.MessagingTopicConfig), args.Error(1)
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
func (m *MockMessagingSubscription) Update(ctx context.Context, cfg servicemanager.MessagingSubscriptionConfig) (*servicemanager.MessagingSubscriptionConfig, error) {
	args := m.Called(ctx, cfg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*servicemanager.MessagingSubscriptionConfig), args.Error(1)
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
func (m *MockMessagingClient) CreateTopicWithConfig(ctx context.Context, topicSpec servicemanager.MessagingTopicConfig) (servicemanager.MessagingTopic, error) {
	args := m.Called(ctx, topicSpec)
	return args.Get(0).(servicemanager.MessagingTopic), args.Error(1)
}
func (m *MockMessagingClient) CreateSubscription(ctx context.Context, subSpec servicemanager.MessagingSubscriptionConfig) (servicemanager.MessagingSubscription, error) {
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

// *** FIX: Correctly pass the arguments to the mock framework. ***
func (m *MockMessagingClient) Validate(resources servicemanager.ResourcesSpec) error {
	args := m.Called(resources)
	return args.Error(0)
}

// --- Helper Function ---

func getTestMessagingConfig() *servicemanager.TopLevelConfig {
	return &servicemanager.TopLevelConfig{
		DefaultProjectID: "test-project",
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "test-project"},
			"prod": {ProjectID: "prod-project", TeardownProtection: true},
		},
		Resources: servicemanager.ResourcesSpec{
			MessagingTopics: []servicemanager.MessagingTopicConfig{
				{Name: "test-topic-1"},
				{Name: "test-topic-2", Labels: map[string]string{"env": "test"}},
			},
			MessagingSubscriptions: []servicemanager.MessagingSubscriptionConfig{
				{Name: "test-sub-1", Topic: "test-topic-1", AckDeadlineSeconds: 30, MessageRetention: servicemanager.Duration(10 * time.Minute)},
			},
		},
	}
}

// --- Test Cases ---

func TestPubSubManager_Setup_CreateNewResources(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	config := getTestMessagingConfig()
	ctx := context.Background()

	// *** FIX: Add expectation for the Validate call. ***
	mockClient.On("Validate", config.Resources).Return(nil)

	mockTopic1 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateTopic", ctx, "test-topic-1").Return(mockTopic1, nil)
	mockTopic1.On("ID").Return("test-topic-1")

	mockTopic2 := new(MockMessagingTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateTopicWithConfig", ctx, config.Resources.MessagingTopics[1]).Return(mockTopic2, nil)
	mockTopic2.On("ID").Return("test-topic-2")

	mockSub1 := new(MockMessagingSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateSubscription", ctx, config.Resources.MessagingSubscriptions[0]).Return(mockSub1, nil)
	mockSub1.On("ID").Return("test-sub-1")

	// Act
	err = manager.Setup(ctx, config, "test")

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// *** ADDED: New test case for validation failure. ***
func TestPubSubManager_Setup_ValidationFails(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)

	config := getTestMessagingConfig()
	ctx := context.Background()

	expectedErr := errors.New("invalid subscription config")
	mockClient.On("Validate", config.Resources).Return(expectedErr)

	// Act
	err = manager.Setup(ctx, config, "test")

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockClient.AssertExpectations(t)
	// Ensure no other calls were made
	mockClient.AssertNotCalled(t, "Topic", mock.Anything)
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)
}

func TestPubSubManager_Teardown_Success(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)
	config := getTestMessagingConfig()
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

	// Act
	err = manager.Teardown(ctx, config, "test")

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestPubSubManager_Teardown_ProtectionEnabled(t *testing.T) {
	// Arrange
	mockClient := new(MockMessagingClient)
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(mockClient, logger)
	require.NoError(t, err)
	config := getTestMessagingConfig()
	ctx := context.Background()

	// Act
	err = manager.Teardown(ctx, config, "prod")

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, fmt.Sprintf("teardown protection enabled for environment: %s", "prod"))
	mockClient.AssertNotCalled(t, "Subscription", mock.Anything)
	mockClient.AssertNotCalled(t, "Topic", mock.Anything)
}
