//go:build integration

package servicemanager_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	emulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	emulatorPort  = "8085/tcp"
	testProjectID = "integration-test-project"
	testTopicName = "integration-test-topic"
	testSubName   = "integration-test-sub"
)

// --- Test Suite Setup ---

// PubSubIntegrationTestSuite defines a suite of tests that run against a real Pub/Sub emulator.
type PubSubIntegrationTestSuite struct {
	suite.Suite
	ctx                 context.Context
	cancel              context.CancelFunc
	emulatorHost        string
	emulatorCleanupFunc func()
	clientOptions       []option.ClientOption

	// A client that uses our adapter, which is the primary subject of our tests.
	adapterClient servicemanager.MessagingClient

	// A direct client used for verification, bypassing our adapter.
	verificationClient *pubsub.Client
}

// SetupSuite starts the Pub/Sub emulator container and initializes clients once for all tests in the suite.
func (s *PubSubIntegrationTestSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)

	// Start the emulator using testcontainers
	req := testcontainers.ContainerRequest{
		Image:        emulatorImage,
		ExposedPorts: []string{emulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testProjectID), "--host-port=0.0.0.0:8085"},
		WaitingFor:   wait.ForLog("Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(s.T(), err, "Failed to start Pub/Sub emulator container")

	s.emulatorCleanupFunc = func() {
		s.T().Log("Terminating Pub/Sub emulator container...")
		if termErr := container.Terminate(s.ctx); termErr != nil {
			s.T().Fatalf("failed to terminate container: %s", termErr)
		}
	}

	host, err := container.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := container.MappedPort(s.ctx, emulatorPort)
	require.NoError(s.T(), err)
	s.emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	s.T().Logf("Pub/Sub emulator container started, listening on: %s", s.emulatorHost)
	s.T().Setenv("PUBSUB_EMULATOR_HOST", s.emulatorHost)

	// --- Client Setup ---
	s.clientOptions = []option.ClientOption{
		option.WithEndpoint(s.emulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	// Create the real Google Pub/Sub client
	realGcpClient, err := pubsub.NewClient(s.ctx, testProjectID, s.clientOptions...)
	require.NoError(s.T(), err)

	// Create the adapter we want to test
	s.adapterClient = servicemanager.NewGoogleMessagingClientAdapter(realGcpClient)
	require.NotNil(s.T(), s.adapterClient)

	// Create the direct verification client
	s.verificationClient, err = pubsub.NewClient(s.ctx, testProjectID, s.clientOptions...)
	require.NoError(s.T(), err)
}

// TearDownSuite stops the emulator container and closes clients after all tests have run.
func (s *PubSubIntegrationTestSuite) TearDownSuite() {
	if s.adapterClient != nil {
		s.adapterClient.Close()
	}
	if s.verificationClient != nil {
		s.verificationClient.Close()
	}
	s.emulatorCleanupFunc()
	s.cancel()
}

// TearDownTest cleans up resources between tests to ensure they are isolated.
func (s *PubSubIntegrationTestSuite) TearDownTest() {
	s.T().Log("Tearing down resources between tests...")

	// Delete subscription first
	sub := s.verificationClient.Subscription(testSubName)
	if exists, _ := sub.Exists(s.ctx); exists {
		err := sub.Delete(s.ctx)
		// It's okay if it's not found, it might have been deleted by the test
		if err != nil && status.Code(err) != codes.NotFound {
			s.T().Fatalf("Failed to delete subscription in teardown: %v", err)
		}
	}

	// Then delete the topic
	topic := s.verificationClient.Topic(testTopicName)
	if exists, _ := topic.Exists(s.ctx); exists {
		err := topic.Delete(s.ctx)
		if err != nil && status.Code(err) != codes.NotFound {
			s.T().Fatalf("Failed to delete topic in teardown: %v", err)
		}
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run.
func TestPubSubIntegrationSuite(t *testing.T) {
	suite.Run(t, new(PubSubIntegrationTestSuite))
}

// --- Test Cases ---

// Test_01_Manager_SetupAndTeardown tests the full happy path of creating and deleting resources via the manager.
func (s *PubSubIntegrationTestSuite) Test_01_Manager_SetupAndTeardown() {
	// --- Arrange ---
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(s.adapterClient, logger)
	require.NoError(s.T(), err)

	yamlContent := fmt.Sprintf(`
resources:
  pubsub_topics:
    - name: "%s"
      labels: { "app": "test-runner" }
  pubsub_subscriptions:
    - name: "%s"
      topic: "%s"
      ack_deadline_seconds: 42
`, testTopicName, testSubName, testTopicName)

	config := s.createTestConfig(yamlContent)

	// --- Act: Setup ---
	err = manager.Setup(s.ctx, config, "integration")

	// --- Assert: Setup ---
	require.NoError(s.T(), err, "Manager.Setup should succeed")

	// Verify Topic was created correctly
	topic := s.verificationClient.Topic(testTopicName)
	exists, err := topic.Exists(s.ctx)
	require.NoError(s.T(), err)
	assert.True(s.T(), exists, "Topic should have been created")
	topicCfg, err := topic.Config(s.ctx)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "test-runner", topicCfg.Labels["app"])

	// Verify Subscription was created correctly
	sub := s.verificationClient.Subscription(testSubName)
	exists, err = sub.Exists(s.ctx)
	require.NoError(s.T(), err)
	assert.True(s.T(), exists, "Subscription should have been created")
	subCfg, err := sub.Config(s.ctx)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), topic.String(), subCfg.Topic.String())
	assert.Equal(s.T(), 42*time.Second, subCfg.AckDeadline)

	// --- Act: Teardown ---
	err = manager.Teardown(s.ctx, config, "integration")

	// --- Assert: Teardown ---
	require.NoError(s.T(), err, "Manager.Teardown should succeed")

	// Verify Topic is gone
	topicExists, err := s.verificationClient.Topic(testTopicName).Exists(s.ctx)
	require.NoError(s.T(), err)
	assert.False(s.T(), topicExists, "Topic should have been deleted")

	// Verify Subscription is gone
	subExists, err := s.verificationClient.Subscription(testSubName).Exists(s.ctx)
	require.NoError(s.T(), err)
	assert.False(s.T(), subExists, "Subscription should have been deleted")
}

// Test_02_Manager_UpdateExistingResources tests that running Setup a second time updates resources correctly.
func (s *PubSubIntegrationTestSuite) Test_02_Manager_UpdateExistingResources() {
	// --- Arrange ---
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewMessagingManager(s.adapterClient, logger)
	require.NoError(s.T(), err)

	// First, create the resources with initial config
	initialYaml := fmt.Sprintf(`
resources:
  pubsub_topics:
    - name: "%s"
      labels: { "version": "1" }
  pubsub_subscriptions:
    - name: "%s"
      topic: "%s"
      ack_deadline_seconds: 20
`, testTopicName, testSubName, testTopicName)
	initialConfig := s.createTestConfig(initialYaml)
	err = manager.Setup(s.ctx, initialConfig, "integration")
	require.NoError(s.T(), err, "Initial setup failed")

	// --- Act ---
	// Now, define a new config and run Setup again
	updatedYaml := fmt.Sprintf(`
resources:
  pubsub_topics:
    - name: "%s"
      labels: { "version": "2" } # Changed label
  pubsub_subscriptions:
    - name: "%s"
      topic: "%s"
      ack_deadline_seconds: 55 # Changed deadline
`, testTopicName, testSubName, testTopicName)
	updatedConfig := s.createTestConfig(updatedYaml)
	err = manager.Setup(s.ctx, updatedConfig, "integration")
	require.NoError(s.T(), err, "Update setup failed")

	// --- Assert ---
	// Verify Topic was updated
	topicCfg, err := s.verificationClient.Topic(testTopicName).Config(s.ctx)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "2", topicCfg.Labels["version"])

	// Verify Subscription was updated
	subCfg, err := s.verificationClient.Subscription(testSubName).Config(s.ctx)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 55*time.Second, subCfg.AckDeadline)
}

// Test_03_Adapter_CreateSubscriptionFailsForMissingTopic tests the adapter's logic directly.
func (s *PubSubIntegrationTestSuite) Test_03_Adapter_CreateSubscriptionFailsForMissingTopic() {
	// --- Arrange ---
	subSpec := servicemanager.MessagingSubscriptionConfig{
		Name:  testSubName,
		Topic: "this-topic-does-not-exist",
	}

	// --- Act ---
	// Use the adapter client directly
	_, err := s.adapterClient.CreateSubscription(s.ctx, subSpec)

	// --- Assert ---
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "its topic 'this-topic-does-not-exist' does not exist")

	// Verify that the subscription was indeed not created
	exists, err := s.verificationClient.Subscription(testSubName).Exists(s.ctx)
	require.NoError(s.T(), err)
	assert.False(s.T(), exists)
}

// createTestConfig is a helper to generate a valid config from a YAML string.
func (s *PubSubIntegrationTestSuite) createTestConfig(yamlContent string) *servicemanager.TopLevelConfig {
	fullYaml := fmt.Sprintf(`
default_project_id: "%s"
environments:
  integration:
    project_id: "%s"
%s
`, testProjectID, testProjectID, yamlContent)

	tmpDir := s.T().TempDir()
	filePath := filepath.Join(tmpDir, "integration_test_config.yaml")
	err := os.WriteFile(filePath, []byte(fullYaml), 0600)
	require.NoError(s.T(), err)

	cfg, err := servicemanager.LoadAndValidateConfig(filePath)
	require.NoError(s.T(), err)
	return cfg
}
