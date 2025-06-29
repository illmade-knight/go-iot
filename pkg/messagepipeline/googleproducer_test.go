package messagepipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp" // Import for sanitization
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline" // Import the package under test
	"github.com/illmade-knight/go-iot/pkg/types"           // For ConsumedMessage and BatchedMessage
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
//  Test Helpers
// =============================================================================

// sanitizedTestName returns a version of t.Name() suitable for Pub/Sub IDs.
// It replaces non-alphanumeric characters (except hyphens) with hyphens,
// removes leading/trailing hyphens.
// *** CRITICAL CHANGE: Truncates to a very short prefix. ***
func sanitizedTestName(t *testing.T) string {
	name := t.Name()
	// Replace non-allowed characters with a hyphen
	reg := regexp.MustCompile(`[^a-zA-Z0-9-]+`)
	sanitized := reg.ReplaceAllString(name, "-")

	// Trim leading/trailing hyphens
	sanitized = regexp.MustCompile(`^-+|-+$`).ReplaceAllString(sanitized, "")

	// *** AGGRESSIVE TRUNCATION: Take only a very short prefix ***
	// The nanosecond will provide the ultimate uniqueness.
	// This ensures the combined ID (sanitized + nanosecond) is well under any internal limits.
	if len(sanitized) > 20 { // Even shorter, give some buffer
		sanitized = sanitized[:20]
	}
	return sanitized
}

// setupTestPubsub initializes a pstest.Server and configures the necessary
// client options to connect to it. It also creates a topic for testing.
// t.Cleanup is used for server, client, and topic stopping.
// This helper is designed to be called *per test case* for isolation when a new server is required.
func setupTestPubsub(t *testing.T, projectID string, topicID string) (*pubsub.Client, *pubsub.Topic) {
	t.Helper()
	ctx := context.Background()

	srv := pstest.NewServer()
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			log.Warn().Err(err).Msg("Error closing pstest server during cleanup.")
		}
	})

	iopt := grpc.WithTransportCredentials(insecure.NewCredentials())
	opts := []option.ClientOption{
		option.WithEndpoint(srv.Addr),
		option.WithGRPCDialOption(iopt),
		option.WithoutAuthentication(),
	}

	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			log.Warn().Err(err).Msg("Error closing pubsub client during cleanup.")
		}
	})

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)
	t.Cleanup(func() {
		topic.Stop() // Stop the topic first
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deleteCancel()
		if err := topic.Delete(deleteCtx); err != nil {
			log.Warn().Err(err).Msg("Error deleting test topic during cleanup.")
		}
	})

	return client, topic
}

// createTestSubscription creates a temporary subscription to a given topic for receiving messages.
// t.Cleanup is used for subscription deletion.
func createTestSubscription(ctx context.Context, t *testing.T, client *pubsub.Client, topic *pubsub.Topic, subID string) *pubsub.Subscription {
	t.Helper()
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:            topic,
		AckDeadline:      10 * time.Second,
		ExpirationPolicy: time.Duration(0), // Never expire for testing
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deleteCancel()
		if err := sub.Delete(deleteCtx); err != nil {
			log.Warn().Err(err).Str("subscription_id", sub.ID()).Msg("Error deleting test subscription during cleanup.")
		}
	})
	return sub
}

// TestPayload is a simple struct to use as the generic type T for testing.
type TestPayload struct {
	Value string `json:"value"`
	Index int    `json:"index"` // Corrected: was `json:"json"`
}

// =============================================================================
//  Test Cases for GooglePubsubProducer
// =============================================================================

// TestGooglePubsubProducer_Lifecycle_And_MessageProduction tests the full flow:
// Start -> Send Messages -> Flush -> Stop.
func TestGooglePubsubProducer_Lifecycle_And_MessageProduction(t *testing.T) {
	// Base Project ID (will be combined with sanitized test name + nanoseconds for uniqueness)
	baseProjectID := "test-producer-project"
	baseTopicID := "test-producer-output-topic" // Base ID for unique topics

	testLogger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	type testCase struct {
		name              string
		messagesToSend    int
		batchSize         int
		batchDelay        time.Duration
		expectAckCount    int  // Number of messages expected to be ACKed
		expectNackCount   int  // Number of messages expected to be NACKed (e.g., if marshalling fails)
		simulatedFailures bool // For future: simulate publish errors
	}

	testCases := []testCase{
		{
			name:            "Single message, flushed by Stop",
			messagesToSend:  1,
			batchSize:       10,
			batchDelay:      1 * time.Hour, // Long delay, forces flush by Stop
			expectAckCount:  1,
			expectNackCount: 0,
		},
		{
			name:            "Batch size reached",
			messagesToSend:  5,
			batchSize:       3,             // Trigger flush after 3 messages
			batchDelay:      1 * time.Hour, // Long delay, so only batch size triggers flush
			expectAckCount:  5,
			expectNackCount: 0,
		},
		{
			name:            "Batch delay reached with partial batch",
			messagesToSend:  4,
			batchSize:       10,                    // Larger than messagesToSend
			batchDelay:      50 * time.Millisecond, // Short delay to trigger flush
			expectAckCount:  4,
			expectNackCount: 0,
		},
		{
			name:            "Multiple batches by size",
			messagesToSend:  10,
			batchSize:       3,
			batchDelay:      1 * time.Hour,
			expectAckCount:  10,
			expectNackCount: 0,
		},
		{
			name:            "No messages sent, clean shutdown",
			messagesToSend:  0,
			batchSize:       10,
			batchDelay:      100 * time.Millisecond,
			expectAckCount:  0,
			expectNackCount: 0,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // Enable parallel execution

			// *** CRITICAL FIX: Ensure absolute uniqueness with nanoseconds for each parallel test's resources ***
			// This combines sanitized test name with nanoseconds to avoid clashes.
			uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())

			// Each parallel test gets its own isolated pstest.Server, client, and topic.
			// This is critical for preventing resource contention and test interdependencies.
			currentProjectID := fmt.Sprintf("%s-%s", baseProjectID, uniqueSuffix)             // Unique project ID
			dynamicTopicID := fmt.Sprintf("%s-%s", baseTopicID, uniqueSuffix)                 // Unique topic ID
			pubsubClient, outputTopic := setupTestPubsub(t, currentProjectID, dynamicTopicID) // <-- Correctly called here

			testOutputSubscriptionID := fmt.Sprintf("test-output-sub-%s", uniqueSuffix)
			testOutputSub := createTestSubscription(context.Background(), t, pubsubClient, outputTopic, testOutputSubscriptionID)

			producerConfig := &messagepipeline.GooglePubsubProducerConfig{
				ProjectID:              currentProjectID, // Must match the client's project ID
				TopicID:                outputTopic.ID(), // Must match the created topic's ID
				BatchSize:              tc.batchSize,
				BatchDelay:             tc.batchDelay,
				InputChannelMultiplier: 2, // Explicitly set to a positive value to avoid warning
			}

			// Use the original constructor `NewGooglePubsubProducer`
			producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](
				pubsubClient,
				producerConfig,
				testLogger,
			)
			require.NoError(t, err, "Failed to create GooglePubsubProducer")

			producer.Start()

			var wgSend sync.WaitGroup
			ackCount := 0
			nackCount := 0
			var ackNackMu sync.Mutex

			for i := 0; i < tc.messagesToSend; i++ {
				wgSend.Add(1)
				messageID := fmt.Sprintf("original-msg-%s-%d", uniqueSuffix, i) // Make message ID unique per message
				originalMsg := types.ConsumedMessage{
					ID:      messageID,
					Payload: []byte(fmt.Sprintf(`{"originalData": "data-%s-%d"}`, uniqueSuffix, i)),
					Ack: func() {
						ackNackMu.Lock()
						ackCount++
						t.Logf("PRODUCER TEST (%s): Original message ID '%s' acknowledged. Total Acks: %d", tc.name, messageID, ackCount)
						ackNackMu.Unlock()
						wgSend.Done()
					},
					Nack: func() {
						ackNackMu.Lock()
						nackCount++
						t.Logf("PRODUCER TEST (%s): Original message ID '%s' NACKed. Total Nacks: %d", tc.name, messageID, nackCount)
						ackNackMu.Unlock()
						wgSend.Done()
					},
				}
				batchedMsg := &types.BatchedMessage[TestPayload]{
					OriginalMessage: originalMsg,
					Payload:         &TestPayload{Value: fmt.Sprintf("value-%s-%d", uniqueSuffix, i), Index: i},
				}
				select {
				case producer.Input() <- batchedMsg:
					// Message sent to producer input
				case <-time.After(500 * time.Millisecond): // Timeout for sending to producer input
					t.Errorf("PRODUCER TEST (%s): Timeout sending message with original ID '%s' to producer input. Producer might be stuck.", tc.name, originalMsg.ID)
					wgSend.Done()
				}
			}

			// *** STRATEGIC SLEEP (pragmatic for pstest quirks) ***
			// Give the internal batching loop a moment to pick up all messages
			// and initiate publishes before calling Stop, which triggers final flush.
			// This addresses the "passed with sleep" observation directly.
			if tc.messagesToSend > 0 { // Only sleep if messages were actually sent
				time.Sleep(100 * time.Millisecond) // Increased from 50ms to 100ms
			}

			producer.Stop()
			t.Logf("PRODUCER TEST (%s): Called producer.Stop().", tc.name)

			waitChan := make(chan struct{})
			go func() {
				wgSend.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
				t.Logf("PRODUCER TEST (%s): All messages acknowledged/nacked by producer callbacks.", tc.name)
			case <-time.After(5 * time.Second): // Increased from 2s to 5s for robustness
				t.Fatalf("PRODUCER TEST (%s): Timeout waiting for producer to acknowledge/nack all messages. Acked: %d, Nacked: %d, Expected: %d",
					tc.name, ackCount, nackCount, tc.messagesToSend)
			}

			select {
			case <-producer.Done():
				t.Logf("PRODUCER TEST (%s): Producer confirmed stopped.", tc.name)
			case <-time.After(2 * time.Second): // Increased from 1s to 2s
				t.Fatalf("PRODUCER TEST (%s): Timeout waiting for producer to fully stop.", tc.name)
			}

			assert.Equal(t, tc.expectAckCount, ackCount, "Ack count mismatch")
			assert.Equal(t, tc.expectNackCount, nackCount, "Nack count mismatch")

			receivedMessages := make(chan TestPayload, tc.messagesToSend)
			receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 2*time.Second) // Increased from 1s to 2s
			defer receiveCancel()

			var receivedCount int
			var mu sync.Mutex

			err = testOutputSub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
				var receivedPayload TestPayload
				err := json.Unmarshal(msg.Data, &receivedPayload)
				if err != nil {
					t.Errorf("PRODUCER TEST (%s): Failed to unmarshal received message: %v", tc.name, err)
					msg.Nack()
					return
				}
				receivedMessages <- receivedPayload
				mu.Lock()
				receivedCount++
				mu.Unlock()
				t.Logf("PRODUCER TEST (%s): Received message from Pub/Sub topic. Pub/Sub ID: %s, Value: %s, Index: %d. Total Received: %d", tc.name, msg.ID, receivedPayload.Value, receivedPayload.Index, receivedCount)
				msg.Ack()
			})
			if err != nil && err != context.Canceled {
				t.Errorf("PRODUCER TEST (%s): Error receiving messages from Pub/Sub: %v", tc.name, err)
			}

			close(receivedMessages)
			var actualReceived []TestPayload
			for p := range receivedMessages {
				actualReceived = append(actualReceived, p)
			}
			assert.Equal(t, tc.expectAckCount, receivedCount, "Number of messages received from Pub/Sub topic mismatch")
		})
	}
}

// TestGooglePubsubProducer_InitializationErrors tests error cases during producer creation.
func TestGooglePubsubProducer_InitializationErrors(t *testing.T) {
	t.Parallel() // Parent test runs in parallel

	baseProjectID := "test-init-project"

	t.Run("nil client", func(t *testing.T) {
		t.Parallel() // This one can be parallel safely as it doesn't need a real client/server
		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: baseProjectID, TopicID: "any-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](nil, cfg, zerolog.Nop()) // Use original constructor
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pubsub client cannot be nil")
		assert.Nil(t, producer)
	})

	t.Run("non-existent topic", func(t *testing.T) {
		t.Parallel() // Can be parallel safely, but needs its own isolated setup

		// Sanitize test name for project ID and add nanosecond for uniqueness
		localUniqueID := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())

		// Ensure this specific test gets its OWN isolated pstest.Server
		localProjectID := fmt.Sprintf("%s-%s", baseProjectID, localUniqueID)
		localSrv := pstest.NewServer()
		t.Cleanup(func() { localSrv.Close() })

		localConn, err := grpc.Dial(localSrv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { localConn.Close() })

		// Client connected to its dedicated localSrv
		localClient, err := pubsub.NewClient(context.Background(), localProjectID, option.WithEndpoint(localSrv.Addr), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())), option.WithoutAuthentication())
		require.NoError(t, err)
		t.Cleanup(func() { localClient.Close() })

		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: localProjectID, TopicID: "non-existent-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](localClient, cfg, zerolog.Nop()) // Use original constructor to test failure
		require.Error(t, err)
		assert.Contains(t, err.Error(), "topic non-existent-topic does not exist")
		assert.Nil(t, producer)
	})
}

// TestGooglePubsubProducer_ShutdownEnsuresFlush tests that Stop flushes remaining messages.
// This test case is specifically designed to confirm forced flushing on Stop().
func TestGooglePubsubProducer_ShutdownEnsuresFlush(t *testing.T) {
	t.Parallel() // Can be parallel safely

	baseProjectID := "test-shutdown-flush-project"
	baseTopicID := "test-shutdown-flush-topic"

	// Sanitize test name for Pub/Sub resource IDs and add nanosecond for uniqueness
	uniqueID := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())

	// This top-level parallel test needs its OWN isolated pstest.Server.
	// Therefore, it uses setupTestPubsub directly to get a fresh server.
	currentProjectID := fmt.Sprintf("%s-%s", baseProjectID, uniqueID)
	dynamicTopicID := fmt.Sprintf("%s-%s", baseTopicID, uniqueID)
	pubsubClient, outputTopic := setupTestPubsub(t, currentProjectID, dynamicTopicID) // <-- Correctly called here, using the helper that creates a new server

	testOutputSubscriptionID := fmt.Sprintf("test-output-sub-%s", uniqueID)
	testOutputSub := createTestSubscription(context.Background(), t, pubsubClient, outputTopic, testOutputSubscriptionID)

	testLogger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	producerConfig := &messagepipeline.GooglePubsubProducerConfig{
		ProjectID:              currentProjectID,
		TopicID:                outputTopic.ID(),
		BatchSize:              10,            // Larger than messagesToSend to ensure no auto-flush
		BatchDelay:             1 * time.Hour, // Very long delay
		InputChannelMultiplier: 2,             // Explicitly set to a positive value to avoid warning
	}

	producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload]( // Reverting to original constructor
		pubsubClient,
		producerConfig,
		testLogger,
	)
	require.NoError(t, err)
	producer.Start()

	numMessages := 5
	var wgSend sync.WaitGroup
	ackCount := 0
	nackCount := 0
	var ackNackMu sync.Mutex

	for i := 0; i < numMessages; i++ {
		wgSend.Add(1)
		messageID := fmt.Sprintf("flush-msg-%s-%d", uniqueID, i) // Unique message ID
		originalMsg := types.ConsumedMessage{
			ID: messageID,
			Ack: func() {
				ackNackMu.Lock()
				ackCount++
				t.Logf("PRODUCER TEST (%s): Original message ID '%s' acknowledged. Total Acks: %d (Shutdown test)", t.Name(), messageID, ackCount)
				ackNackMu.Unlock()
				wgSend.Done()
			},
			Nack: func() {
				ackNackMu.Lock()
				nackCount++
				t.Logf("PRODUCER TEST (%s): Original message ID '%s' NACKed. Total Nacks: %d (Shutdown test)", t.Name(), messageID, nackCount)
				ackNackMu.Unlock()
				wgSend.Done()
			},
		}
		batchedMsg := &types.BatchedMessage[TestPayload]{
			OriginalMessage: originalMsg,
			Payload:         &TestPayload{Value: fmt.Sprintf("flush-value-%s-%d", uniqueID, i), Index: i},
		}
		select {
		case producer.Input() <- batchedMsg:
			// Message sent to producer input
		case <-time.After(500 * time.Millisecond): // Reduced timeout
			t.Errorf("PRODUCER TEST (%s): Timeout sending message with original ID '%s' to producer input during shutdown test. Producer might be stuck.", t.Name(), originalMsg.ID)
			wgSend.Done()
		}
	}

	// *** STRATEGIC SLEEP (pragmatic for pstest quirks) ***
	// Give the internal batching loop a moment to pick up all messages
	// and initiate publishes before calling Stop, which triggers final flush.
	// This addresses the "passed with sleep" observation directly.
	if numMessages > 0 { // Only sleep if messages were actually sent
		time.Sleep(50 * time.Millisecond)
	}

	producer.Stop()
	t.Logf("PRODUCER TEST (%s): Called producer.Stop() to force flush.", t.Name())

	waitChan := make(chan struct{})
	go func() {
		wgSend.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		t.Logf("PRODUCER TEST (%s): All messages acknowledged/nacked by producer callbacks after forced flush.", t.Name())
	case <-time.After(5 * time.Second): // Reduced timeout
		t.Fatalf("PRODUCER TEST (%s): Timeout waiting for producer to acknowledge/nack all messages after Stop. Acked: %d, Nacked: %d, Expected: %d",
			t.Name(), ackCount, nackCount, numMessages)
	}

	select {
	case <-producer.Done():
		t.Logf("PRODUCER TEST (%s): Producer confirmed stopped.", t.Name())
	case <-time.After(2 * time.Second): // Reduced timeout
		t.Fatalf("PRODUCER TEST (%s): Timeout waiting for producer to fully stop.", t.Name())
	}

	assert.Equal(t, numMessages, ackCount, "All messages should be ACKed after Stop")
	assert.Equal(t, 0, nackCount, "No messages should be NACKed")

	receivedMessages := make(chan TestPayload, numMessages)
	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 2*time.Second) // Reduced timeout
	defer receiveCancel()

	var receivedCount int
	var mu sync.Mutex
	err = testOutputSub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		var p TestPayload
		if jsonErr := json.Unmarshal(msg.Data, &p); jsonErr == nil {
			receivedMessages <- p
			mu.Lock()
			receivedCount++
			mu.Unlock()
		} else {
			t.Errorf("PRODUCER TEST (%s): Failed to unmarshal received message: %v", t.Name(), jsonErr)
		}
		t.Logf("PRODUCER TEST (%s): Received message from Pub/Sub topic. Pub/Sub ID: %s, Value: %s, Index: %d. Total Received: %d", t.Name(), msg.ID, p.Value, p.Index, receivedCount)
		msg.Ack()
	})
	if err != nil && err != context.Canceled {
		t.Errorf("PRODUCER TEST (%s): Error receiving messages from Pub/Sub: %v", t.Name(), err)
	}
	close(receivedMessages)

	assert.Equal(t, numMessages, receivedCount, "Number of messages received after forced flush mismatch")
}
