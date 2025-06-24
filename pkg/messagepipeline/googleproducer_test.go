package messagepipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline" // Import the package under test
	"github.com/illmade-knight/go-iot/pkg/types"           // For ConsumedMessage and BatchedMessage
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// =============================================================================
//  Test Helpers (Duplicated from consumer_test.go for self-containnation)
// =============================================================================

// setupTestPubsub initializes a pstest.Server and configures the necessary
// client options to connect to it. It also creates a topic for testing.
// t.Cleanup is used for server, client, and topic stopping.
func setupTestPubsub(t *testing.T, projectID, topicID string) (*pubsub.Client, *pubsub.Topic) {
	t.Helper()
	ctx := context.Background()

	srv := pstest.NewServer()

	iopt := grpc.WithTransportCredentials(insecure.NewCredentials())
	opts := []option.ClientOption{
		option.WithEndpoint(srv.Addr),
		option.WithGRPCDialOption(iopt),
		option.WithoutAuthentication(),
	}

	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)

	t.Cleanup(func() {
		topic.Stop()
		require.NoError(t, client.Close())
		require.NoError(t, srv.Close())
	})

	return client, topic
}

// createTestSubscription creates a temporary subscription to a given topic for receiving messages.
// t.Cleanup is used for subscription deletion.
func createTestSubscription(ctx context.Context, t *testing.T, client *pubsub.Client, topic *pubsub.Topic, subID string) *pubsub.Subscription {
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:            topic,
		AckDeadline:      10 * time.Second,
		ExpirationPolicy: time.Duration(0), // Never expire for testing
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		// Use a separate context for cleanup that doesn't time out with the test receiveCtx
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deleteCancel()
		sub.Delete(deleteCtx)
	})

	return sub
}

// TestPayload is a simple struct to use as the generic type T for testing.
type TestPayload struct {
	Value string `json:"value"`
	Index int    `json:"index"`
}

// =============================================================================
//  Test Cases for GooglePubsubProducer
// =============================================================================

// TestGooglePubsubProducer_Lifecycle_And_MessageProduction tests the full flow:
// Start -> Send Messages -> Flush -> Stop.
func TestGooglePubsubProducer_Lifecycle_And_MessageProduction(t *testing.T) {
	projectID := "test-producer-project"
	topicID := "test-producer-output-topic"
	pubsubClient, outputTopic := setupTestPubsub(t, projectID, topicID)

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
		// Future test case: Malformed message that fails JSON marshalling
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testOutputSubscriptionID := fmt.Sprintf("test-output-sub-%s-%d", tc.name, time.Now().UnixNano())
			testOutputSub := createTestSubscription(context.Background(), t, pubsubClient, outputTopic, testOutputSubscriptionID)

			producerConfig := &messagepipeline.GooglePubsubProducerConfig{
				ProjectID:  projectID,
				TopicID:    topicID,
				BatchSize:  tc.batchSize,
				BatchDelay: tc.batchDelay,
			}

			producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](
				pubsubClient,
				producerConfig,
				testLogger, // Use the active logger here
			)
			require.NoError(t, err, "Failed to create GooglePubsubProducer")

			producer.Start()

			var wgSend sync.WaitGroup
			ackCount := 0
			nackCount := 0
			var ackNackMu sync.Mutex

			for i := 0; i < tc.messagesToSend; i++ {
				wgSend.Add(1)
				messageID := fmt.Sprintf("original-msg-%d", i)
				originalMsg := types.ConsumedMessage{
					ID:      messageID,
					Payload: []byte(fmt.Sprintf(`{"originalData": "data-%d"}`, i)),
					Ack: func() {
						ackNackMu.Lock()
						ackCount++
						t.Logf("PRODUCER TEST: Original message ID '%s' acknowledged. Total Acks: %d", messageID, ackCount)
						ackNackMu.Unlock()
						wgSend.Done()
					},
					Nack: func() {
						ackNackMu.Lock()
						nackCount++
						t.Logf("PRODUCER TEST: Original message ID '%s' NACKed. Total Nacks: %d", messageID, nackCount)
						ackNackMu.Unlock()
						wgSend.Done()
					},
				}
				batchedMsg := &types.BatchedMessage[TestPayload]{
					OriginalMessage: originalMsg,
					Payload:         &TestPayload{Value: fmt.Sprintf("value-%d", i), Index: i},
				}
				select {
				case producer.Input() <- batchedMsg:
					t.Logf("PRODUCER TEST: Sent message with original ID '%s' to producer input", originalMsg.ID)
				case <-time.After(5 * time.Second):
					t.Errorf("PRODUCER TEST: Timeout sending message with original ID '%s' to producer input. Producer might be stuck.", originalMsg.ID)
					wgSend.Done()
				}
				time.Sleep(1 * time.Millisecond) // Small delay to allow the producer's internal goroutine to pick up the message
			}

			// Add a short sleep here for auto-flush cases to give Pub/Sub client internal buffers a moment.
			// This is a tactical workaround for potential emulator/client library timing quirks.
			if tc.messagesToSend > 0 && tc.batchDelay > 0 && tc.batchDelay < 1*time.Hour { // Only for cases with auto-flush by delay or multiple batches by size
				t.Logf("PRODUCER TEST: Giving a moment for auto-flushes to process for '%s'...", tc.name)
				time.Sleep(tc.batchDelay + 50*time.Millisecond) // Give a bit more than the batch delay
			} else if tc.messagesToSend > tc.batchSize && tc.batchDelay > 1*time.Minute { // For multiple batches by size with long delay
				t.Logf("PRODUCER TEST: Giving a moment for full batches to process for '%s'...", tc.name)
				time.Sleep(100 * time.Millisecond) // Small fixed delay after all sends
			}

			// --- Core synchronization logic remains ---
			// Always call Stop() to ensure remaining messages are flushed.
			producer.Stop()
			t.Logf("PRODUCER TEST: Called producer.Stop() for test case '%s'.", tc.name)

			// Wait for all messages sent to have their Ack/Nack callbacks invoked (now that Stop() has triggered flush).
			waitChan := make(chan struct{})
			go func() {
				wgSend.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
				t.Log("PRODUCER TEST: All messages acknowledged/nacked by producer callbacks.")
			case <-time.After(20 * time.Second): // Increased timeout to give ample time for forced flush
				t.Fatalf("PRODUCER TEST: Timeout waiting for producer to acknowledge/nack all messages. Acked: %d, Nacked: %d, Expected: %d",
					ackCount, nackCount, tc.messagesToSend)
			}

			// Wait for the producer's internal goroutine to fully stop.
			select {
			case <-producer.Done():
				t.Log("PRODUCER TEST: Producer confirmed stopped.")
			case <-time.After(5 * time.Second):
				t.Fatalf("PRODUCER TEST: Timeout waiting for producer to fully stop.")
			}

			// Assert ack/nack counts
			assert.Equal(t, tc.expectAckCount, ackCount, "Ack count mismatch")
			assert.Equal(t, tc.expectNackCount, nackCount, "Nack count mismatch")

			// Verify messages were actually published by receiving from the output subscription
			receivedMessages := make(chan TestPayload, tc.messagesToSend)
			receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer receiveCancel()

			var receivedCount int
			var mu sync.Mutex

			err = testOutputSub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
				var receivedPayload TestPayload
				err := json.Unmarshal(msg.Data, &receivedPayload)
				if err != nil {
					t.Errorf("PRODUCER TEST: Failed to unmarshal received message: %v", err)
					msg.Nack()
					return
				}
				receivedMessages <- receivedPayload
				mu.Lock()
				receivedCount++
				mu.Unlock()
				t.Logf("PRODUCER TEST: Received message from Pub/Sub topic. Pub/Sub ID: %s, Value: %s, Index: %d. Total Received: %d", msg.ID, receivedPayload.Value, receivedPayload.Index, receivedCount)
				msg.Ack()
			})
			if err != nil && err != context.Canceled {
				t.Errorf("PRODUCER TEST: Error receiving messages from Pub/Sub: %v", err)
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
	projectID := "test-init-project"
	topicID := "test-init-topic"
	client, _ := setupTestPubsub(t, projectID, topicID)

	logger := zerolog.Nop()

	t.Run("nil client", func(t *testing.T) {
		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: projectID, TopicID: topicID}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](nil, cfg, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pubsub client cannot be nil")
		assert.Nil(t, producer)
	})

	t.Run("non-existent topic", func(t *testing.T) {
		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: projectID, TopicID: "non-existent-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](client, cfg, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "topic non-existent-topic does not exist")
		assert.Nil(t, producer)
	})
}

// TestGooglePubsubProducer_ShutdownEnsuresFlush tests that Stop flushes remaining messages.
// This test case is specifically designed to confirm forced flushing on Stop().
func TestGooglePubsubProducer_ShutdownEnsuresFlush(t *testing.T) {
	projectID := "test-shutdown-flush-project"
	topicID := "test-shutdown-flush-topic"
	pubsubClient, outputTopic := setupTestPubsub(t, projectID, topicID)

	testOutputSubscriptionID := fmt.Sprintf("test-output-sub-%s-%d", "shutdown-flush", time.Now().UnixNano())
	testOutputSub := createTestSubscription(context.Background(), t, pubsubClient, outputTopic, testOutputSubscriptionID)

	testLogger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	producerConfig := &messagepipeline.GooglePubsubProducerConfig{
		ProjectID:  projectID,
		TopicID:    topicID,
		BatchSize:  10,            // Larger than messagesToSend to ensure no auto-flush
		BatchDelay: 1 * time.Hour, // Very long delay
	}

	producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](
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
		messageID := fmt.Sprintf("flush-msg-%d", i)
		originalMsg := types.ConsumedMessage{
			ID: messageID,
			Ack: func() {
				ackNackMu.Lock()
				ackCount++
				t.Logf("PRODUCER TEST: Original message ID '%s' acknowledged. Total Acks: %d (Shutdown test)", messageID, ackCount)
				ackNackMu.Unlock()
				wgSend.Done()
			},
			Nack: func() {
				ackNackMu.Lock()
				nackCount++
				t.Logf("PRODUCER TEST: Original message ID '%s' NACKed. Total Nacks: %d (Shutdown test)", messageID, nackCount)
				ackNackMu.Unlock()
				wgSend.Done()
			},
		}
		batchedMsg := &types.BatchedMessage[TestPayload]{
			OriginalMessage: originalMsg,
			Payload:         &TestPayload{Value: fmt.Sprintf("flush-value-%d", i), Index: i},
		}
		select {
		case producer.Input() <- batchedMsg:
			t.Logf("PRODUCER TEST: Sent message with original ID '%s' to producer input (Shutdown test)", originalMsg.ID)
		case <-time.After(5 * time.Second):
			t.Errorf("PRODUCER TEST: Timeout sending message with original ID '%s' to producer input during shutdown test. Producer might be stuck.", originalMsg.ID)
			wgSend.Done()
		}
		time.Sleep(1 * time.Millisecond)
	}

	producer.Stop()
	t.Logf("PRODUCER TEST: Called producer.Stop() for 'ShutdownEnsuresFlush' to force flush.")

	waitChan := make(chan struct{})
	go func() {
		wgSend.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		t.Log("PRODUCER TEST: All messages acknowledged/nacked by producer callbacks after forced flush.")
	case <-time.After(20 * time.Second):
		t.Fatalf("PRODUCER TEST: Timeout waiting for producer to acknowledge/nack all messages after Stop. Acked: %d, Nacked: %d, Expected: %d",
			ackCount, nackCount, numMessages)
	}

	select {
	case <-producer.Done():
		t.Log("PRODUCER TEST: Producer confirmed stopped.")
	case <-time.After(5 * time.Second):
		t.Fatalf("PRODUCER TEST: Timeout waiting for producer to fully stop.")
	}

	assert.Equal(t, numMessages, ackCount, "All messages should be ACKed after Stop")
	assert.Equal(t, 0, nackCount, "No messages should be NACKed")

	receivedMessages := make(chan TestPayload, numMessages)
	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			t.Errorf("PRODUCER TEST: Failed to unmarshal received message: %v", jsonErr)
		}
		t.Logf("PRODUCER TEST: Received message from Pub/Sub topic. Pub/Sub ID: %s, Value: %s, Index: %d. Total Received: %d (Shutdown test)", msg.ID, p.Value, p.Index, receivedCount)
		msg.Ack()
	})
	if err != nil && err != context.Canceled {
		t.Errorf("PRODUCER TEST: Error receiving messages from Pub/Sub: %v", err)
	}
	close(receivedMessages)

	assert.Equal(t, numMessages, receivedCount, "Number of messages received after forced flush mismatch")
}
