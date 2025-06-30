package messagepipeline_test

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
//  Test Helpers
// =============================================================================

func sanitizedTestName(t *testing.T) string {
	name := t.Name()
	reg := regexp.MustCompile(`[^a-zA-Z0-9-]+`)
	sanitized := reg.ReplaceAllString(name, "-")
	sanitized = regexp.MustCompile(`^-+|-+$`).ReplaceAllString(sanitized, "")
	if len(sanitized) > 20 {
		sanitized = sanitized[:20]
	}
	return sanitized
}

func setupTestPubsub(t *testing.T, projectID string, topicID string) (*pubsub.Client, *pubsub.Topic) {
	t.Helper()
	ctx := context.Background()
	srv := pstest.NewServer()
	t.Cleanup(func() { srv.Close() })

	opts := []option.ClientOption{
		option.WithEndpoint(srv.Addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}

	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)
	t.Cleanup(topic.Stop)

	return client, topic
}

type TestPayload struct {
	Value string `json:"value"`
	Index int    `json:"index"`
}

// =============================================================================
//  Test Cases for GooglePubsubProducer
// =============================================================================

func TestGooglePubsubProducer_Lifecycle_And_MessageProduction(t *testing.T) {
	baseProjectID := "test-producer-project"
	baseTopicID := "test-producer-output-topic"
	testLogger := zerolog.Nop()

	testCases := []struct {
		name           string
		messagesToSend int
		batchSize      int
		batchDelay     time.Duration
		expectAckCount int
	}{
		{"Single message, flushed by Stop", 1, 10, 1 * time.Hour, 1},
		{"Batch size reached", 5, 3, 1 * time.Hour, 5},
		{"Batch delay reached", 4, 10, 100 * time.Millisecond, 4},
		{"Multiple batches by size", 10, 3, 1 * time.Hour, 10},
		{"No messages sent", 0, 10, 100 * time.Millisecond, 0},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
			currentProjectID := fmt.Sprintf("%s-%s", baseProjectID, uniqueSuffix)
			dynamicTopicID := fmt.Sprintf("%s-%s", baseTopicID, uniqueSuffix)
			pubsubClient, outputTopic := setupTestPubsub(t, currentProjectID, dynamicTopicID)

			producerConfig := &messagepipeline.GooglePubsubProducerConfig{
				ProjectID:              currentProjectID,
				TopicID:                outputTopic.ID(),
				BatchSize:              tc.batchSize,
				BatchDelay:             tc.batchDelay,
				InputChannelMultiplier: 2,
			}

			producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](
				pubsubClient, producerConfig, testLogger,
			)
			require.NoError(t, err)
			producer.Start()

			var wgSend sync.WaitGroup
			var ackCount int32
			for i := 0; i < tc.messagesToSend; i++ {
				wgSend.Add(1)
				messageID := fmt.Sprintf("msg-%s-%d", uniqueSuffix, i)
				originalMsg := types.ConsumedMessage{
					PublishMessage: types.PublishMessage{ID: messageID},
					Ack: func() {
						atomic.AddInt32(&ackCount, 1)
						wgSend.Done()
					},
					Nack: func() { t.Errorf("Message %s was unexpectedly Nacked", messageID); wgSend.Done() },
				}
				producer.Input() <- &types.BatchedMessage[TestPayload]{
					OriginalMessage: originalMsg,
					Payload:         &TestPayload{Value: fmt.Sprintf("v-%d", i), Index: i},
				}
			}

			// Give the batcher time to process before shutting down.
			if tc.batchDelay < 1*time.Hour {
				time.Sleep(tc.batchDelay * 2)
			}
			producer.Stop()

			waitChan := make(chan struct{})
			go func() {
				wgSend.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
			case <-time.After(15 * time.Second): // Generous timeout
				t.Fatalf("Timeout waiting for all messages to be acknowledged. Got %d, want %d", ackCount, tc.expectAckCount)
			}

			assert.Equal(t, int32(tc.expectAckCount), atomic.LoadInt32(&ackCount), "Ack count mismatch")
		})
	}
}

func TestGooglePubsubProducer_InitializationErrors(t *testing.T) {
	t.Parallel()
	baseProjectID := "test-init-project"

	t.Run("nil client", func(t *testing.T) {
		t.Parallel()
		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: baseProjectID, TopicID: "any-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](nil, cfg, zerolog.Nop())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pubsub client cannot be nil")
		assert.Nil(t, producer)
	})

	t.Run("non-existent topic", func(t *testing.T) {
		t.Parallel()
		localUniqueID := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
		localProjectID := fmt.Sprintf("%s-%s", baseProjectID, localUniqueID)
		localSrv := pstest.NewServer()
		t.Cleanup(func() { localSrv.Close() })

		opts := []option.ClientOption{
			option.WithEndpoint(localSrv.Addr),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		}
		localClient, err := pubsub.NewClient(context.Background(), localProjectID, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { localClient.Close() })

		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: localProjectID, TopicID: "non-existent-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](localClient, cfg, zerolog.Nop())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "topic non-existent-topic does not exist")
		assert.Nil(t, producer)
	})
}
