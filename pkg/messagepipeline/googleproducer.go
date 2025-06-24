package messagepipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
)

// GooglePubsubProducerConfig holds configuration for the Google Pub/Sub producer.
type GooglePubsubProducerConfig struct {
	ProjectID  string
	TopicID    string
	BatchSize  int           // New: for internal buffering
	BatchDelay time.Duration // New: for internal buffering
}

// LoadGooglePubsubProducerConfigFromEnv loads producer configuration from environment variables.
func LoadGooglePubsubProducerConfigFromEnv() (*GooglePubsubProducerConfig, error) {
	topicID := os.Getenv("PUBSUB_TOPIC_ID_ENRICHED_OUTPUT")

	cfg := &GooglePubsubProducerConfig{
		ProjectID:  os.Getenv("GCP_PROJECT_ID"),
		TopicID:    topicID,
		BatchSize:  100,                    // Default batch size
		BatchDelay: 100 * time.Millisecond, // Default batch delay
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub producer")
	}
	if cfg.TopicID == "" {
		return nil, errors.New("PUBSUB_TOPIC_ID_ENRICHED_OUTPUT environment variable not set for Pub/Sub producer")
	}
	// Optionally load BatchSize and BatchDelay from env
	if bs := os.Getenv("PUBSUB_PRODUCER_BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			cfg.BatchSize = val
		}
	}
	if bd := os.Getenv("PUBSUB_PRODUCER_BATCH_DELAY"); bd != "" {
		if val, err := time.ParseDuration(bd); err == nil {
			cfg.BatchDelay = val
		}
	}

	return cfg, nil
}

// GooglePubsubProducer implements MessageProcessor for publishing to Google Cloud Pub/Sub.
type GooglePubsubProducer[T any] struct {
	client       *pubsub.Client
	topic        *pubsub.Topic
	logger       zerolog.Logger
	inputChan    chan *types.BatchedMessage[T] // Channel to receive messages from ProcessingService
	doneChan     chan struct{}                 // Signals full shutdown
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
	wg           sync.WaitGroup // For batchProcessorLoop goroutine
	batchSize    int
	batchDelay   time.Duration
}

// NewGooglePubsubProducer creates a new GooglePubsubProducer.
// It takes an existing *pubsub.Client instance, allowing for dependency injection.
func NewGooglePubsubProducer[T any](
	client *pubsub.Client,
	cfg *GooglePubsubProducerConfig,
	logger zerolog.Logger,
) (*GooglePubsubProducer[T], error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil for producer")
	}

	topic := client.Topic(cfg.TopicID)

	topicCtx, topicCancel := context.WithTimeout(context.Background(), time.Second*10)
	defer topicCancel()
	// Check if the topic exists.
	exists, err := topic.Exists(topicCtx) // Use passed ctx for initial check
	if err != nil {
		return nil, fmt.Errorf("failed to check existence of topic %s: %w", cfg.TopicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist", cfg.TopicID)
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background()) // Producer manages its own shutdown context

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer initialized successfully.")

	return &GooglePubsubProducer[T]{
		client:       client,
		topic:        topic,
		logger:       logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
		inputChan:    make(chan *types.BatchedMessage[T], cfg.BatchSize*2), // Buffered channel
		doneChan:     make(chan struct{}),
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
		batchSize:    cfg.BatchSize,
		batchDelay:   cfg.BatchDelay,
	}, nil
}

// Input returns the channel to send messages to the producer.
func (p *GooglePubsubProducer[T]) Input() chan<- *types.BatchedMessage[T] {
	return p.inputChan
}

// Start initiates the producer's internal processing loop.
func (p *GooglePubsubProducer[T]) Start() {
	p.logger.Info().Msg("Starting Pub/Sub producer batcher...")
	p.wg.Add(1)
	go p.batchProcessorLoop()
}

// batchProcessorLoop handles batching and publishing messages.
func (p *GooglePubsubProducer[T]) batchProcessorLoop() {
	defer p.wg.Done()
	// *** CRITICAL CHANGE: Ensure p.topic.Stop() is called before doneChan is closed ***
	// This ensures all pending Pub/Sub publishes complete before signaling Done.
	defer func() {
		if p.topic != nil {
			p.logger.Info().Msg("Pub/Sub producer's internal loop stopping topic, flushing any remaining async messages...")
			p.topic.Stop() // This blocks until all outstanding messages are published.
			p.logger.Info().Msg("Pub/Sub producer's topic stopped.")
		}
		close(p.doneChan) // Now close doneChan AFTER topic.Stop()
	}()

	var messages []*types.BatchedMessage[T]
	ticker := time.NewTicker(p.batchDelay)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-p.inputChan:
			if !ok {
				p.logger.Info().Msg("Producer input channel closed, flushing remaining messages.")
				p.flush(messages)
				return // Channel closed, exit loop, triggers defer
			}
			messages = append(messages, msg)
			if len(messages) >= p.batchSize {
				p.logger.Debug().Int("count", len(messages)).Msg("Batch size reached, flushing messages.")
				p.flush(messages)
				messages = nil // Reset batch
			}
		case <-ticker.C:
			if len(messages) > 0 {
				p.logger.Debug().Int("count", len(messages)).Msg("Batch delay reached, flushing messages.")
				p.flush(messages)
				messages = nil // Reset batch
			}
		case <-p.shutdownCtx.Done():
			p.logger.Info().Msg("Producer shutdown context cancelled, flushing remaining messages.")
			p.flush(messages)
			return // Shutdown signal, exit loop, triggers defer
		}
	}
}

// flush publishes a batch of messages to Pub/Sub.
func (p *GooglePubsubProducer[T]) flush(messages []*types.BatchedMessage[T]) {
	if len(messages) == 0 {
		return
	}

	p.logger.Debug().Int("count", len(messages)).Msg("Publishing batch to Pub/Sub.")

	var publishWg sync.WaitGroup // New: WaitGroup for this flush operation's publishes

	for _, batchedMsg := range messages {
		// Marshal each payload individually
		payload, err := json.Marshal(batchedMsg.Payload)
		if err != nil {
			p.logger.Error().Err(err).
				Str("original_msg_id", batchedMsg.OriginalMessage.ID).
				Msg("Failed to marshal payload for publishing, Nacking original message.")
			batchedMsg.OriginalMessage.Nack()
			continue
		}

		publishWg.Add(1) // Increment for each publish initiated
		go func(bm *types.BatchedMessage[T]) {
			defer publishWg.Done() // Ensure Done is called when this goroutine finishes

			// Use a context with a timeout for the publish itself.
			publishCtx, publishCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer publishCancel()

			res := p.topic.Publish(publishCtx, &pubsub.Message{
				Data: payload,
			})

			// Wait for the result of this specific publish.
			msgID, err := res.Get(publishCtx) // Use the same publishCtx for Get()

			if err != nil {
				p.logger.Error().Err(err).
					Str("original_msg_id", bm.OriginalMessage.ID).
					Msg("Failed to get publish result, Nacking original message.")
				bm.OriginalMessage.Nack()
			} else {
				p.logger.Debug().
					Str("original_msg_id", bm.OriginalMessage.ID).
					Str("pubsub_msg_id", msgID).
					Msg("Message published successfully and confirmed by Pub/Sub.")
				bm.OriginalMessage.Ack()
			}
		}(batchedMsg) // Pass batchedMsg to the goroutine
	}

	// Wait for all publishes in this batch to complete and their callbacks invoked.
	publishWg.Wait() // This ensures all res.Get() calls have returned before flush() exits.
}

// Stop gracefully shuts down the producer.
func (p *GooglePubsubProducer[T]) Stop() {
	p.logger.Info().Msg("Stopping Pub/Sub producer...")
	// 1. Signal the batchProcessorLoop to stop and flush
	p.shutdownFunc()
	// 2. Close the input channel to unblock the loop's select statement if no more messages are coming
	close(p.inputChan)
	// 3. Wait for the batchProcessorLoop to finish (which now includes calling p.topic.Stop() and closing p.doneChan)
	p.wg.Wait()
	p.logger.Info().Msg("Pub/Sub producer stopped gracefully.")

	// The client is passed in, so the caller owns its lifecycle.
	// We should not close a client we didn't create.
	p.logger.Info().Msg("GooglePubsubProducer does not close the injected Pub/Sub client.")
}

// Done returns a channel that is closed when the producer has fully stopped.
func (p *GooglePubsubProducer[T]) Done() <-chan struct{} {
	return p.doneChan
}
