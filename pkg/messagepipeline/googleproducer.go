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
	"github.com/rs/zerolog/log"
)

// GooglePubsubProducerConfig holds configuration for the Google Pub/Sub producer.
type GooglePubsubProducerConfig struct {
	ProjectID              string
	TopicID                string
	BatchSize              int
	BatchDelay             time.Duration
	InputChannelMultiplier int // New: Multiplier for BatchSize to determine input channel capacity
}

// LoadGooglePubsubProducerConfigFromEnv loads producer configuration from environment variables.
func LoadGooglePubsubProducerConfigFromEnv() (*GooglePubsubProducerConfig, error) {
	topicID := os.Getenv("PUBSUB_TOPIC_ID_ENRICHED_OUTPUT")

	cfg := &GooglePubsubProducerConfig{
		ProjectID:              os.Getenv("GCP_PROJECT_ID"),
		TopicID:                topicID,
		BatchSize:              100,                    // Default batch size
		BatchDelay:             100 * time.Millisecond, // Default batch delay
		InputChannelMultiplier: 2,                      // Default multiplier
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
			if val < 0 {
				log.Warn().Dur("invalid_delay", val).Msg("PUBSUB_PRODUCER_BATCH_DELAY is negative, producer will handle it.")
			}
			cfg.BatchDelay = val
		}
	}
	// Optionally load InputChannelMultiplier from env
	if icm := os.Getenv("PUBSUB_PRODUCER_INPUT_CHAN_MULTIPLIER"); icm != "" {
		if val, err := strconv.Atoi(icm); err == nil {
			cfg.InputChannelMultiplier = val
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

	// Validate InputChannelMultiplier
	if cfg.InputChannelMultiplier <= 0 {
		logger.Warn().Int("invalid_multiplier", cfg.InputChannelMultiplier).Msg("GooglePubsubProducerConfig.InputChannelMultiplier is non-positive. Defaulting to 1.")
		cfg.InputChannelMultiplier = 1
	}

	topic := client.Topic(cfg.TopicID)

	// Retry logic for topic existence check (keep this for production robustness)
	maxRetries := 3
	retryDelay := 100 * time.Millisecond
	exists := false
	var existsErr error

	for i := 0; i < maxRetries; i++ {
		topicCtx, topicCancel := context.WithTimeout(context.Background(), 5*time.Second)
		exists, existsErr = topic.Exists(topicCtx)
		topicCancel() // Ensure context is cancelled for each iteration

		if existsErr == nil && exists {
			logger.Debug().Str("topic_id", cfg.TopicID).Int("attempt", i+1).Msg("Topic confirmed to exist by producer's internal check.")
			break // Topic exists and no error, success!
		}

		if existsErr != nil {
			logger.Warn().Err(existsErr).Str("topic_id", cfg.TopicID).Int("attempt", i+1).
				Msg("NewGooglePubsubProducer: Failed to check existence of topic, retrying...")
		} else if !exists {
			logger.Warn().Str("topic_id", cfg.TopicID).Int("attempt", i+1).
				Msg("NewGooglePubsubProducer: Topic reported as not existing by producer's internal check, retrying...")
		}

		time.Sleep(retryDelay)
		retryDelay *= 2 // Exponential backoff
	}

	if existsErr != nil {
		return nil, fmt.Errorf("failed to check existence of topic %s after %d retries: %w", cfg.TopicID, maxRetries, existsErr)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist after %d retries", cfg.TopicID, maxRetries)
	}

	// Validate BatchDelay
	if cfg.BatchDelay < 0 {
		logger.Warn().Dur("invalid_delay", cfg.BatchDelay).Msg("GooglePubsubProducerConfig.BatchDelay is negative. Time-based batching will be disabled.")
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background()) // Producer manages its own shutdown context

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer initialized successfully.")

	return &GooglePubsubProducer[T]{
		client:       client,
		topic:        topic,
		logger:       logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
		inputChan:    make(chan *types.BatchedMessage[T], cfg.BatchSize*cfg.InputChannelMultiplier), // Use multiplier
		doneChan:     make(chan struct{}),
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
		batchSize:    cfg.BatchSize,
		batchDelay:   cfg.BatchDelay,
	}, nil
}

// NewGooglePubsubProducerWithExistingTopic creates a new GooglePubsubProducer
// by directly accepting an already created and validated *pubsub.Topic object.
// This is useful for testing or scenarios where the topic lifecycle is managed externally.
func NewGooglePubsubProducerWithExistingTopic[T any](
	client *pubsub.Client,
	topic *pubsub.Topic, // Direct injection of the topic object
	cfg *GooglePubsubProducerConfig,
	logger zerolog.Logger,
) (*GooglePubsubProducer[T], error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil for producer")
	}
	if topic == nil {
		return nil, fmt.Errorf("pubsub topic cannot be nil for producer")
	}
	// Optional: Sanity check that the config's topic ID matches the provided topic's ID
	if cfg.TopicID != topic.ID() {
		logger.Warn().Str("config_topic_id", cfg.TopicID).Str("provided_topic_id", topic.ID()).
			Msg("Producer config TopicID does not match provided Pub/Sub Topic object ID. Using provided topic object's ID.")
		cfg.TopicID = topic.ID() // Adjust config to match the actual topic object
	}

	// Validate InputChannelMultiplier
	if cfg.InputChannelMultiplier <= 0 {
		logger.Warn().Int("invalid_multiplier", cfg.InputChannelMultiplier).Msg("GooglePubsubProducerConfig.InputChannelMultiplier is non-positive. Defaulting to 1.")
		cfg.InputChannelMultiplier = 1
	}

	// No topic.Exists() check needed here, as the caller (test) has already validated it.

	// Validate BatchDelay
	if cfg.BatchDelay < 0 {
		logger.Warn().Dur("invalid_delay", cfg.BatchDelay).Msg("GooglePubsubProducerConfig.BatchDelay is negative. Time-based batching will be disabled.")
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer (WithExistingTopic) initialized successfully.")

	return &GooglePubsubProducer[T]{
		client:       client,
		topic:        topic, // Use the injected topic
		logger:       logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
		inputChan:    make(chan *types.BatchedMessage[T], cfg.BatchSize*cfg.InputChannelMultiplier), // Use multiplier
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
	defer func() {
		if p.topic != nil {
			p.logger.Info().Msg("Pub/Sub producer's internal loop stopping topic, flushing any remaining async messages...")
			p.topic.Stop() // This blocks until all outstanding messages are published.
			p.logger.Info().Msg("Pub/Sub producer's topic stopped.")
		}
		close(p.doneChan)
	}()

	var messages []*types.BatchedMessage[T]

	// Only create a ticker if the batch delay is a positive duration.
	var ticker *time.Ticker
	var tickerC <-chan time.Time
	if p.batchDelay > 0 {
		ticker = time.NewTicker(p.batchDelay)
		tickerC = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case msg, ok := <-p.inputChan:
			if !ok {
				p.logger.Info().Msg("Producer input channel closed, flushing remaining messages.")
				p.flush(messages)
				return
			}
			messages = append(messages, msg)
			if len(messages) >= p.batchSize {
				p.logger.Debug().Int("count", len(messages)).Msg("Batch size reached, flushing messages.")
				p.flush(messages)
				messages = nil // Reset batch
			}
		case <-tickerC: // This channel will be nil if batchDelay <= 0, so this case is effectively disabled.
			if len(messages) > 0 {
				p.logger.Debug().Int("count", len(messages)).Msg("Batch delay reached, flushing messages.")
				p.flush(messages)
				messages = nil // Reset batch
			}
		case <-p.shutdownCtx.Done():
			p.logger.Info().Msg("Producer shutdown context cancelled, flushing remaining messages.")
			p.flush(messages)
			return
		}
	}
}

// flush publishes a batch of messages to Pub/Sub.
func (p *GooglePubsubProducer[T]) flush(messages []*types.BatchedMessage[T]) {
	if len(messages) == 0 {
		return
	}

	p.logger.Debug().Int("count", len(messages)).Msg("Publishing batch to Pub/Sub.")

	var publishWg sync.WaitGroup

	for _, batchedMsg := range messages {
		payload, err := json.Marshal(batchedMsg.Payload)
		if err != nil {
			p.logger.Error().Err(err).
				Str("original_msg_id", batchedMsg.OriginalMessage.ID).
				Msg("Failed to marshal payload for publishing, Nacking original message.")
			batchedMsg.OriginalMessage.Nack()
			continue
		}

		publishWg.Add(1)
		go func(bm *types.BatchedMessage[T]) {
			defer publishWg.Done()

			// *** INCREASED TIMEOUT FOR PUBLISH CONTEXT ***
			publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second) // Increased from 5s
			defer publishCancel()

			res := p.topic.Publish(publishCtx, &pubsub.Message{
				Data: payload,
			})

			msgID, err := res.Get(publishCtx)

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
		}(batchedMsg)
	}

	publishWg.Wait()
}

// Stop gracefully shuts down the producer.
func (p *GooglePubsubProducer[T]) Stop() {
	p.logger.Info().Msg("Stopping Pub/Sub producer...")
	p.shutdownFunc()
	close(p.inputChan)
	p.wg.Wait()
	p.logger.Info().Msg("Pub/Sub producer stopped gracefully.")
	p.logger.Info().Msg("GooglePubsubProducer does not close the injected Pub/Sub client.")
}

// Done returns a channel that is closed when the producer has fully stopped.
func (p *GooglePubsubProducer[T]) Done() <-chan struct{} {
	return p.doneChan
}
