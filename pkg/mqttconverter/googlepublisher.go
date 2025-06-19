package mqttconverter

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"os"
	"time"
)

// --- Google Cloud Pub/Sub Publisher Implementation ---

// GooglePubsubPublisherConfig holds configuration for the Pub/Sub publisher.
type GooglePubsubPublisherConfig struct {
	ProjectID       string
	TopicID         string
	ClientOptions   []option.ClientOption
	PublishSettings pubsub.PublishSettings
}

func GetDefaultPublishSettings() pubsub.PublishSettings {
	return pubsub.PublishSettings{
		DelayThreshold: 100 * time.Millisecond,
		CountThreshold: 100,
		ByteThreshold:  1e6,
		NumGoroutines:  10,
		Timeout:        60 * time.Second,
	}
}

// LoadGooglePubsubPublisherConfigFromEnv loads Pub/Sub configuration from environment variables.
func LoadGooglePubsubPublisherConfigFromEnv(topicID string) (*GooglePubsubPublisherConfig, error) {
	cfg := &GooglePubsubPublisherConfig{
		ProjectID: os.Getenv("GCP_PROJECT_ID"),
		TopicID:   os.Getenv(topicID),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub")
	}
	if cfg.TopicID == "" {
		return nil, fmt.Errorf("%s environment variable not set for Pub/Sub", topicID)
	}

	credentialsFile := os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE")
	if credentialsFile != "" {
		cfg.ClientOptions = []option.ClientOption{option.WithCredentialsFile(credentialsFile)}
	}

	return cfg, nil
}

// GooglePubsubPublisher implements MessagePublisher for Google Cloud Pub/Sub.
type GooglePubsubPublisher struct {
	client *pubsub.Client
	topic  *pubsub.Topic
	logger zerolog.Logger
}

// NewGooglePubsubPublisher creates a new publisher for Google Cloud Pub/Sub.
func NewGooglePubsubPublisher(ctx context.Context, cfg GooglePubsubPublisherConfig, logger zerolog.Logger) (*GooglePubsubPublisher, error) {

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, cfg.ClientOptions...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	topic := client.Topic(cfg.TopicID)

	topic.PublishSettings.DelayThreshold = cfg.PublishSettings.DelayThreshold
	topic.PublishSettings.CountThreshold = cfg.PublishSettings.CountThreshold
	topic.PublishSettings.ByteThreshold = cfg.PublishSettings.CompressionBytesThreshold
	topic.PublishSettings.NumGoroutines = cfg.PublishSettings.NumGoroutines
	topic.PublishSettings.Timeout = cfg.PublishSettings.Timeout

	logger.Info().Str("project_id", cfg.ProjectID).Str("topic_id", cfg.TopicID).Msg("GooglePubsubPublisher initialized successfully")
	return &GooglePubsubPublisher{
		client: client,
		topic:  topic,
		logger: logger,
	}, nil
}

// Publish publishes the raw message payload to Pub/Sub asynchronously.
// The original payload is never modified.
func (p *GooglePubsubPublisher) Publish(ctx context.Context, mqttTopic string, payload []byte, attributes map[string]string) error {
	if payload == nil {
		return errors.New("cannot publish a nil payload")
	}

	// Create a new map for attributes to avoid modifying the original,
	// and add common attributes.
	pubsubAttributes := make(map[string]string)
	for k, v := range attributes {
		pubsubAttributes[k] = v
	}
	pubsubAttributes["mqtt_topic"] = mqttTopic

	result := p.topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: pubsubAttributes,
	})

	go func() {
		msgID, err := result.Get(context.Background())
		if err != nil {
			p.logger.Error().Err(err).Interface("attributes", pubsubAttributes).Msg("Failed to publish message to Pub/Sub")
			return
		}
		p.logger.Debug().Str("message_id", msgID).Str("topic", p.topic.ID()).Msg("Message published successfully to Pub/Sub")
	}()

	return nil
}

// Stop flushes pending messages and closes the Pub/Sub client.
func (p *GooglePubsubPublisher) Stop() {
	p.logger.Info().Msg("Stopping GooglePubsubPublisher...")
	if p.topic != nil {
		p.topic.Stop()
		p.logger.Info().Msg("Pub/Sub topic stopped and flushed.")
	}
	if p.client != nil {
		if err := p.client.Close(); err != nil {
			p.logger.Error().Err(err).Msg("Error closing Pub/Sub client")
		} else {
			p.logger.Info().Msg("Pub/Sub client closed.")
		}
	}
}
