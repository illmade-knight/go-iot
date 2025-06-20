package servicemanager

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"time"
)

// --- Conversion Helpers ---

func fromGCPTopicConfig(t *pubsub.TopicConfig) *MessagingTopicConfig {
	return &MessagingTopicConfig{
		Name:   t.ID(),
		Labels: t.Labels,
	}
}

func fromGCPSubscriptionConfig(s *pubsub.SubscriptionConfig) *MessagingSubscriptionConfig {
	spec := &MessagingSubscriptionConfig{
		Name:               s.ID(),
		Topic:              s.Topic.ID(),
		Labels:             s.Labels,
		AckDeadlineSeconds: int(s.AckDeadline.Seconds()),
		MessageRetention:   Duration(s.RetentionDuration),
	}
	// RetryPolicy is now ignored for simplicity.
	return spec
}

// --- Adapter Implementations ---

type gcpTopicAdapter struct{ topic *pubsub.Topic }

func (a *gcpTopicAdapter) ID() string                               { return a.topic.ID() }
func (a *gcpTopicAdapter) Exists(ctx context.Context) (bool, error) { return a.topic.Exists(ctx) }
func (a *gcpTopicAdapter) Update(ctx context.Context, cfg MessagingTopicConfigToUpdate) (*MessagingTopicConfig, error) {
	gcpUpdate := pubsub.TopicConfigToUpdate{
		Labels: cfg.Labels,
	}
	updatedCfg, err := a.topic.Update(ctx, gcpUpdate)
	if err != nil {
		return nil, err
	}
	return fromGCPTopicConfig(&updatedCfg), nil
}
func (a *gcpTopicAdapter) Delete(ctx context.Context) error { return a.topic.Delete(ctx) }

type gcpSubscriptionAdapter struct{ sub *pubsub.Subscription }

func (a *gcpSubscriptionAdapter) ID() string                               { return a.sub.ID() }
func (a *gcpSubscriptionAdapter) Exists(ctx context.Context) (bool, error) { return a.sub.Exists(ctx) }
func (a *gcpSubscriptionAdapter) Update(ctx context.Context, cfg MessagingSubscriptionConfigToUpdate) (*MessagingSubscriptionConfig, error) {
	gcpUpdate := pubsub.SubscriptionConfigToUpdate{
		AckDeadline:       cfg.AckDeadline,
		Labels:            cfg.Labels,
		RetentionDuration: cfg.RetentionDuration,
	}
	if cfg.RetryPolicy != nil {
		gcpUpdate.RetryPolicy = &pubsub.RetryPolicy{
			MinimumBackoff: gcpUpdate.RetryPolicy.MinimumBackoff,
			MaximumBackoff: gcpUpdate.RetryPolicy.MaximumBackoff,
		}
	}
	updatedCfg, err := a.sub.Update(ctx, gcpUpdate)
	if err != nil {
		return nil, err
	}
	return fromGCPSubscriptionConfig(&updatedCfg), nil
}
func (a *gcpSubscriptionAdapter) Delete(ctx context.Context) error { return a.sub.Delete(ctx) }

type gcpMessagingClientAdapter struct{ client *pubsub.Client }

func (a *gcpMessagingClientAdapter) Topic(id string) MessagingTopic {
	return &gcpTopicAdapter{topic: a.client.Topic(id)}
}
func (a *gcpMessagingClientAdapter) Subscription(id string) MessagingSubscription {
	return &gcpSubscriptionAdapter{sub: a.client.Subscription(id)}
}
func (a *gcpMessagingClientAdapter) CreateTopic(ctx context.Context, topicID string) (MessagingTopic, error) {
	t, err := a.client.CreateTopic(ctx, topicID)
	if err != nil {
		return nil, err
	}
	return &gcpTopicAdapter{topic: t}, nil
}
func (a *gcpMessagingClientAdapter) CreateTopicWithConfig(ctx context.Context, topicSpec MessagingTopicConfig) (MessagingTopic, error) {
	gcpConfig := &pubsub.TopicConfig{
		Labels: topicSpec.Labels,
	}
	t, err := a.client.CreateTopicWithConfig(ctx, topicSpec.Name, gcpConfig)
	if err != nil {
		return nil, err
	}
	return &gcpTopicAdapter{topic: t}, nil
}

func (a *gcpMessagingClientAdapter) CreateSubscription(ctx context.Context, subSpec MessagingSubscriptionConfig) (MessagingSubscription, error) {
	topic := a.client.Topic(subSpec.Topic)
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for subscription's topic '%s': %w", subSpec.Topic, err)
	}
	if !topicExists {
		return nil, fmt.Errorf("cannot create subscription '%s', its topic '%s' does not exist", subSpec.Name, subSpec.Topic)
	}

	gcpConfig := pubsub.SubscriptionConfig{
		Topic:             topic,
		Labels:            subSpec.Labels,
		RetentionDuration: time.Duration(subSpec.MessageRetention),
	}
	if subSpec.AckDeadlineSeconds > 0 {
		gcpConfig.AckDeadline = time.Duration(subSpec.AckDeadlineSeconds) * time.Second
	}
	if subSpec.RetryPolicy != nil {
		gcpConfig.RetryPolicy = &pubsub.RetryPolicy{
			MinimumBackoff: subSpec.RetryPolicy.MinimumBackoff,
			MaximumBackoff: subSpec.RetryPolicy.MaximumBackoff,
		}
	}

	s, err := a.client.CreateSubscription(ctx, subSpec.Name, gcpConfig)
	if err != nil {
		return nil, err
	}
	return &gcpSubscriptionAdapter{sub: s}, nil
}
func (a *gcpMessagingClientAdapter) Close() error { return a.client.Close() }

// NewGoogleMessagingClientAdapter wraps a concrete *pubsub.Client to satisfy the MessagingClient interface.
func NewGoogleMessagingClientAdapter(client *pubsub.Client) MessagingClient {
	if client == nil {
		return nil
	}
	return &gcpMessagingClientAdapter{client: client}
}

// CreateGoogleMessagingClient creates a real Pub/Sub client for use in production.
func CreateGoogleMessagingClient(ctx context.Context, projectID string, clientOpts ...option.ClientOption) (MessagingClient, error) {
	realClient, err := pubsub.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}
	return NewGoogleMessagingClientAdapter(realClient), nil
}
