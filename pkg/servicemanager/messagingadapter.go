package servicemanager

import (
	"context"
)

// --- Pub/Sub Client Abstraction Interfaces ---

// MessagingTopic defines the interface for a single Pub/Sub topic.
// It now uses generic types for its method signatures.
type MessagingTopic interface {
	ID() string
	Exists(ctx context.Context) (bool, error)
	Update(ctx context.Context, cfg MessagingTopicConfig) (*MessagingTopicConfig, error)
	Delete(ctx context.Context) error
}

// MessagingSubscription defines the interface for a single Pub/Sub subscription.
// It now uses generic types for its method signatures.
type MessagingSubscription interface {
	ID() string
	Exists(ctx context.Context) (bool, error)
	Update(ctx context.Context, cfg MessagingSubscriptionConfig) (*MessagingSubscriptionConfig, error)
	Delete(ctx context.Context) error
}

// MessagingClient defines the fully generic interface for a Pub/Sub client.
type MessagingClient interface {
	Topic(id string) MessagingTopic
	Subscription(id string) MessagingSubscription
	CreateTopic(ctx context.Context, topicID string) (MessagingTopic, error)
	// CreateTopicWithConfig now accepts our generic MessagingTopicConfig spec.
	CreateTopicWithConfig(ctx context.Context, topicSpec MessagingTopicConfig) (MessagingTopic, error)
	// CreateSubscription already correctly uses our generic MessagingSubscriptionConfig spec.
	CreateSubscription(ctx context.Context, subSpec MessagingSubscriptionConfig) (MessagingSubscription, error)
	Close() error
	// Validate checks if the resource configuration is valid for the specific implementation.
	Validate(resources ResourcesSpec) error
	// Check if we need to be able to access this generally or only in specific adapters
	//Config(ctx context.Context) (*MessagingSubscriptionConfig, error)
}
