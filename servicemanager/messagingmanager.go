package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"strings"
)

// --- PubSub Manager ---

// MessagingManager handles the creation and deletion of Pub/Sub topics and subscriptions.
type MessagingManager struct {
	client MessagingClient // Use the interface for testability
	logger zerolog.Logger
}

// NewMessagingManager creates a new MessagingManager.
func NewMessagingManager(client MessagingClient, logger zerolog.Logger) (*MessagingManager, error) {
	if client == nil {
		return nil, fmt.Errorf("PubSub client (MessagingClient interface) cannot be nil")
	}
	return &MessagingManager{
		client: client,
		logger: logger.With().Str("subcomponent", "MessagingManager").Logger(),
	}, nil
}

// Setup creates all configured Pub/Sub topics and subscriptions for a given resource specification.
// The signature is updated to remove the dependency on MicroserviceArchitecture.
func (m *MessagingManager) Setup(ctx context.Context, environment Environment, resources CloudResourcesSpec) error {
	m.logger.Info().Str("project_id", environment.ProjectID).Msg("Starting Pub/Sub setup")

	m.logger.Info().Msg("Validating resource configuration...")
	if err := m.client.Validate(resources); err != nil {
		m.logger.Error().Err(err).Msg("Resource configuration failed validation")
		return err
	}
	m.logger.Info().Msg("Resource configuration is valid")

	if err := m.setupTopics(ctx, resources.Topics); err != nil {
		return err
	}

	if err := m.setupSubscriptions(ctx, resources.Subscriptions); err != nil {
		return err
	}

	m.logger.Info().Msg("Pub/Sub setup completed.")
	return nil
}

func (m *MessagingManager) setupTopics(ctx context.Context, topicsToSetup []TopicConfig) error {
	m.logger.Info().Int("count", len(topicsToSetup)).Msg("Setting up Pub/Sub topics...")
	for _, topicSpec := range topicsToSetup {
		if topicSpec.Name == "" {
			continue
		}
		topic := m.client.Topic(topicSpec.Name)
		exists, err := topic.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Failed to check if topic exists")
			return fmt.Errorf("failed to check topic %s existence: %w", topicSpec.Name, err)
		}

		if exists {
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic already exists, attempting to update.")
			_, err = topic.Update(ctx, topicSpec)
			if err != nil {
				m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Failed to update topic")
				return fmt.Errorf("failed to update topic %s: %w", topicSpec.Name, err)
			}
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic updated.")
		} else {
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic does not exist, creating.")
			_, err := m.client.CreateTopicWithConfig(ctx, topicSpec)
			if err != nil {
				m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Failed to create topic")
				return fmt.Errorf("failed to create topic %s: %w", topicSpec.Name, err)
			}
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic created.")
		}
	}
	return nil
}

func (m *MessagingManager) setupSubscriptions(ctx context.Context, subscriptionsToSetup []SubscriptionConfig) error {
	m.logger.Info().Int("count", len(subscriptionsToSetup)).Msg("Setting up Pub/Sub subscriptions...")
	for _, subSpec := range subscriptionsToSetup {
		if subSpec.Name == "" {
			continue
		}
		// Ensure the topic for the subscription exists before trying to create the subscription.
		// The client.Validate call should ideally catch missing topics, but this provides a safeguard.
		topic := m.client.Topic(subSpec.Topic)
		topicExists, err := topic.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("topic_id", subSpec.Topic).Msg("Failed to check topic existence for subscription")
			return fmt.Errorf("failed to check topic %s existence for subscription %s: %w", subSpec.Topic, subSpec.Name, err)
		}
		if !topicExists {
			m.logger.Error().Str("topic_id", subSpec.Topic).Msg("Target topic for subscription does not exist")
			return fmt.Errorf("target topic '%s' for subscription '%s' does not exist", subSpec.Topic, subSpec.Name)
		}

		subscription := m.client.Subscription(subSpec.Name)
		exists, err := subscription.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("subscription_id", subSpec.Name).Msg("Failed to check if subscription exists")
			return fmt.Errorf("failed to check subscription %s existence: %w", subSpec.Name, err)
		}

		if exists {
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription already exists, attempting to update.")
			_, err = subscription.Update(ctx, subSpec)
			if err != nil {
				m.logger.Error().Err(err).Str("subscription_id", subSpec.Name).Msg("Failed to update subscription")
				return fmt.Errorf("failed to update subscription %s: %w", subSpec.Name, err)
			}
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription updated.")
		} else {
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription does not exist, creating.")
			_, err := m.client.CreateSubscription(ctx, subSpec)
			if err != nil {
				m.logger.Error().Err(err).Str("subscription_id", subSpec.Name).Msg("Failed to create subscription")
				return fmt.Errorf("failed to create subscription %s: %w", subSpec.Name, err)
			}
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription created.")
		}
	}
	return nil
}

// Teardown deletes all configured Pub/Sub topics and subscriptions.
func (m *MessagingManager) Teardown(ctx context.Context, environment Environment, resources CloudResourcesSpec) error {
	m.logger.Info().Str("project_id", environment.ProjectID).Msg("Starting Pub/Sub teardown")

	var allErrors []error // Slice to collect all errors

	// Teardown subscriptions first
	if err := m.teardownSubscriptions(ctx, resources.Subscriptions); err != nil {
		m.logger.Error().Err(err).Msg("Failed to tear down subscriptions")
		allErrors = append(allErrors, err) // Collect error
	}

	// Teardown topics
	if err := m.teardownTopics(ctx, resources.Topics); err != nil {
		m.logger.Error().Err(err).Msg("Failed to tear down topics")
		allErrors = append(allErrors, err) // Collect error
	}

	if len(allErrors) > 0 {
		// Join all collected errors into a single error string for the top-level return.
		var errorMessages []string
		for _, err := range allErrors {
			errorMessages = append(errorMessages, err.Error())
		}
		return errors.New(strings.Join(errorMessages, "; "))
	}

	m.logger.Info().Msg("Pub/Sub teardown completed.")
	return nil
}

func (m *MessagingManager) teardownSubscriptions(ctx context.Context, subscriptionsToTeardown []SubscriptionConfig) error {
	m.logger.Info().Int("count", len(subscriptionsToTeardown)).Msg("Tearing down Pub/Sub subscriptions...")
	var errorMessages []string
	for i := len(subscriptionsToTeardown) - 1; i >= 0; i-- {
		subSpec := subscriptionsToTeardown[i]
		if subSpec.TeardownProtection {
			m.logger.Warn().Str("name", subSpec.Name).Msg("teardown protection in place")
			continue
		}
		if subSpec.Name == "" {
			continue
		}
		sub := m.client.Subscription(subSpec.Name)
		m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Attempting to delete subscription...")
		if err := sub.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "NotFound") {
				m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription not found, skipping.")
			} else {
				m.logger.Error().Err(err).Str("subscription_id", subSpec.Name).Msg("Failed to delete subscription")
				errorMessages = append(errorMessages, fmt.Sprintf("failed to delete subscription %s: %v", subSpec.Name, err))
			}
		} else {
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription deleted successfully")
		}
	}
	if len(errorMessages) > 0 {
		return errors.New(strings.Join(errorMessages, "; "))
	}
	return nil
}

func (m *MessagingManager) teardownTopics(ctx context.Context, topicsToTeardown []TopicConfig) error {
	m.logger.Info().Int("count", len(topicsToTeardown)).Msg("Tearing down Pub/Sub topics...")
	var errorMessages []string
	for i := len(topicsToTeardown) - 1; i >= 0; i-- {
		topicSpec := topicsToTeardown[i]
		if topicSpec.TeardownProtection {
			m.logger.Warn().Str("name", topicSpec.Name).Msg("teardown protection in place")
			continue
		}
		if topicSpec.Name == "" {
			continue
		}
		topic := m.client.Topic(topicSpec.Name)
		m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Attempting to delete topic...")
		if err := topic.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "NotFound") {
				m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic not found, skipping.")
			} else if strings.Contains(err.Error(), "still has subscriptions") {
				m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Topic still has subscriptions.")
				errorMessages = append(errorMessages, fmt.Sprintf("topic %s still has subscriptions: %v", topicSpec.Name, err))
			} else {
				m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Failed to delete topic")
				errorMessages = append(errorMessages, fmt.Sprintf("failed to delete topic %s: %v", topicSpec.Name, err))
			}
		} else {
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic deleted successfully")
		}
	}
	if len(errorMessages) > 0 {
		return errors.New(strings.Join(errorMessages, "; "))
	}
	return nil
}

// VerifyTopics checks if the specified Pub/Sub topics exist and have compatible configurations.
func (m *MessagingManager) VerifyTopics(ctx context.Context, topicsToVerify []TopicConfig) error {
	m.logger.Info().Int("count", len(topicsToVerify)).Msg("Verifying Pub/Sub topics...")
	for _, topicSpec := range topicsToVerify {
		if topicSpec.Name == "" {
			m.logger.Warn().Msg("Skipping verification for topic with empty name")
			continue
		}
		topic := m.client.Topic(topicSpec.Name)
		exists, err := topic.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of topic '%s' during verification: %w", topicSpec.Name, err)
		}
		if !exists {
			return fmt.Errorf("topic '%s' not found during verification", topicSpec.Name)
		}
		m.logger.Debug().Str("topic_id", topicSpec.Name).Msg("Topic verified successfully (existence only).")
	}
	return nil
}

// VerifySubscriptions checks if the specified Pub/Sub subscriptions exist.
func (m *MessagingManager) VerifySubscriptions(ctx context.Context, subsToVerify []SubscriptionConfig) error {
	m.logger.Info().Int("count", len(subsToVerify)).Msg("Verifying Pub/Sub subscriptions...")
	for _, subSpec := range subsToVerify {
		if subSpec.Name == "" || subSpec.Topic == "" {
			m.logger.Warn().Str("sub_name", subSpec.Name).Str("topic_name", subSpec.Topic).Msg("Skipping verification for subscription with empty name or topic")
			continue
		}

		sub := m.client.Subscription(subSpec.Name)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of subscription '%s' during verification: %w", subSpec.Name, err)
		}
		if !exists {
			return fmt.Errorf("subscription '%s' not found during verification", subSpec.Name)
		}
		m.logger.Debug().Str("subscription_id", subSpec.Name).Msg("Subscription verified successfully (existence only).")
	}
	return nil
}
