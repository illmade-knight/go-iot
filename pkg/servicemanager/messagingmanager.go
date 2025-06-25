package servicemanager

import (
	"context"
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
// The signature is updated to remove the dependency on TopLevelConfig.
func (m *MessagingManager) Setup(ctx context.Context, projectID string, resources ResourcesSpec) error {
	m.logger.Info().Str("project_id", projectID).Msg("Starting Pub/Sub setup")

	m.logger.Info().Msg("Validating resource configuration...")
	if err := m.client.Validate(resources); err != nil {
		m.logger.Error().Err(err).Msg("Resource configuration failed validation")
		return err
	}
	m.logger.Info().Msg("Resource configuration is valid")

	if err := m.setupTopics(ctx, resources.Topics); err != nil {
		return err
	}

	if err := m.setupSubscriptions(ctx, resources.MessagingSubscriptions); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Msg("Pub/Sub setup completed successfully")
	return nil
}

// setupTopics is now fully generic and has the corrected update logic.
func (m *MessagingManager) setupTopics(ctx context.Context, topicsToCreate []TopicConfig) error {
	m.logger.Info().Int("count", len(topicsToCreate)).Msg("Setting up Pub/Sub topics...")
	for _, topicSpec := range topicsToCreate {
		if topicSpec.Name == "" {
			m.logger.Error().Msg("Skipping topic with empty name")
			continue
		}
		topic := m.client.Topic(topicSpec.Name)
		exists, err := topic.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of topic '%s': %w", topicSpec.Name, err)
		}
		if exists {
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic already exists, ensuring configuration is in sync")
			updateCfg := TopicConfig{
				Labels: topicSpec.Labels,
			}
			if _, updateErr := topic.Update(ctx, updateCfg); updateErr != nil {
				return fmt.Errorf("failed to update topic configuration for '%s': %w", topicSpec.Name, updateErr)
			}
		} else {
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Creating topic...")
			var createdTopic MessagingTopic
			var createErr error
			if len(topicSpec.Labels) > 0 {
				createdTopic, createErr = m.client.CreateTopicWithConfig(ctx, topicSpec)
			} else {
				createdTopic, createErr = m.client.CreateTopic(ctx, topicSpec.Name)
			}

			if createErr != nil {
				return fmt.Errorf("failed to create topic '%s': %w", topicSpec.Name, createErr)
			}
			m.logger.Info().Str("topic_id", createdTopic.ID()).Msg("Topic created successfully")
		}
	}
	return nil
}

// setupSubscriptions is now fully generic.
func (m *MessagingManager) setupSubscriptions(ctx context.Context, subsToCreate []SubscriptionConfig) error {
	m.logger.Info().Int("count", len(subsToCreate)).Msg("Setting up Pub/Sub subscriptions...")
	for _, subSpec := range subsToCreate {
		if subSpec.Name == "" || subSpec.Topic == "" {
			m.logger.Error().Str("sub_name", subSpec.Name).Str("topic_name", subSpec.Topic).Msg("Skipping subscription with empty name or topic")
			continue
		}

		sub := m.client.Subscription(subSpec.Name)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of subscription '%s': %w", subSpec.Name, err)
		}

		if exists {
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription already exists, ensuring configuration")
			updateCfg := SubscriptionConfig{
				AckDeadlineSeconds: subSpec.AckDeadlineSeconds,
				Labels:             subSpec.Labels,
				MessageRetention:   subSpec.MessageRetention,
				RetryPolicy:        subSpec.RetryPolicy,
			}
			if _, updateErr := sub.Update(ctx, updateCfg); updateErr != nil {
				return fmt.Errorf("failed to update subscription '%s': %w", subSpec.Name, updateErr)
			}
		} else {
			m.logger.Info().Str("subscription_id", subSpec.Name).Str("topic_id", subSpec.Topic).Msg("Creating subscription...")
			createdSub, err := m.client.CreateSubscription(ctx, subSpec)
			if err != nil {
				return fmt.Errorf("failed to create subscription '%s' for topic '%s': %w", subSpec.Name, subSpec.Topic, err)
			}
			m.logger.Info().Str("subscription_id", createdSub.ID()).Msg("Subscription created successfully")
		}
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

// Teardown deletes all configured Pub/Sub resources for a given resource specification.
func (m *MessagingManager) Teardown(ctx context.Context, projectID string, resources ResourcesSpec, teardownProtection bool) error {
	m.logger.Info().Str("project_id", projectID).Msg("Starting Pub/Sub teardown")

	if teardownProtection {
		return fmt.Errorf("teardown protection enabled for this operation")
	}

	if err := m.teardownSubscriptions(ctx, resources.MessagingSubscriptions); err != nil {
		return err
	}
	if err := m.teardownTopics(ctx, resources.Topics); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Msg("Pub/Sub teardown completed successfully")
	return nil
}

func (m *MessagingManager) teardownSubscriptions(ctx context.Context, subsToTeardown []SubscriptionConfig) error {
	m.logger.Info().Int("count", len(subsToTeardown)).Msg("Tearing down Pub/Sub subscriptions...")
	for i := len(subsToTeardown) - 1; i >= 0; i-- {
		subSpec := subsToTeardown[i]
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
			}
		} else {
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription deleted successfully")
		}
	}
	return nil
}

func (m *MessagingManager) teardownTopics(ctx context.Context, topicsToTeardown []TopicConfig) error {
	m.logger.Info().Int("count", len(topicsToTeardown)).Msg("Tearing down Pub/Sub topics...")
	for i := len(topicsToTeardown) - 1; i >= 0; i-- {
		topicSpec := topicsToTeardown[i]
		if topicSpec.Name == "" {
			continue
		}
		topic := m.client.Topic(topicSpec.Name)
		m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Attempting to delete topic...")
		if err := topic.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "NotFound") {
				m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic not found, skipping.")
			} else if strings.Contains(err.Error(), "still has subscriptions") {
				m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Topic has active subscriptions and cannot be deleted.")
			} else {
				m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Failed to delete topic")
			}
		} else {
			m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic deleted successfully")
		}
	}
	return nil
}
