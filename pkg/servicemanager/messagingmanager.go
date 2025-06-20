package servicemanager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
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
		logger: logger.With().Str("component", "MessagingManager").Logger(),
	}, nil
}

// GetTargetProjectID determines the project ID to use based on the environment.
func GetTargetProjectID(cfg *TopLevelConfig, environment string) (string, error) {
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if cfg.DefaultProjectID != "" {
		return cfg.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default_project_id set", environment)
}

// Setup creates all configured Pub/Sub topics and subscriptions for a given environment.
func (m *MessagingManager) Setup(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return err
	}
	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub setup")

	if err := m.setupTopics(ctx, cfg.Resources.PubSubTopics); err != nil {
		return err
	}

	if err := m.setupSubscriptions(ctx, cfg.Resources.PubSubSubscriptions); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Pub/Sub setup completed successfully")
	return nil
}

// setupTopics is now fully generic and has the corrected update logic.
func (m *MessagingManager) setupTopics(ctx context.Context, topicsToCreate []MessagingTopicConfig) error {
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
			// CORRECTED LOGIC: Always attempt to sync the labels to match the config.
			updateCfg := MessagingTopicConfigToUpdate{
				Labels: topicSpec.Labels,
			}
			if _, updateErr := topic.Update(ctx, updateCfg); updateErr != nil {
				// Note: GCP returns an error if you try to "update" with the exact same labels.
				// This is generally safe to ignore, but real production code might inspect the error more closely.
				m.logger.Warn().Err(updateErr).Str("topic_id", topicSpec.Name).Msg("Failed to update topic configuration")
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
func (m *MessagingManager) setupSubscriptions(ctx context.Context, subsToCreate []MessagingSubscriptionConfig) error {
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
			updateCfg := MessagingSubscriptionConfigToUpdate{
				AckDeadline:       time.Duration(subSpec.AckDeadlineSeconds) * time.Second,
				Labels:            subSpec.Labels,
				RetentionDuration: time.Duration(subSpec.MessageRetention),
				RetryPolicy:       subSpec.RetryPolicy,
			}
			if _, updateErr := sub.Update(ctx, updateCfg); updateErr != nil {
				m.logger.Warn().Err(updateErr).Str("subscription_id", subSpec.Name).Msg("Failed to update subscription")
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

// Teardown deletes all configured Pub/Sub resources for a given environment.
func (m *MessagingManager) Teardown(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return err
	}
	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub teardown")

	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		return fmt.Errorf("teardown protection enabled for environment: %s", environment)
	}

	if err := m.teardownSubscriptions(ctx, cfg.Resources.PubSubSubscriptions); err != nil {
		return err
	}
	if err := m.teardownTopics(ctx, cfg.Resources.PubSubTopics); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Pub/Sub teardown completed successfully")
	return nil
}

func (m *MessagingManager) teardownSubscriptions(ctx context.Context, subsToTeardown []MessagingSubscriptionConfig) error {
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

func (m *MessagingManager) teardownTopics(ctx context.Context, topicsToTeardown []MessagingTopicConfig) error {
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
