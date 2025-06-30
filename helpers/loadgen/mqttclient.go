// mqtt/client.go

package loadgen

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// MqttClient implements the Client interface for MQTT.
type MqttClient struct {
	client       mqtt.Client
	brokerURL    string
	topicPattern string
	qos          byte
	logger       zerolog.Logger
}

// NewMqttClient creates a new MQTT client.
func NewMqttClient(brokerURL, topicPattern string, qos byte, logger zerolog.Logger) Client {
	return &MqttClient{
		brokerURL:    brokerURL,
		topicPattern: topicPattern,
		qos:          qos,
		logger:       logger,
	}
}

// Connect establishes a connection to the MQTT broker.
func (c *MqttClient) Connect() error {
	opts := mqtt.NewClientOptions().
		AddBroker(c.brokerURL).
		SetClientID(fmt.Sprintf("loadgen-client-%s", uuid.New().String())).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			c.logger.Error().Err(err).Msg("MQTT Connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			c.logger.Info().Str("broker", c.brokerURL).Msg("Successfully connected to MQTT broker")
		})

	c.client = mqtt.NewClient(opts)
	if token := c.client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		c.logger.Error().Err(token.Error()).Msg("Failed to connect to MQTT broker")
		return token.Error()
	}

	if !c.client.IsConnected() {
		err := fmt.Errorf("failed to connect to %s", c.brokerURL)
		c.logger.Error().Err(err).Msg("MQTT connection check failed")
		return err
	}

	return nil
}

// Disconnect closes the connection to the MQTT broker.
func (c *MqttClient) Disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
		c.logger.Info().Msg("MQTT client disconnected")
	}
}

// Publish generates a payload and sends a message to the MQTT broker.
// It now returns true only on a successful publish acknowledgement.
func (c *MqttClient) Publish(ctx context.Context, device *Device) (bool, error) {
	// First, check if the context is already cancelled before doing any work.
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	payloadBytes, err := device.PayloadGenerator.GeneratePayload(device)
	if err != nil {
		return false, fmt.Errorf("failed to generate payload for device %s: %w", device.ID, err)
	}

	topic := strings.Replace(c.topicPattern, "+", device.ID, 1)
	token := c.client.Publish(topic, c.qos, false, payloadBytes)

	// CORRECTED: This no longer uses the main context, which was causing the race condition.
	// It now waits for a fixed, reasonable duration for the broker to acknowledge the publish.
	if token.WaitTimeout(2 * time.Second) {
		if token.Error() != nil {
			err := fmt.Errorf("mqtt publish error for device %s: %w", device.ID, token.Error())
			c.logger.Warn().Err(err).Msg("Publish failed")
			return false, err
		}
		c.logger.Debug().Str("device_id", device.ID).Str("topic", topic).Msg("Message published")
		return true, nil // Success
	}

	// This now correctly indicates a genuine timeout waiting for the broker's ACK,
	// not a premature context cancellation.
	err = fmt.Errorf("timed out waiting for publish confirmation for device %s", device.ID)
	c.logger.Error().Err(err).Str("device_id", device.ID).Msg("Publish timeout")
	return false, err
}
