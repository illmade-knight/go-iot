//go:build integration

package loadgen_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NEW TEST to verify the payload format regression.
func TestMqttClient_Publish_PayloadFormat(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Start an MQTT emulator.
	mqttConnInfo := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())

	// 2. Create the client under test (our MqttClient).
	topicPattern := "test/+/data"
	publisher := loadgen.NewMqttClient(mqttConnInfo.EmulatorAddress, topicPattern, 1, logger)
	err := publisher.Connect()
	require.NoError(t, err)
	defer publisher.Disconnect()

	// 3. Create a standard Paho client to subscribe and receive the message.
	messageCh := make(chan []byte, 1)
	opts := mqtt.NewClientOptions().AddBroker(mqttConnInfo.EmulatorAddress).SetClientID("test-subscriber")
	subscriber := mqtt.NewClient(opts)
	token := subscriber.Connect()
	require.True(t, token.WaitTimeout(5*time.Second), "subscriber failed to connect")
	require.NoError(t, token.Error())
	defer subscriber.Disconnect(250)

	// Subscribe to the specific topic we expect the publisher to use.
	topicToReceive := "test/device-123/data"
	token = subscriber.Subscribe(topicToReceive, 1, func(client mqtt.Client, msg mqtt.Message) {
		messageCh <- msg.Payload()
	})
	require.True(t, token.WaitTimeout(5*time.Second), "subscriber failed to subscribe")
	require.NoError(t, token.Error())

	// 4. Setup mock payload generator to return a known payload.
	mockGenerator := new(MockPayloadGenerator)
	expectedPayload, _ := json.Marshal(map[string]string{"key": "value", "id": "device-123"})
	mockGenerator.On("GeneratePayload").Return(expectedPayload, nil)

	device := &loadgen.Device{
		ID:               "device-123",
		PayloadGenerator: mockGenerator,
	}

	// Act
	_, err = publisher.Publish(ctx, device)
	require.NoError(t, err)

	// Assert
	// Wait for the message to be received by the subscriber, with a timeout.
	select {
	case receivedPayload := <-messageCh:
		// The core of the test: assert that the received payload is exactly what the generator produced.
		assert.Equal(t, string(expectedPayload), string(receivedPayload), "The published payload should not be wrapped")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message to be received")
	}
}
