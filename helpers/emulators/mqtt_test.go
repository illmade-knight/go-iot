package emulators

import (
	"context"
	"testing"
)

func TestSetupMosquittoContainer(t *testing.T) {
	t.Parallel() // Allow tests to run in parallel

	// Removed: testCtx, testCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	// Removed: defer testCancel()

	cfg := GetDefaultMqttImageContainer()
	// Pass context.Background() to SetupMosquittoContainer for container lifecycle
	connInfo := SetupMosquittoContainer(t, context.Background(), cfg) //

	// --- Verify EmulatorConnectionInfo ---
	if connInfo.EmulatorAddress == "" {
		t.Error("EmulatorAddress is empty") //
	}

	// --- Test Connectivity ---
	clientID := "test-publisher-client"
	// CreateTestMqttPublisher handles its own timeouts internally with token.WaitTimeout
	publisher, err := CreateTestMqttPublisher(connInfo.EmulatorAddress, clientID) //
	if err != nil {
		t.Fatalf("Failed to create MQTT publisher: %v", err) //
	}
	defer publisher.Disconnect(250) // Disconnect client cleanly

	if !publisher.IsConnected() {
		t.Error("MQTT publisher is not connected") //
	}

	t.Logf("MQTT emulator test passed. Connected to Broker URL: %s", connInfo.EmulatorAddress) //
}

func TestGetDefaultMqttImageContainer(t *testing.T) {
	cfg := GetDefaultMqttImageContainer() //

	if cfg.EmulatorImage != mosquitoImage {
		t.Errorf("Expected image %q, got %q", mosquitoImage, cfg.EmulatorImage) //
	}
	if cfg.EmulatorPort != mosquitoPort {
		t.Errorf("Expected port %q, got %q", mosquitoPort, cfg.EmulatorPort) //
	}
}

func TestCreateTestMqttPublisher(t *testing.T) {
	t.Skip("Skipping TestCreateTestMqttPublisher as it relies on a running broker, tested in TestSetupMosquittoContainer") //
}
