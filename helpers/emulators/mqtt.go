package emulators

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	mosquitoImage = "eclipse-mosquitto:2.0"
	mosquitoPort  = "1883"
)

func GetDefaultMqttImageContainer() ImageContainer {
	return ImageContainer{
		EmulatorImage: mosquitoImage,
		EmulatorPort:  mosquitoPort,
	}
}

// SetupMosquittoContainer starts an MQTT (Mosquitto) emulator container.
// It returns an EmulatorConnectionInfo struct with connection details.
func SetupMosquittoContainer(t *testing.T, ctx context.Context, cfg ImageContainer) EmulatorConnectionInfo { // Changed return type
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "mosquitto.conf")
	err := os.WriteFile(confPath, []byte("listener 1883\nallow_anonymous true\n"), 0644)
	require.NoError(t, err)

	port := fmt.Sprintf("%s/tcp", cfg.EmulatorPort)

	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{port},
		WaitingFor:   wait.ForLog("mosquitto version 2.0").WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: confPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, "1883/tcp")
	require.NoError(t, err)
	brokerURL := fmt.Sprintf("tcp://%s:%s", host, mappedPort.Port())

	return EmulatorConnectionInfo{ // Populating the new struct
		EmulatorAddress: brokerURL,
	}
}

func CreateTestMqttPublisher(brokerURL, clientID string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().AddBroker(brokerURL).SetClientID(clientID)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test mqtt publisher connect error: %w", token.Error())
	}
	return client, nil
}
