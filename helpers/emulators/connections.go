package emulators

import (
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Endpoint struct {
	Port     string
	Endpoint string
}

// EmulatorConnectionInfo holds all the connection details for an emulator.
type EmulatorConnectionInfo struct {
	HTTPEndpoint    Endpoint
	GRPCEndpoint    Endpoint
	EmulatorAddress string                // e.g., "tcp://localhost:1883" for MQTT
	ClientOptions   []option.ClientOption // Common Google Cloud client options
}

func getEmulatorOptions(endpoint string) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
}
