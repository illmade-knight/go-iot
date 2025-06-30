// loadgen/interfaces.go

package loadgen

import (
	"context"
)

// PayloadGenerator defines the interface for generating message payloads.
type PayloadGenerator interface {
	GeneratePayload(device *Device) ([]byte, error)
}

// Client defines the interface for a client that can publish messages.
type Client interface {
	Connect() error
	Disconnect()
	// Publish now returns a boolean indicating if the publish was successful, along with an error.
	Publish(ctx context.Context, device *Device) (bool, error)
}
