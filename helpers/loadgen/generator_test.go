package loadgen_test

import (
	"context"
	"errors"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

// MockPayloadGenerator is a mock implementation of the PayloadGenerator interface.
type MockPayloadGenerator struct {
	mock.Mock
}

func (m *MockPayloadGenerator) GeneratePayload(_ *loadgen.Device) ([]byte, error) {
	args := m.Called()
	var payload []byte
	if p, ok := args.Get(0).([]byte); ok {
		payload = p
	}
	return payload, args.Error(1)
}

// MockClient is a mock implementation of the Client interface.
type MockClient struct {
	mock.Mock
}

func (m *MockClient) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClient) Disconnect() {
	m.Called()
}

// Publish mock now matches the new (bool, error) signature.
func (m *MockClient) Publish(ctx context.Context, device *loadgen.Device) (bool, error) {
	args := m.Called(ctx, device)
	return args.Bool(0), args.Error(1)
}

// --- Tests ---

func TestLoadGenerator_Run(t *testing.T) {
	logger := zerolog.Nop()

	t.Run("Successful run counts messages", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		mockGenerator := new(MockPayloadGenerator)

		devices := []*loadgen.Device{
			{ID: "device-1", MessageRate: 10, PayloadGenerator: mockGenerator},
		}
		duration := 250 * time.Millisecond

		mockClient.On("Connect").Return(nil).Once()
		mockClient.On("Disconnect").Return().Once()
		// Return true for success on publish calls.
		mockClient.On("Publish", mock.Anything, devices[0]).Return(true, nil).Maybe()

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		// The Run method now returns a count.
		count, err := lg.Run(context.Background(), duration)

		// Assert
		assert.NoError(t, err)
		// With a rate of 10Hz over 0.25s, we expect 2 messages.
		assert.Equal(t, 2, count)
		mockClient.AssertExpectations(t)
	})

	t.Run("Connect fails", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		connectErr := errors.New("connection failed")
		mockClient.On("Connect").Return(connectErr).Once()

		lg := loadgen.NewLoadGenerator(mockClient, []*loadgen.Device{}, logger)

		// Act
		count, err := lg.Run(context.Background(), 1*time.Second)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, connectErr, err)
		mockClient.AssertExpectations(t)
		mockClient.AssertNotCalled(t, "Disconnect")
	})

	t.Run("Device with zero message rate", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		mockGenerator := new(MockPayloadGenerator)
		devices := []*loadgen.Device{
			{ID: "device-1", MessageRate: 0, PayloadGenerator: mockGenerator},
		}
		duration := 100 * time.Millisecond

		mockClient.On("Connect").Return(nil).Once()
		mockClient.On("Disconnect").Return().Once()

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		count, err := lg.Run(context.Background(), duration)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		mockClient.AssertExpectations(t)
		mockClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything)
	})

	t.Run("Context cancellation stops devices", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		mockGenerator := new(MockPayloadGenerator)
		devices := []*loadgen.Device{
			{ID: "device-1", MessageRate: 100, PayloadGenerator: mockGenerator},
		}
		duration := 1 * time.Second
		cancelAfter := 50 * time.Millisecond

		var wg sync.WaitGroup
		var once sync.Once
		wg.Add(1)

		mockClient.On("Connect").Return(nil).Once()
		mockClient.On("Disconnect").Return().Once()
		mockClient.On("Publish", mock.Anything, devices[0]).Return(true, nil).Run(func(args mock.Arguments) {
			once.Do(func() {
				wg.Done()
			})
		}).Maybe()

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(cancelAfter)
			cancel()
		}()

		_, err := lg.Run(ctx, duration)

		// Assert
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}
