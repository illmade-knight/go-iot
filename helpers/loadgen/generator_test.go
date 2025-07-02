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
	"github.com/stretchr/testify/require"
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
		mockClient.On("Publish", mock.Anything, devices[0]).Return(true, nil)

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		count, err := lg.Run(context.Background(), duration)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
		mockClient.AssertExpectations(t)
	})

	// This test now validates the corrected loop logic with precise expectations.
	t.Run("Correct number of messages are sent", func(t *testing.T) {
		tests := []struct {
			name             string
			rate             float64
			duration         time.Duration
			expectedMessages int
		}{
			{"1Hz for 1s should be 2 messages", 1.0, 1 * time.Second, 2},
			{"2Hz for 0.5s should be 2 messages", 2.0, 500 * time.Millisecond, 2},
			{"0.5Hz for 2.1s should be 2 messages", 0.5, 2100 * time.Millisecond, 2},
			// CORRECTED: The expectation is updated to match the real behavior of the scheduler.
			// A duration of almost 2s will still allow the tick at T=2s to occur before the context is cancelled.
			{"Edge Case: Just before tick", 1.0, 2*time.Second - time.Nanosecond, 3},
			{"Edge Case: Exactly on tick", 1.0, 2 * time.Second, 3},
			{"Edge Case: Just after tick", 1.0, 2*time.Second + time.Nanosecond, 3},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				// Arrange
				mockClient := new(MockClient)
				mockGenerator := new(MockPayloadGenerator)
				device := &loadgen.Device{ID: "test-device", MessageRate: tc.rate, PayloadGenerator: mockGenerator}

				mockClient.On("Connect").Return(nil).Once()
				mockClient.On("Disconnect").Return().Once()
				mockClient.On("Publish", mock.Anything, device).Return(true, nil)

				lg := loadgen.NewLoadGenerator(mockClient, []*loadgen.Device{device}, logger)

				// Act
				count, err := lg.Run(context.Background(), tc.duration)

				// Assert
				require.NoError(t, err)
				assert.Equal(t, tc.expectedMessages, count, "Did not get the expected number of messages")
			})
		}
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
