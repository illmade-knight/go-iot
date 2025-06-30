package messagepipeline_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Payload ---
type processTestPayload struct {
	Data string
}

// --- Helper Functions ---

// newTestService is a helper to create a ProcessingService with standard mocks for testing.
func newTestService[T any](numWorkers, consumerBuffer, processorBuffer int) (*messagepipeline.ProcessingService[T], *MockMessageConsumer, *MockMessageProcessor[T]) {
	consumer := NewMockMessageConsumer(consumerBuffer)
	processor := NewMockMessageProcessor[T](processorBuffer)
	// A standard transformer that succeeds.
	transformer := func(msg types.ConsumedMessage) (*T, bool, error) {
		payload := any(&processTestPayload{Data: string(msg.Payload)}).(*T)
		return payload, false, nil
	}

	service, err := messagepipeline.NewProcessingService[T](numWorkers, consumer, processor, transformer, zerolog.Nop())
	if err != nil {
		panic(err) // Should not happen with valid inputs.
	}
	return service, consumer, processor
}

// --- Test Cases ---

func TestProcessingService_Lifecycle(t *testing.T) {
	service, consumer, processor := newTestService[processTestPayload](1, 10, 10)

	err := service.Start()
	require.NoError(t, err)

	assert.Equal(t, 1, consumer.GetStartCount())
	assert.Equal(t, 1, processor.startCount)

	service.Stop()

	assert.Equal(t, 1, consumer.GetStopCount())
	assert.Equal(t, 1, processor.stopCount)
}

func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	service, consumer, processor := newTestService[processTestPayload](1, 10, 10)

	err := service.Start()
	require.NoError(t, err)
	defer service.Stop()

	ackCalled, nackCalled := false, false
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{
			ID:      "test-msg-1",
			Payload: []byte("original"),
		},
		Ack:  func() { ackCalled = true },
		Nack: func() { nackCalled = true },
	}
	consumer.Push(msg)

	require.Eventually(t, func() bool {
		return len(processor.GetReceived()) > 0
	}, time.Second, 10*time.Millisecond, "Processor did not receive message in time")

	received := processor.GetReceived()
	assert.Equal(t, "original", received[0].Payload.Data)
	assert.False(t, ackCalled, "Ack should not be called by the service on success")
	assert.False(t, nackCalled, "Nack should not be called by the service on success")
}

// CORRECTED: This test now correctly injects the failing transformer at construction time.
func TestProcessingService_ProcessMessage_TransformerError(t *testing.T) {
	// Arrange
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[processTestPayload](10)
	failingTransformer := func(msg types.ConsumedMessage) (*processTestPayload, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	service, err := messagepipeline.NewProcessingService[processTestPayload](1, consumer, processor, failingTransformer, zerolog.Nop())
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	nackCalled := false
	var nackMu sync.Mutex
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-err"},
		Nack:           func() { nackMu.Lock(); nackCalled = true; nackMu.Unlock() },
	}

	// Act
	consumer.Push(msg)

	// Assert
	assert.Eventually(t, func() bool {
		nackMu.Lock()
		defer nackMu.Unlock()
		return nackCalled
	}, time.Second, 10*time.Millisecond, "Nack was not called on transformer error")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on transformer error")
}

// CORRECTED: This test is simplified and focuses on the skip logic.
func TestProcessingService_ProcessMessage_Skip(t *testing.T) {
	// Arrange
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[processTestPayload](10)
	skippingTransformer := func(msg types.ConsumedMessage) (*processTestPayload, bool, error) {
		return nil, true, nil // Signal to skip
	}

	service, err := messagepipeline.NewProcessingService[processTestPayload](1, consumer, processor, skippingTransformer, zerolog.Nop())
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	ackCalled := false
	var ackMu sync.Mutex
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-skip"},
		Ack:            func() { ackMu.Lock(); ackCalled = true; ackMu.Unlock() },
	}

	// Act
	consumer.Push(msg)

	// Assert
	assert.Eventually(t, func() bool {
		ackMu.Lock()
		defer ackMu.Unlock()
		return ackCalled
	}, time.Second, 10*time.Millisecond, "Ack was not called on skip")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on skip")
}
