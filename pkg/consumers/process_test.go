package consumers

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Payload ---
type testPayload struct {
	Data string
}

// --- Helper Functions ---

// newTestService is a helper to create a ProcessingService with standard mocks for testing.
func newTestService[T any](numWorkers int) (*ProcessingService[T], *MockMessageConsumer, *MockMessageProcessor[T]) {
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[T](10)
	// A standard transformer that succeeds.
	transformer := func(msg types.ConsumedMessage) (*T, bool, error) {
		// This is a type assertion; it will panic if T is not *testPayload.
		// The generic constraints in Go don't yet allow for a compile-time check for this.
		// For tests, this is acceptable as we control the type T.
		payload := any(&testPayload{Data: string(msg.Payload)}).(*T)
		return payload, false, nil
	}

	// We cast the specific transformer to the generic MessageTransformer type.
	service, err := NewProcessingService[T](numWorkers, consumer, processor, transformer, zerolog.Nop())
	if err != nil {
		panic(err) // Should not happen with valid inputs.
	}
	return service, consumer, processor
}

// --- Test Cases ---

// TestProcessingService_Lifecycle verifies the Start and Stop sequences are called correctly.
func TestProcessingService_Lifecycle(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1)

	// Start the service
	err := service.Start()
	require.NoError(t, err)

	// Verify components were started
	assert.Equal(t, 1, consumer.startCount)
	assert.Equal(t, 1, processor.startCount)

	// Stop the service
	service.Stop()

	// Verify components were stopped
	assert.Equal(t, 1, consumer.stopCount)
	assert.Equal(t, 1, processor.stopCount)
}

// TestProcessingService_ConsumerStartError ensures the service cleans up if the consumer fails to start.
func TestProcessingService_ConsumerStartError(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1)
	expectedErr := errors.New("consumer failed to start")
	consumer.SetStartError(expectedErr)

	// Attempt to start the service
	err := service.Start()

	// Assert that the error is propagated
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)

	// Assert that the service correctly stopped the processor as part of cleanup.
	assert.Equal(t, 1, processor.startCount, "Processor should have been started")
	assert.Equal(t, 1, processor.stopCount, "Processor should have been stopped after consumer failure")
}

// TestProcessingService_ProcessMessage_Success verifies a message is successfully
// transformed and sent to the processor.
func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1)

	err := service.Start()
	require.NoError(t, err)
	defer service.Stop()

	// Send a message to the consumer
	ackCalled := false
	nackCalled := false
	msg := types.ConsumedMessage{
		ID:      "test-msg-1",
		Payload: []byte("original"),
		Ack:     func() { ackCalled = true },
		Nack:    func() { nackCalled = true },
	}
	consumer.Push(msg)

	// Wait for the processor to receive the message.
	require.Eventually(t, func() bool {
		return len(processor.GetReceived()) > 0
	}, 1*time.Second, 10*time.Millisecond, "Processor did not receive message in time")

	received := processor.GetReceived()
	assert.Equal(t, "original", received[0].Payload.Data)
	assert.Equal(t, "test-msg-1", received[0].OriginalMessage.ID)

	// The service itself should not call Ack or Nack on a successful handoff.
	// This responsibility is delegated to the processor/batcher.
	assert.False(t, ackCalled, "Ack should not be called by the service on success")
	assert.False(t, nackCalled, "Nack should not be called by the service on success")
}

// TestProcessingService_ProcessMessage_TransformerError verifies that a Nack is called
// when the transformer returns an error.
func TestProcessingService_ProcessMessage_TransformerError(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1)
	// Override the transformer to always return an error.
	service.transformer = func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	err := service.Start()
	require.NoError(t, err)
	defer service.Stop()

	nackCalled := false
	var nackMu sync.Mutex
	msg := types.ConsumedMessage{
		ID:   "test-msg-err",
		Ack:  func() {},
		Nack: func() { nackMu.Lock(); nackCalled = true; nackMu.Unlock() },
	}
	consumer.Push(msg)

	// Wait for Nack to be called.
	assert.Eventually(t, func() bool {
		nackMu.Lock()
		defer nackMu.Unlock()
		return nackCalled
	}, 1*time.Second, 10*time.Millisecond, "Nack was not called on transformer error")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on transformer error")
}

// TestProcessingService_ProcessMessage_Skip verifies that an Ack is called
// when the transformer signals to skip a message.
func TestProcessingService_ProcessMessage_Skip(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1)
	// Override the transformer to always skip.
	service.transformer = func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, true, nil
	}

	err := service.Start()
	require.NoError(t, err)
	defer service.Stop()

	ackCalled := false
	var ackMu sync.Mutex
	msg := types.ConsumedMessage{
		ID:   "test-msg-skip",
		Ack:  func() { ackMu.Lock(); ackCalled = true; ackMu.Unlock() },
		Nack: func() {},
	}
	consumer.Push(msg)

	// Wait for Ack to be called.
	assert.Eventually(t, func() bool {
		ackMu.Lock()
		defer ackMu.Unlock()
		return ackCalled
	}, 1*time.Second, 10*time.Millisecond, "Ack was not called for skipped message")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on skip")
}

// TestProcessingService_GracefulShutdownWithInFlightMessage tests that a message
// being processed when shutdown starts is correctly Nacked.
func TestProcessingService_GracefulShutdownWithInFlightMessage(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1)

	// Use a hook to block processing of the first message until we signal it.
	processingStarted := make(chan struct{})
	unblockProcessing := make(chan struct{})
	processor.SetProcessingHook(func(msg *types.BatchedMessage[testPayload]) {
		close(processingStarted) // Signal that the processor has the message.
		<-unblockProcessing      // Wait until the test tells us to continue.
	})

	err := service.Start()
	require.NoError(t, err)
	defer close(unblockProcessing) // Ensure we don't leak the goroutine.

	nackCalled := false
	var nackMu sync.Mutex
	msg := types.ConsumedMessage{
		ID:   "in-flight-msg",
		Nack: func() { nackMu.Lock(); nackCalled = true; nackMu.Unlock() },
		Ack:  func() {},
	}

	// Push the message and wait for the worker to send it to the (blocked) processor.
	consumer.Push(msg)
	<-processingStarted

	// Now that the message is in-flight inside the processor, stop the service.
	service.Stop()

	// The message should not have been added to the "received" list yet
	// because the processor's processing goroutine is blocked.
	assert.Empty(t, processor.GetReceived())
	// Because shutdown started while the worker was blocked sending to the processor,
	// the select case in the worker should have fallen through to the shutdown case,
	// resulting in a Nack.
	// NOTE: This behavior depends on the worker's select statement.
	// The current implementation Nacks if the shutdown context is done.
	assert.Eventually(t, func() bool {
		nackMu.Lock()
		defer nackMu.Unlock()
		return nackCalled
	}, 1*time.Second, 10*time.Millisecond, "Nack was not called for in-flight message during shutdown")
}
