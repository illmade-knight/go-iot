package messagepipeline

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
func newTestService[T any](numWorkers, consumerBuffer, processorBuffer int) (*ProcessingService[T], *MockMessageConsumer, *MockMessageProcessor[T]) {
	consumer := NewMockMessageConsumer(consumerBuffer)
	processor := NewMockMessageProcessor[T](processorBuffer)
	// A standard transformer that succeeds.
	transformer := func(msg types.ConsumedMessage) (*T, bool, error) {
		payload := any(&testPayload{Data: string(msg.Payload)}).(*T)
		return payload, false, nil
	}

	service, err := NewProcessingService[T](numWorkers, consumer, processor, transformer, zerolog.Nop())
	if err != nil {
		panic(err) // Should not happen with valid inputs.
	}
	return service, consumer, processor
}

// --- Test Cases ---

// TestProcessingService_Lifecycle verifies the Start and Stop sequences are called correctly.
func TestProcessingService_Lifecycle(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1, 10, 10)

	err := service.Start()
	require.NoError(t, err)

	assert.Equal(t, 1, consumer.GetStartCount())
	assert.Equal(t, 1, processor.startCount)

	service.Stop()

	assert.Equal(t, 1, consumer.GetStopCount())
	assert.Equal(t, 1, processor.stopCount)
}

// TestProcessingService_ConsumerStartError ensures the service cleans up if the consumer fails to start.
func TestProcessingService_ConsumerStartError(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1, 10, 10)
	expectedErr := errors.New("consumer failed to start")
	consumer.SetStartError(expectedErr)

	err := service.Start()

	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 1, processor.startCount, "Processor should have been started")
	assert.Equal(t, 1, processor.stopCount, "Processor should have been stopped after consumer failure")
}

// TestProcessingService_ProcessMessage_Success verifies a message is successfully
// transformed and sent to the processor.
func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1, 10, 10)

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

// TestProcessingService_ProcessMessage_TransformerError verifies that a Nack is called
// when the transformer returns an error.
func TestProcessingService_ProcessMessage_TransformerError(t *testing.T) {
	service, consumer, processor := newTestService[testPayload](1, 10, 10)
	service.transformer = func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	err := service.Start()
	require.NoError(t, err)
	defer service.Stop()

	nackCalled := false
	var nackMu sync.Mutex
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{
			ID: "test-msg-err",
		},
		Nack: func() { nackMu.Lock(); nackCalled = true; nackMu.Unlock() }}
	consumer.Push(msg)

	assert.Eventually(t, func() bool {
		nackMu.Lock()
		defer nackMu.Unlock()
		return nackCalled
	}, time.Second, 10*time.Millisecond, "Nack was not called on transformer error")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on transformer error")
}

// TestProcessingService_GracefulShutdownScenarios tests various conditions during graceful shutdown.
func TestProcessingService_GracefulShutdownScenarios(t *testing.T) {
	tests := []struct {
		name                      string
		consumerBufferSize        int
		processorBufferSize       int
		messagesToPush            []*messageState
		setupHook                 func(p *MockMessageProcessor[testPayload], s *ProcessingService[testPayload]) (unblock chan struct{}, blockSignal chan struct{})
		expectedProcessorReceived int
		expectedAcks              map[string]bool
		expectedNacks             map[string]bool
	}{
		{
			name:                "All messages processed and acknowledged before shutdown",
			consumerBufferSize:  10,
			processorBufferSize: 10,
			messagesToPush:      []*messageState{{ID: "msg1"}, {ID: "msg2"}},
			setupHook: func(p *MockMessageProcessor[testPayload], s *ProcessingService[testPayload]) (chan struct{}, chan struct{}) {
				p.SetAckOnProcess(true) // Ensure processor acks messages.
				return nil, nil
			},
			expectedProcessorReceived: 2,
			expectedAcks:              map[string]bool{"msg1": true, "msg2": true},
			expectedNacks:             map[string]bool{},
		},
		{
			name:                "In-flight message is Nacked when worker blocks on unresponsive processor",
			consumerBufferSize:  10,
			processorBufferSize: 0, // Unbuffered channel makes sends block until received.
			messagesToPush:      []*messageState{{ID: "processed-msg"}, {ID: "in-flight-msg"}},
			setupHook: func(p *MockMessageProcessor[testPayload], s *ProcessingService[testPayload]) (unblock chan struct{}, blockSignal chan struct{}) {
				unblock = make(chan struct{})
				p.SetProcessingHook(func(msg *types.BatchedMessage[testPayload]) {
					if msg.OriginalMessage.ID == "processed-msg" {
						<-unblock
					}
				})
				return unblock, nil
			},
			expectedProcessorReceived: 1,
			expectedAcks:              map[string]bool{},
			expectedNacks:             map[string]bool{"in-flight-msg": true, "processed-msg": false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service, consumer, processor := newTestService[testPayload](1, tc.consumerBufferSize, tc.processorBufferSize)

			messageStates := make(map[string]*messageState)
			for _, msg := range tc.messagesToPush {
				messageStates[msg.ID] = msg
			}

			var unblockCh, blockSignalCh chan struct{}
			if tc.setupHook != nil {
				unblockCh, blockSignalCh = tc.setupHook(processor, service)
			}

			require.NoError(t, service.Start())

			for i, msgState := range tc.messagesToPush {
				msg := types.ConsumedMessage{
					PublishMessage: types.PublishMessage{ID: msgState.ID, Payload: []byte(msgState.ID)}, Ack: msgState.Ack, Nack: msgState.Nack}
				if i == 1 && blockSignalCh != nil {
					<-blockSignalCh
				}
				consumer.Push(msg)
			}

			time.Sleep(50 * time.Millisecond)

			// FIX: This is the correct, deterministic teardown logic.
			stopDone := make(chan struct{})
			go func() {
				defer close(stopDone)
				service.Stop()
			}()

			// For the specific blocking test, we now deterministically wait for the Nack.
			if tc.name == "In-flight message is Nacked when worker blocks on unresponsive processor" {
				require.Eventually(t, func() bool {
					return messageStates["in-flight-msg"].IsNacked()
				}, time.Second, 10*time.Millisecond, "in-flight-msg was not Nacked on shutdown")
			}

			// Now that the critical assertion has passed, unblock the processor to allow a clean shutdown.
			if unblockCh != nil {
				close(unblockCh)
			}

			// Wait for the full shutdown to complete.
			select {
			case <-stopDone:
				// success
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for service shutdown")
			}

			finalReceived := processor.GetReceived()
			var receivedIDs []string
			for _, r := range finalReceived {
				receivedIDs = append(receivedIDs, r.OriginalMessage.ID)
			}
			assert.Len(t, finalReceived, tc.expectedProcessorReceived, "Incorrect number of messages received by processor for test '%s'. Expected %d, Got %d. Received IDs: %v", tc.name, tc.expectedProcessorReceived, len(finalReceived), receivedIDs)

			for _, msg := range tc.messagesToPush {
				assert.Equal(t, tc.expectedAcks[msg.ID], msg.IsAcked(), "Message %s: incorrect Ack status", msg.ID)
				assert.Equal(t, tc.expectedNacks[msg.ID], msg.IsNacked(), "Message %s: incorrect Nack status", msg.ID)
			}
		})
	}
}
