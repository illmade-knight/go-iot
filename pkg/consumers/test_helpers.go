package consumers

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/go-iot/pkg/types"
)

// ====================================================================================
// This file contains mocks for the interfaces defined in this package.
// These mocks are intended for use in unit tests of services that depend on
// the consumer pipeline.
// ====================================================================================

// --- MockMessageConsumer ---

// MockMessageConsumer is a mock implementation of the MessageConsumer interface.
// It is designed to be used in unit tests to simulate a message source.
type MockMessageConsumer struct {
	msgChan    chan types.ConsumedMessage
	doneChan   chan struct{}
	stopOnce   sync.Once
	startErr   error // Error to be returned by Start()
	startMu    sync.Mutex
	startCount int
	stopCount  int
}

// NewMockMessageConsumer creates a new mock consumer with a buffered channel.
func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		msgChan:  make(chan types.ConsumedMessage, bufferSize),
		doneChan: make(chan struct{}),
	}
}

// Messages returns the read-only channel for consuming messages.
func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage {
	return m.msgChan
}

// Start simulates the startup of a real consumer.
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	m.startCount++
	if m.startErr != nil {
		return m.startErr
	}
	go func() {
		<-ctx.Done()
		m.Stop()
	}()
	return nil
}

// Stop gracefully closes the message and done channels.
func (m *MockMessageConsumer) Stop() error {
	m.stopOnce.Do(func() {
		m.startMu.Lock()
		defer m.startMu.Unlock()
		m.stopCount++
		close(m.msgChan)
		close(m.doneChan)
	})
	return nil
}

// Done returns the channel that signals when the consumer has fully stopped.
func (m *MockMessageConsumer) Done() <-chan struct{} {
	return m.doneChan
}

// Push is a test helper to inject a message into the mock consumer's channel.
func (m *MockMessageConsumer) Push(msg types.ConsumedMessage) {
	m.msgChan <- msg
}

// SetStartError configures the mock to return an error on Start().
func (m *MockMessageConsumer) SetStartError(err error) {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	m.startErr = err
}

// --- MockMessageProcessor ---

// MockMessageProcessor is a mock implementation of the MessageProcessor interface.
type MockMessageProcessor[T any] struct {
	InputChan      chan *types.BatchedMessage[T]
	Received       []*types.BatchedMessage[T]
	mu             sync.Mutex
	wg             sync.WaitGroup
	startCount     int
	stopCount      int
	processDelay   time.Duration // To simulate a slow processor
	processingHook func(msg *types.BatchedMessage[T])
}

// NewMockMessageProcessor creates a new mock processor.
func NewMockMessageProcessor[T any](bufferSize int) *MockMessageProcessor[T] {
	return &MockMessageProcessor[T]{
		InputChan: make(chan *types.BatchedMessage[T], bufferSize),
		Received:  []*types.BatchedMessage[T]{},
	}
}

// Input returns the write-only channel for sending messages to the processor.
func (m *MockMessageProcessor[T]) Input() chan<- *types.BatchedMessage[T] {
	return m.InputChan
}

// Start begins the processor's operations.
func (m *MockMessageProcessor[T]) Start() {
	m.mu.Lock()
	m.startCount++
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for msg := range m.InputChan {
			if m.processDelay > 0 {
				time.Sleep(m.processDelay)
			}
			m.mu.Lock()
			m.Received = append(m.Received, msg)
			if m.processingHook != nil {
				// The hook must be called inside the lock to prevent race conditions
				// if it modifies shared state with the test goroutine.
				m.processingHook(msg)
			}
			m.mu.Unlock()
		}
	}()
}

// Stop gracefully shuts down the processor.
func (m *MockMessageProcessor[T]) Stop() {
	m.mu.Lock()
	m.stopCount++
	m.mu.Unlock()
	close(m.InputChan)
	m.wg.Wait() // Wait for the processing goroutine to finish.
}

// GetReceived returns a copy of the messages received by the processor.
func (m *MockMessageProcessor[T]) GetReceived() []*types.BatchedMessage[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	receivedCopy := make([]*types.BatchedMessage[T], len(m.Received))
	copy(receivedCopy, m.Received)
	return receivedCopy
}

// SetProcessDelay introduces a delay for every message processed.
func (m *MockMessageProcessor[T]) SetProcessDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processDelay = d
}

// SetProcessingHook sets a function to be called for each message processed.
func (m *MockMessageProcessor[T]) SetProcessingHook(hook func(msg *types.BatchedMessage[T])) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processingHook = hook
}
