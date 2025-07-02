// loadgen/replaygenerator.go

package loadgen

import (
	"io" // Required for io.EOF
	"sync"
)

// ReplayPayloadGenerator implements loadgen.PayloadGenerator for replaying pre-loaded messages.
// It allows the load generator to "publish" messages that have already been read from GCS.
type ReplayPayloadGenerator struct {
	messages [][]byte   // Raw JSON payloads to be replayed
	index    int        // Current index in the messages slice
	mu       sync.Mutex // Mutex to protect access to index in concurrent scenarios
}

// NewReplayPayloadGenerator creates a new generator from a slice of raw message payloads.
func NewReplayPayloadGenerator(messages [][]byte) *ReplayPayloadGenerator {
	return &ReplayPayloadGenerator{
		messages: messages,
		index:    0,
	}
}

// GeneratePayload returns the next pre-loaded payload.
// It's called by the load generator to get the message to publish.
// It returns io.EOF when no more messages are available.
func (r *ReplayPayloadGenerator) GeneratePayload(device *Device) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.index >= len(r.messages) {
		// Signal that there are no more messages to replay for this generator.
		return nil, io.EOF
	}
	payload := r.messages[r.index]
	r.index++
	return payload, nil
}
