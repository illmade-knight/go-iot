// loadgen/loadgen.go

package loadgen

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Device represents a single simulated device in the load test.
type Device struct {
	ID               string
	MessageRate      float64
	PayloadGenerator PayloadGenerator
}

// LoadGenerator orchestrates the load test.
type LoadGenerator struct {
	client         Client
	devices        []*Device
	logger         zerolog.Logger
	publishedCount int64
}

// NewLoadGenerator creates a new LoadGenerator.
func NewLoadGenerator(client Client, devices []*Device, logger zerolog.Logger) *LoadGenerator {
	return &LoadGenerator{
		client:  client,
		devices: devices,
		logger:  logger.With().Str("component", "LoadGenerator").Logger(),
	}
}

// ExpectedMessagesForDuration calculates the exact number of messages that will be sent
// by all devices for a given duration, based on the "publish-then-tick" logic.
func (lg *LoadGenerator) ExpectedMessagesForDuration(duration time.Duration) int {
	totalExpected := 0
	for _, device := range lg.devices {
		if device.MessageRate > 0 {
			// The number of ticks is the floor of the duration divided by the interval.
			// Total messages = 1 (for T=0) + number of subsequent ticks.
			interval := time.Duration(float64(time.Second) / device.MessageRate)
			if interval > 0 {
				numTicks := int(math.Floor(float64(duration) / float64(interval)))
				totalExpected += 1 + numTicks
			} else {
				// If rate is very high, interval could be 0. Handle gracefully.
				// This case is unlikely in realistic scenarios.
				totalExpected += 1
			}
		}
	}
	return totalExpected
}

// Run now returns the total number of successfully published messages.
func (lg *LoadGenerator) Run(ctx context.Context, duration time.Duration) (int, error) {
	atomic.StoreInt64(&lg.publishedCount, 0)
	lg.logger.Info().Int("num_devices", len(lg.devices)).Dur("duration", duration).Msg("Starting...")

	if err := lg.client.Connect(); err != nil {
		lg.logger.Error().Err(err).Msg("Failed to connect client")
		return 0, err
	}
	defer lg.client.Disconnect()

	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	for _, device := range lg.devices {
		wg.Add(1)
		go func(d *Device) {
			defer wg.Done()
			lg.runDevice(runCtx, d)
		}(device)
	}

	wg.Wait()
	finalCount := int(atomic.LoadInt64(&lg.publishedCount))
	lg.logger.Info().Int("successful_publishes", finalCount).Msg("Finished")
	return finalCount, nil
}

// runDevice runs the message publishing loop for a single device.
// It uses a "publish-then-tick" loop. This means it publishes a message
// immediately at the start of the loop (T=0) and then on every subsequent tick.
// This ensures that for a given rate R and duration D, the number of messages
// sent will be floor(R*D) + 1. For example, a rate of 1Hz for 3 seconds
// will send messages at T=0, T=1, T=2, and T=3, for a total of 4 messages.
func (lg *LoadGenerator) runDevice(ctx context.Context, device *Device) {
	if device.MessageRate <= 0 {
		lg.logger.Warn().Str("device_id", device.ID).Msg("Device has a message rate of 0, no messages will be sent")
		return
	}

	interval := time.Duration(float64(time.Second) / device.MessageRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lg.logger.Info().Str("device_id", device.ID).Float64("rate_hz", device.MessageRate).Dur("interval", interval).Msg("Device starting loop")

	for {
		// Attempt to publish a message immediately.
		if success, err := lg.client.Publish(ctx, device); err != nil {
			lg.logger.Error().Err(err).Str("device_id", device.ID).Msg("Failed to publish message")
		} else if success {
			atomic.AddInt64(&lg.publishedCount, 1)
		}

		// Then, wait for the next tick or for the context to be cancelled.
		select {
		case <-ctx.Done():
			lg.logger.Info().Str("device_id", device.ID).Msg("Device stopping")
			return
		case <-ticker.C:
			// Continue to the next iteration of the loop.
		}
	}
}
