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
// It is deterministic: it publishes one message immediately at T=0, and then enters
// a "wait-then-publish" loop for subsequent messages. This ensures that for a given
// rate R and duration D, the number of messages is exactly ceil(R*D).
// For example, a rate of 1Hz for 2 seconds sends messages at T=0s and T=1s
// for a total of 2 messages. A rate of 1Hz for 2.1 seconds sends messages
// at T=0s, T=1s, and T=2s for a total of 3 messages.
func (lg *LoadGenerator) runDevice(ctx context.Context, device *Device) {
	if device.MessageRate <= 0 {
		lg.logger.Warn().Str("device_id", device.ID).Msg("Device has a message rate of 0, no messages will be sent.")
		return
	}

	interval := time.Duration(float64(time.Second) / device.MessageRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lg.logger.Info().Str("device_id", device.ID).Float64("rate_hz", device.MessageRate).Dur("interval", interval).Msg("Device starting loop.")

	// 1. Publish the first message immediately for T=0, but only if the context isn't already done.
	select {
	case <-ctx.Done():
		lg.logger.Info().Str("device_id", device.ID).Msg("Context already done before first message.")
		return
	default:
		// Context is not done, so proceed with the first publish.
		if success, err := lg.client.Publish(ctx, device); err != nil {
			lg.logger.Error().Err(err).Str("device_id", device.ID).Msg("Failed to publish message.")
		} else if success {
			atomic.AddInt64(&lg.publishedCount, 1)
		}
	}

	// 2. Loop for all subsequent messages, using a "wait-then-publish" pattern.
	for {
		select {
		case <-ctx.Done():
			// The duration is up, stop waiting for more ticks.
			lg.logger.Info().Str("device_id", device.ID).Msg("Device stopping.")
			return
		case <-ticker.C:
			// A tick occurred. We are now allowed to publish another message.
			if success, err := lg.client.Publish(ctx, device); err != nil {
				lg.logger.Error().Err(err).Str("device_id", device.ID).Msg("Failed to publish message.")
			} else if success {
				atomic.AddInt64(&lg.publishedCount, 1)
			}
		}
	}
}
