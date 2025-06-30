// loadgen/loadgen.go

package loadgen

import (
	"context"
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
	publishedCount int64 // Counter for successful publishes.
}

// NewLoadGenerator creates a new LoadGenerator.
func NewLoadGenerator(client Client, devices []*Device, logger zerolog.Logger) *LoadGenerator {
	return &LoadGenerator{
		client:  client,
		devices: devices,
		logger:  logger,
	}
}

// Run now returns the total number of successfully published messages.
func (lg *LoadGenerator) Run(ctx context.Context, duration time.Duration) (int, error) {
	atomic.StoreInt64(&lg.publishedCount, 0) // Reset counter for each run.
	lg.logger.Info().Int("num_devices", len(lg.devices)).Dur("duration", duration).Msg("Starting load generator")

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
	lg.logger.Info().Int("successful_publishes", finalCount).Msg("Load generator finished")
	return finalCount, nil
}

// runDevice now increments the counter on successful publish.
func (lg *LoadGenerator) runDevice(ctx context.Context, device *Device) {
	if device.MessageRate <= 0 {
		lg.logger.Warn().Str("device_id", device.ID).Msg("Device has a message rate of 0, no messages will be sent")
		return
	}

	interval := time.Duration(float64(time.Second) / device.MessageRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lg.logger.Info().Str("device_id", device.ID).Float64("rate_hz", device.MessageRate).Dur("interval", interval).Msg("Device starting")

	for {
		select {
		case <-ctx.Done():
			lg.logger.Info().Str("device_id", device.ID).Msg("Device stopping")
			return
		case <-ticker.C:
			// Check the boolean result from Publish.
			if success, err := lg.client.Publish(ctx, device); err != nil {
				lg.logger.Error().Err(err).Str("device_id", device.ID).Msg("Failed to publish message")
			} else if success {
				// Only increment if the publish was successful.
				atomic.AddInt64(&lg.publishedCount, 1)
			}
		}
	}
}
