package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// GardenMonitorMessage represents the full structure of the message as it
// arrives from the Pub/Sub topic.
type GardenMonitorMessage struct {
	Payload *GardenMonitorReadings `json:"payload"`
	// Other top-level fields from the message can be added here if needed.
}

func ConsumedMessageTransformer(msg ConsumedMessage) (*GardenMonitorReadings, bool, error) {
	var upstreamMsg GardenMonitorMessage
	if err := json.Unmarshal(msg.Payload, &upstreamMsg); err != nil {
		// This is a malformed message, return an error to Nack it.
		return nil, false, fmt.Errorf("failed to unmarshal upstream message: %w", err)
	}
	// If the inner payload is nil, we want to skip this message but still Ack it.
	if upstreamMsg.Payload == nil {
		return nil, true, nil
	}
	// Success case
	return upstreamMsg.Payload, false, nil
}

// --- Garden Monitor Specific Types ---

// GardenMonitorReadings is the data structure that will be inserted into BigQuery.
// The `bigquery` tags are used by the bqstore library to infer the table schema.
type GardenMonitorReadings struct {
	DE           string    `json:"DE" bigquery:"uid"`
	SIM          string    `json:"SIM" bigquery:"sim"`
	RSSI         string    `json:"RS" bigquery:"rssi"`
	Version      string    `json:"VR" bigquery:"version"`
	Sequence     int       `json:"SQ" bigquery:"sequence"`
	Battery      int       `json:"BA" bigquery:"battery"`
	Temperature  int       `json:"TM" bigquery:"temperature"`
	Humidity     int       `json:"HM" bigquery:"humidity"`
	SoilMoisture int       `json:"SM1" bigquery:"soil_moisture"`
	WaterFlow    int       `json:"FL1" bigquery:"water_flow"`
	WaterQuality int       `json:"WQ" bigquery:"water_quality"`
	TankLevel    int       `json:"DL1" bigquery:"tank_level"`
	AmbientLight int       `json:"AM" bigquery:"ambient_light"`
	Timestamp    time.Time `json:"timestamp" bigquery:"timestamp"`
}

// GardenMonitorExtractor is a local implementation for testing attribute extraction.
type GardenMonitorExtractor struct{}

// NewGardenMonitorExtractor creates a new extractor.
func NewGardenMonitorExtractor() *GardenMonitorExtractor {
	return &GardenMonitorExtractor{}
}

// Extract pulls the "device_eui" from the JSON payload to use as a Pub/Sub attribute.
func (e *GardenMonitorExtractor) Extract(payload []byte) (map[string]string, error) {
	var partialPayload struct {
		Payload struct {
			DE string `json:"DE"`
		} `json:"Payload"`
	}

	if err := json.Unmarshal(payload, &partialPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload for attribute extraction: %w", err)
	}

	if partialPayload.Payload.DE == "" {
		return nil, fmt.Errorf("extracted device_eui is empty")
	}

	attributes := map[string]string{
		"device_eui": partialPayload.Payload.DE,
	}

	return attributes, nil
}
