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
