package device

import "errors"

// ErrMetadataNotFound is an error returned when metadata for a device cannot be found.
var ErrMetadataNotFound = errors.New("device metadata not found")

// ErrInvalidMessage is an error for when an incoming MQTT message is invalid for processing.
var ErrInvalidMessage = errors.New("invalid MQTT message for processing")

// DeviceMetadataFetcher is a function type for fetching device-specific metadata.
type DeviceMetadataFetcher func(deviceEUI string) (clientID, locationID, category string, err error)
