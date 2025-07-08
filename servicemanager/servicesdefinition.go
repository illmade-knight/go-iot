package servicemanager

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ServicesDefinition is an interface that abstracts the source of the complete
// service and resource configuration for the entire system.
type ServicesDefinition interface {
	GetMicroserviceArchitecture() (*MicroserviceArchitecture, error)
}

// --- YAML Implementation ---

// YAMLServicesDefinition implements ServicesDefinition for a local YAML file.
type YAMLServicesDefinition struct {
	filePath      string
	parsedConfig  *MicroserviceArchitecture
	dataflowIndex map[string]ResourceGroup
	serviceIndex  map[string]ServiceSpec
}

// NewYAMLServicesDefinition creates and initializes a ServicesDefinition from a YAML file path.
func NewYAMLServicesDefinition(filePath string) (*YAMLServicesDefinition, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read services definition file '%s': %w", filePath, err)
	}

	var config MicroserviceArchitecture
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML from '%s': %w", filePath, err)
	}

	return &YAMLServicesDefinition{
		filePath:     filePath,
		parsedConfig: &config,
	}, nil
}

// --- In-Memory Implementation ---

// InMemoryServicesDefinition implements ServicesDefinition for an in-memory config struct.
type InMemoryServicesDefinition struct {
	parsedConfig  *MicroserviceArchitecture
	dataflowIndex map[string]ResourceGroup
	serviceIndex  map[string]ServiceSpec
}

// NewInMemoryServicesDefinition creates and initializes a ServicesDefinition from a MicroserviceArchitecture struct.
func NewInMemoryServicesDefinition(config *MicroserviceArchitecture) (*InMemoryServicesDefinition, error) {
	if config == nil {
		return nil, fmt.Errorf("cannot create services definition from nil config")
	}

	return &InMemoryServicesDefinition{
		parsedConfig: config,
	}, nil
}

// --- Common Logic ---

// GetMicroserviceArchitecture returns the entire parsed configuration struct.
func (sd *YAMLServicesDefinition) GetMicroserviceArchitecture() (*MicroserviceArchitecture, error) {
	return sd.parsedConfig, nil
}
func (sd *InMemoryServicesDefinition) GetMicroserviceArchitecture() (*MicroserviceArchitecture, error) {
	return sd.parsedConfig, nil
}
