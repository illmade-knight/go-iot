package servicemanager

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"

	"gopkg.in/yaml.v3"
)

// ServicesDefinition is an interface that abstracts the source of the complete
// service and resource configuration for the entire system.
type ServicesDefinition interface {
	GetTopLevelConfig() (*TopLevelConfig, error)
	GetDataflow(name string) (*ResourceGroup, error)
	GetService(name string) (*ServiceSpec, error)
	GetProjectID(environment string) (string, error)
}

// --- YAML Implementation ---

// YAMLServicesDefinition implements ServicesDefinition for a local YAML file.
type YAMLServicesDefinition struct {
	filePath      string
	parsedConfig  *TopLevelConfig
	dataflowIndex map[string]ResourceGroup
	serviceIndex  map[string]ServiceSpec
}

// NewYAMLServicesDefinition creates and initializes a ServicesDefinition from a YAML file path.
func NewYAMLServicesDefinition(filePath string) (*YAMLServicesDefinition, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read services definition file '%s': %w", filePath, err)
	}

	var config TopLevelConfig
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML from '%s': %w", filePath, err)
	}

	dfIndex, svcIndex := buildIndexes(&config)

	return &YAMLServicesDefinition{
		filePath:      filePath,
		parsedConfig:  &config,
		dataflowIndex: dfIndex,
		serviceIndex:  svcIndex,
	}, nil
}

// --- In-Memory Implementation ---

// InMemoryServicesDefinition implements ServicesDefinition for an in-memory config struct.
type InMemoryServicesDefinition struct {
	parsedConfig  *TopLevelConfig
	dataflowIndex map[string]ResourceGroup
	serviceIndex  map[string]ServiceSpec
}

// NewInMemoryServicesDefinition creates and initializes a ServicesDefinition from a TopLevelConfig struct.
func NewInMemoryServicesDefinition(config *TopLevelConfig) (*InMemoryServicesDefinition, error) {
	if config == nil {
		return nil, fmt.Errorf("cannot create services definition from nil config")
	}

	dfIndex, svcIndex := buildIndexes(config)

	return &InMemoryServicesDefinition{
		parsedConfig:  config,
		dataflowIndex: dfIndex,
		serviceIndex:  svcIndex,
	}, nil
}

// --- Common Logic ---

// buildIndexes creates lookup maps from a config struct for efficient access.
func buildIndexes(config *TopLevelConfig) (map[string]ResourceGroup, map[string]ServiceSpec) {
	dfIndex := make(map[string]ResourceGroup)
	for _, df := range config.Dataflows {
		dfIndex[df.Name] = df
	}

	svcIndex := make(map[string]ServiceSpec)
	for _, svc := range config.Services {
		svcIndex[svc.Name] = svc
	}
	return dfIndex, svcIndex
}

// GetTopLevelConfig returns the entire parsed configuration struct.
func (sd *YAMLServicesDefinition) GetTopLevelConfig() (*TopLevelConfig, error) {
	return sd.parsedConfig, nil
}
func (sd *InMemoryServicesDefinition) GetTopLevelConfig() (*TopLevelConfig, error) {
	return sd.parsedConfig, nil
}

// GetDataflow finds a dataflow by name using the pre-built index.
func (sd *YAMLServicesDefinition) GetDataflow(name string) (*ResourceGroup, error) {
	df, ok := sd.dataflowIndex[name]
	if !ok {
		return nil, fmt.Errorf("dataflow '%s' not found in services definition", name)
	}
	return &df, nil
}
func (sd *InMemoryServicesDefinition) GetDataflow(name string) (*ResourceGroup, error) {
	df, ok := sd.dataflowIndex[name]
	if !ok {
		return nil, fmt.Errorf("dataflow '%s' not found in services definition", name)
	}
	return &df, nil
}

// GetService finds a service by name using the pre-built index.
func (sd *YAMLServicesDefinition) GetService(name string) (*ServiceSpec, error) {
	svc, ok := sd.serviceIndex[name]
	if !ok {
		return nil, fmt.Errorf("service '%s' not found in services definition", name)
	}
	return &svc, nil
}
func (sd *InMemoryServicesDefinition) GetService(name string) (*ServiceSpec, error) {
	svc, ok := sd.serviceIndex[name]
	if !ok {
		return nil, fmt.Errorf("service '%s' not found in services definition", name)
	}
	return &svc, nil
}

// GetProjectID determines the correct GCP Project ID for a given environment.
func (sd *YAMLServicesDefinition) GetProjectID(environment string) (string, error) {
	if sd.parsedConfig == nil {
		return "", fmt.Errorf("services definition has not been loaded")
	}
	if envSpec, ok := sd.parsedConfig.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if sd.parsedConfig.DefaultProjectID != "" {
		return sd.parsedConfig.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default is set", environment)
}
func (sd *InMemoryServicesDefinition) GetProjectID(environment string) (string, error) {
	if sd.parsedConfig == nil {
		return "", fmt.Errorf("services definition has not been loaded")
	}
	if envSpec, ok := sd.parsedConfig.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if sd.parsedConfig.DefaultProjectID != "" {
		return sd.parsedConfig.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default is set", environment)
}

// MapServiceDefinitions is a concrete implementation of ServicesDefinition
// that loads definitions from a YAML file and provides map-based lookup.
type MapServiceDefinitions struct {
	config     *TopLevelConfig
	serviceMap map[string]*ServiceSpec // Map for quick lookup of ServiceSpec by name
	logger     zerolog.Logger
}

// NewMapServiceDefinitions loads the TopLevelConfig from the specified path
// and initializes the internal service map for efficient lookups.
// This assumes that your `TopLevelConfig` has a way to enumerate or define
// the `ServiceSpec` instances. If `ServiceSpec` is directly defined under
// `services` in your YAML, you might need to add a `Services map[string]ServiceSpec`
// field to your `TopLevelConfig` in `types.go` for this to work effectively.
// For now, it will only populate the service map for services explicitly named
// in the `ServiceNames` list of the embedded `ResourceGroup` in `TopLevelConfig`.
// If `ServiceSpec` is *not* in the YAML, this `GetService` would need to
// synthesize it based on the name.
func NewMapServiceDefinitions(configPath string, logger zerolog.Logger) (*MapServiceDefinitions, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read services definition file %s: %w", configPath, err)
	}

	var cfg TopLevelConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal services definition from %s: %w", configPath, err)
	}

	serviceMap := make(map[string]*ServiceSpec)
	// IMPORTANT ASSUMPTION: This currently assumes that all service names listed
	// in cfg.ServiceNames (from the embedded ResourceGroup) can be
	// mapped to a ServiceSpec. If your YAML contains explicit ServiceSpec definitions
	// in a separate top-level map (e.g., `services: { service-name: { ... } }`),
	// you would populate `serviceMap` from that map.
	// For now, we'll create a minimal ServiceSpec just containing the name.
	for _, serviceName := range cfg.ServiceNames {
		serviceMap[serviceName] = &ServiceSpec{Name: serviceName}
	}

	return &MapServiceDefinitions{
		config:     &cfg,
		serviceMap: serviceMap,
		logger:     logger,
	}, nil
}

// GetService retrieves a ServiceSpec by its name.
// This implementation assumes the service names in the TopLevelConfig.ServiceNames
// correspond to the logical services, and for now, it synthesizes a minimal
// ServiceSpec if a named service exists in the TopLevelConfig's list.
func (m *MapServiceDefinitions) GetService(name string) (*ServiceSpec, error) {
	svc, found := m.serviceMap[name]
	if !found {
		// Check if it's in any dataflow's service names if not in top-level
		for _, dataflow := range m.config.Dataflows {
			for _, dataflowSvcName := range dataflow.ServiceNames {
				if dataflowSvcName == name {
					// Found in a dataflow, synthesize a minimal spec
					m.serviceMap[name] = &ServiceSpec{Name: name} // Cache it
					return m.serviceMap[name], nil
				}
			}
		}
		return nil, fmt.Errorf("service '%s' not found in definitions", name)
	}
	return svc, nil
}

// GetTopLevelConfig returns the complete TopLevelConfig loaded.
func (m *MapServiceDefinitions) GetTopLevelConfig() *TopLevelConfig {
	return m.config
}
