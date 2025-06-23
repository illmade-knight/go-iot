package servicemanager

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadAndValidateConfig reads a YAML configuration file from the given path,
// unmarshals it into the TopLevelConfig struct, and performs basic validation.
func LoadAndValidateConfig(configPath string) (*TopLevelConfig, error) {
	// Read the YAML file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	// Unmarshal the YAML data
	var config TopLevelConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML from '%s': %w", configPath, err)
	}

	// Perform basic validation
	if config.DefaultProjectID == "" {
		fmt.Printf("Warning: default_project_id is not set in the configuration.\n")
	}

	// Check if at least one resource is defined.
	if len(config.Resources.GCSBuckets) == 0 &&
		len(config.Resources.MessagingTopics) == 0 &&
		len(config.Resources.MessagingSubscriptions) == 0 &&
		len(config.Resources.BigQueryDatasets) == 0 &&
		len(config.Resources.BigQueryTables) == 0 {
		return nil, fmt.Errorf("validation error: no resources (e.g., gcs_buckets, messaging_topics) defined in the 'resources' block")
	}

	// Validate that any defined GCS buckets have a name.
	for i, bucket := range config.Resources.GCSBuckets {
		if bucket.Name == "" {
			return nil, fmt.Errorf("validation error: gcs_buckets[%d] is missing a name", i)
		}
	}

	fmt.Printf("Configuration loaded and validated successfully from '%s'\n", configPath)
	return &config, nil
}
