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

	// Check if at least one resource is defined anywhere.
	hasResources := len(config.ResourceGroup.Resources.GCSBuckets) > 0 ||
		len(config.ResourceGroup.Resources.Topics) > 0 ||
		len(config.ResourceGroup.Resources.Subscriptions) > 0 ||
		len(config.ResourceGroup.Resources.BigQueryDatasets) > 0 ||
		len(config.ResourceGroup.Resources.BigQueryTables) > 0

	if !hasResources {
		for _, df := range config.Dataflows {
			if len(df.Resources.GCSBuckets) > 0 ||
				len(df.Resources.Topics) > 0 ||
				len(df.Resources.Subscriptions) > 0 ||
				len(df.Resources.BigQueryDatasets) > 0 ||
				len(df.Resources.BigQueryTables) > 0 {
				hasResources = true
				break
			}
		}
	}

	if !hasResources {
		return nil, fmt.Errorf("validation error: no resources are defined, either globally or within any dataflow")
	}

	// Validate that any defined GCS buckets have a name, both globally and in dataflows.
	for i, bucket := range config.ResourceGroup.Resources.GCSBuckets {
		if bucket.Name == "" {
			return nil, fmt.Errorf("validation error: global gcs_buckets[%d] is missing a name", i)
		}
	}
	for i, df := range config.Dataflows {
		for j, bucket := range df.Resources.GCSBuckets {
			if bucket.Name == "" {
				return nil, fmt.Errorf("validation error: gcs_buckets[%d] in dataflow '%s' (index %d) is missing a name", j, df.Name, i)
			}
		}
	}

	fmt.Printf("Configuration loaded and validated successfully from '%s'\n", configPath)
	return &config, nil
}
