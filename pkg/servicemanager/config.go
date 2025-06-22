package servicemanager

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// TopLevelConfig, EnvironmentSpec, ResourcesSpec, MessagingTopicConfig, MessagingSubscriptionConfig,
// RetryPolicySpec, BigQueryDataset, BigQueryTable, GCSBucket, LifecycleRuleSpec,
// LifecycleActionSpec, LifecycleConditionSpec structs are assumed to be defined
// in another file within this package (e.g., from the manager_config_go_structs artifact).
// If they are in the same file, these comments can be removed.

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

	// --- Modified Validation Logic ---
	// Removed mandatory checks for Pub/Sub Topics and Subscriptions.
	// Now, a config with only GCS buckets will be valid.

	// You can add a check if at least one GCS bucket is defined if it's critical
	if len(config.Resources.GCSBuckets) == 0 &&
		len(config.Resources.MessagingTopics) == 0 &&
		len(config.Resources.MessagingSubscriptions) == 0 &&
		len(config.Resources.BigQueryDatasets) == 0 &&
		len(config.Resources.BigQueryTables) == 0 {
		return nil, fmt.Errorf("validation error: no resources (GCS buckets, Pub/Sub, BigQuery) defined in resources")
	}

	// Example: If you *still* want to validate details of GCS buckets if they exist:
	for i, bucket := range config.Resources.GCSBuckets {
		if bucket.Name == "" {
			return nil, fmt.Errorf("validation error: gcs_buckets[%d] is missing a name", i)
		}
		// Add more specific GCS bucket validation rules here if needed
	}

	fmt.Printf("Configuration loaded and validated successfully from '%s'\n", configPath)
	return &config, nil
}
