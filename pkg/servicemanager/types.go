package servicemanager

import (
	"gopkg.in/yaml.v3"
	"time"
)

// This file defines the Go structs that map directly to the structure of the
// services.yaml file. The `yaml:"..."` tags are essential for the parser
// to know which YAML key corresponds to which Go struct field.

// LifecycleStrategy defines how the ServiceManager should treat a dataflow's resources.
type LifecycleStrategy string

const (
	// LifecycleStrategyPermanent indicates that resources are long-lived.
	LifecycleStrategyPermanent LifecycleStrategy = "permanent"
	// LifecycleStrategyEphemeral indicates that resources are temporary and should be torn down after use.
	LifecycleStrategyEphemeral LifecycleStrategy = "ephemeral"
)

// TopLevelConfig is the root of the configuration structure.
type TopLevelConfig struct {
	DefaultProjectID string                     `yaml:"default_project_id"`
	DefaultLocation  string                     `yaml:"default_location"`
	DefaultRegion    string                     `yaml:"default_region,omitempty"`
	Environments     map[string]EnvironmentSpec `yaml:"environments"`
	Services         []ServiceSpec              `yaml:"services"`
	Dataflows        []DataflowSpec             `yaml:"dataflows"`
	Resources        ResourcesSpec              `yaml:"resources"`
}

// EnvironmentSpec holds configuration specific to a single environment (e.g., test, production).
type EnvironmentSpec struct {
	ProjectID          string            `yaml:"project_id"`
	DefaultLocation    string            `yaml:"default_location,omitempty"`
	DefaultRegion      string            `yaml:"default_region,omitempty"`
	DefaultLabels      map[string]string `yaml:"default_labels,omitempty"`
	TeardownProtection bool              `yaml:"teardown_protection,omitempty"`
}

// ServiceSpec defines a microservice's identity within the system.
type ServiceSpec struct {
	Name           string                 `yaml:"name"`
	ServiceAccount string                 `yaml:"service_account"`
	SourcePath     string                 `yaml:"source_path"`
	MinInstances   int                    `yaml:"min_instances"`
	Metadata       map[string]interface{} `yaml:"metadata,omitempty"`
	HealthCheck    *HealthCheckSpec       `yaml:"health_check,omitempty"`
}

// HealthCheckSpec defines the health check configuration for a service.
type HealthCheckSpec struct {
	Port int    `yaml:"port"`
	Path string `yaml:"path"`
}

// DataflowSpec defines a logical grouping of services that work together.
type DataflowSpec struct {
	Name        string           `yaml:"name"`
	Description string           `yaml:"description,omitempty"`
	Services    []string         `yaml:"services"`
	Lifecycle   *LifecyclePolicy `yaml:"lifecycle,omitempty"`
}

// LifecyclePolicy defines the lifecycle management rules for a dataflow.
type LifecyclePolicy struct {
	Strategy            LifecycleStrategy `yaml:"strategy"`
	KeepResourcesOnTest bool              `yaml:"keep_resources_on_test,omitempty"`
	AutoTeardownAfter   string            `yaml:"auto_teardown_after,omitempty"`
}

// ResourcesSpec is a container for all the cloud resources defined in the system.
type ResourcesSpec struct {
	Topics                 []TopicConfig        `yaml:"topics"`
	MessagingSubscriptions []SubscriptionConfig `yaml:"subscriptions"`
	BigQueryDatasets       []BigQueryDataset    `yaml:"bigquery_datasets"`
	BigQueryTables         []BigQueryTable      `yaml:"bigquery_tables"`
	GCSBuckets             []GCSBucket          `yaml:"gcs_buckets"`
}

// TopicConfig defines the configuration for a Pub/Sub topic.
type TopicConfig struct {
	Name            string            `yaml:"name"`
	Labels          map[string]string `yaml:"labels,omitempty"`
	ProducerService string            `yaml:"producer_service,omitempty"`
}

// SubscriptionConfig defines the configuration for a Pub/Sub subscription.
type SubscriptionConfig struct {
	Name               string            `yaml:"name"`
	Topic              string            `yaml:"topic"`
	AckDeadlineSeconds int               `yaml:"ack_deadline_seconds,omitempty"`
	MessageRetention   Duration          `yaml:"message_retention_duration,omitempty"`
	RetryPolicy        *RetryPolicySpec  `yaml:"retry_policy,omitempty"`
	Labels             map[string]string `yaml:"labels,omitempty"`
	ConsumerService    string            `yaml:"consumer_service,omitempty"`
}

// RetryPolicySpec should be updated to use the new Duration type.
type RetryPolicySpec struct {
	MinimumBackoff Duration `yaml:"minimum_backoff"`
	MaximumBackoff Duration `yaml:"maximum_backoff"`
}

// BigQueryDataset defines the configuration for a BigQuery dataset.
type BigQueryDataset struct {
	Name        string            `yaml:"name"`
	Location    string            `yaml:"location,omitempty"`
	Description string            `yaml:"description,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
}

// BigQueryTable defines the configuration for a BigQuery table.
type BigQueryTable struct {
	Name                   string   `yaml:"name"`
	Dataset                string   `yaml:"dataset"`
	Description            string   `yaml:"description,omitempty"`
	SchemaSourceType       string   `yaml:"schema_source_type"`
	SchemaSourceIdentifier string   `yaml:"schema_source_identifier"`
	TimePartitioningField  string   `yaml:"time_partitioning_field,omitempty"`
	TimePartitioningType   string   `yaml:"time_partitioning_type,omitempty"`
	ClusteringFields       []string `yaml:"clustering_fields,omitempty"`
	AccessingServices      []string `yaml:"accessing_services,omitempty"`
}

// GCSBucket defines the configuration for a GCS bucket.
type GCSBucket struct {
	Name              string              `yaml:"name"`
	Location          string              `yaml:"location,omitempty"`
	StorageClass      string              `yaml:"storage_class,omitempty"`
	VersioningEnabled bool                `yaml:"versioning_enabled,omitempty"`
	LifecycleRules    []LifecycleRuleSpec `yaml:"lifecycle_rules,omitempty"`
	Labels            map[string]string   `yaml:"labels,omitempty"`
	AccessingServices []string            `yaml:"accessing_services,omitempty"`
}

// LifecycleRuleSpec defines a lifecycle rule for a GCS bucket.
type LifecycleRuleSpec struct {
	Action    LifecycleActionSpec    `yaml:"action"`
	Condition LifecycleConditionSpec `yaml:"condition"`
}

// LifecycleActionSpec defines the action to take in a lifecycle rule.
type LifecycleActionSpec struct {
	Type string `yaml:"type"`
}

// LifecycleConditionSpec defines the conditions for a lifecycle rule.
type LifecycleConditionSpec struct {
	AgeDays int `yaml:"age_days,omitempty"`
}

// Duration is a custom type that wraps time.Duration to implement yaml.Unmarshaler.
type Duration time.Duration

// UnmarshalYAML implements the yaml.Unmarshaler interface, allowing "15s" to be parsed directly to a duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}
