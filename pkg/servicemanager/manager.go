package servicemanager

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// ProvisionedPubSubTopic holds details of a created Pub/Sub topic.
type ProvisionedPubSubTopic struct {
	Name string
}

// ProvisionedPubSubSubscription holds details of a created Pub/Sub subscription.
type ProvisionedPubSubSubscription struct {
	Name  string
	Topic string
}

// ProvisionedGCSBucket holds details of a created GCS bucket.
type ProvisionedGCSBucket struct {
	Name string
}

// ProvisionedBigQueryDataset holds details of a created BigQuery dataset.
type ProvisionedBigQueryDataset struct {
	Name string
}

// ProvisionedBigQueryTable holds details of a created BigQuery table.
type ProvisionedBigQueryTable struct {
	Dataset string
	Name    string
}

// ProvisionedResources contains the details of all resources created by a setup operation.
type ProvisionedResources struct {
	PubSubTopics        []ProvisionedPubSubTopic
	PubSubSubscriptions []ProvisionedPubSubSubscription
	GCSBuckets          []ProvisionedGCSBucket
	BigQueryDatasets    []ProvisionedBigQueryDataset
	BigQueryTables      []ProvisionedBigQueryTable
}

// ServiceManager coordinates all resource-specific operations directly.
type ServiceManager struct {
	pubsubClient   PSClient
	gcsClient      StorageClient
	bigqueryClient BQClient
	servicesDef    ServicesDefinition
	logger         zerolog.Logger
}

// NewServiceManager creates a new central manager, initializing all required clients.
func NewServiceManager(ctx context.Context, servicesDef ServicesDefinition, env string, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceManager, error) {
	projectID, err := servicesDef.GetProjectID(env)
	if err != nil {
		return nil, err
	}

	// Initialize real GCP clients using our factory functions
	gcsClient, err := CreateGoogleGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bqClient, err := CreateGoogleBigQueryClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	psClient, err := CreateGooglePubSubClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create PubSub client: %w", err)
	}

	return &ServiceManager{
		pubsubClient:   psClient,
		gcsClient:      gcsClient,
		bigqueryClient: bqClient,
		servicesDef:    servicesDef,
		logger:         logger,
	}, nil
}

// SetupAll runs the setup process for all resource types in the provided config.
func (sm *ServiceManager) SetupAll(ctx context.Context, cfg *TopLevelConfig, environment string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment setup...")
	g, gCtx := errgroup.WithContext(ctx)

	// Pub/Sub Setup
	g.Go(func() error {
		psManagerLogger := sm.logger.With().Str("component", "PubSubSetup").Logger()
		return sm.setupPubSubResources(gCtx, cfg, environment, psManagerLogger)
	})

	// GCS Setup
	g.Go(func() error {
		gcsManagerLogger := sm.logger.With().Str("component", "GcsSetup").Logger()
		return sm.setupGCSResources(gCtx, cfg, environment, gcsManagerLogger)
	})

	// BigQuery Setup
	g.Go(func() error {
		bqManagerLogger := sm.logger.With().Str("component", "BigQuerySetup").Logger()
		return sm.setupBigQueryResources(gCtx, cfg, environment, bqManagerLogger, sm.bigqueryClient)
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed during parallel setup: %w", err)
	}

	// Construct provisioned resources summary
	provResources := &ProvisionedResources{}
	for _, topic := range cfg.Resources.PubSubTopics {
		provResources.PubSubTopics = append(provResources.PubSubTopics, ProvisionedPubSubTopic{Name: topic.Name})
	}
	for _, sub := range cfg.Resources.PubSubSubscriptions {
		provResources.PubSubSubscriptions = append(provResources.PubSubSubscriptions, ProvisionedPubSubSubscription{Name: sub.Name, Topic: sub.Topic})
	}
	for _, bucket := range cfg.Resources.GCSBuckets {
		provResources.GCSBuckets = append(provResources.GCSBuckets, ProvisionedGCSBucket{Name: bucket.Name})
	}
	for _, dataset := range cfg.Resources.BigQueryDatasets {
		provResources.BigQueryDatasets = append(provResources.BigQueryDatasets, ProvisionedBigQueryDataset{Name: dataset.Name})
	}
	for _, table := range cfg.Resources.BigQueryTables {
		provResources.BigQueryTables = append(provResources.BigQueryTables, ProvisionedBigQueryTable{Dataset: table.Dataset, Name: table.Name})
	}

	sm.logger.Info().Str("environment", environment).Msg("Full environment setup completed successfully.")
	return provResources, nil
}

// TeardownAll runs the teardown process for all resource types defined in the provided config.
func (sm *ServiceManager) TeardownAll(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment teardown...")

	bqManagerLogger := sm.logger.With().Str("component", "BigQueryTeardown").Logger()
	if err := sm.teardownBigQueryResources(ctx, cfg, environment, bqManagerLogger, sm.bigqueryClient); err != nil {
		return err
	}

	gcsManagerLogger := sm.logger.With().Str("component", "GcsTeardown").Logger()
	if err := sm.teardownGCSResources(ctx, cfg, environment, gcsManagerLogger); err != nil {
		return err
	}

	psManagerLogger := sm.logger.With().Str("component", "PubSubTeardown").Logger()
	if err := sm.teardownPubSubResources(ctx, cfg, environment, psManagerLogger); err != nil {
		return err
	}

	sm.logger.Info().Str("environment", environment).Msg("Full environment teardown completed successfully.")
	return nil
}

// setupGCSResources now uses the generic storage interfaces and types.
func (sm *ServiceManager) setupGCSResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("GCS Setup: %w", err)
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting GCS Bucket setup")

	defaultLocation := cfg.DefaultLocation
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLocation != "" {
		defaultLocation = envSpec.DefaultLocation
	}
	var defaultLabels map[string]string
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLabels != nil {
		defaultLabels = envSpec.DefaultLabels
	}

	for _, bucketCfg := range cfg.Resources.GCSBuckets {
		if bucketCfg.Name == "" {
			logger.Error().Msg("Skipping GCS bucket with empty name")
			continue
		}

		logger.Debug().Str("bucket_name", bucketCfg.Name).Msg("Processing bucket configuration")

		bucketHandle := sm.gcsClient.Bucket(bucketCfg.Name)
		existingAttrs, err := bucketHandle.Attrs(ctx)

		finalLabels := make(map[string]string)
		for k, v := range defaultLabels {
			finalLabels[k] = v
		}
		for k, v := range bucketCfg.Labels {
			finalLabels[k] = v
		}

		attrsToApply := BucketAttributes{
			Name:              bucketCfg.Name,
			StorageClass:      bucketCfg.StorageClass,
			VersioningEnabled: bucketCfg.VersioningEnabled,
			Labels:            finalLabels,
		}

		if bucketCfg.Location != "" {
			attrsToApply.Location = strings.ToUpper(bucketCfg.Location)
		} else if defaultLocation != "" {
			attrsToApply.Location = strings.ToUpper(defaultLocation)
		} else {
			logger.Warn().Str("bucket_name", bucketCfg.Name).Msg("Bucket location not specified, relying on provider defaults.")
		}

		if len(bucketCfg.LifecycleRules) > 0 {
			attrsToApply.LifecycleRules = make([]LifecycleRule, 0, len(bucketCfg.LifecycleRules))
			for _, ruleSpec := range bucketCfg.LifecycleRules {
				attrsToApply.LifecycleRules = append(attrsToApply.LifecycleRules, LifecycleRule{
					Action:    LifecycleAction{Type: ruleSpec.Action.Type},
					Condition: LifecycleCondition{AgeInDays: ruleSpec.Condition.AgeDays},
				})
			}
		}

		if isBucketNotExist(err) {
			logger.Info().Str("bucket_name", bucketCfg.Name).Str("location", attrsToApply.Location).Msg("Bucket not found, creating...")
			if errCreate := bucketHandle.Create(ctx, projectID, &attrsToApply); errCreate != nil {
				return fmt.Errorf("failed to create bucket '%s' in project '%s': %w", bucketCfg.Name, projectID, errCreate)
			}
			logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket created successfully")
		} else if err != nil {
			return fmt.Errorf("failed to get attributes for bucket '%s': %w", bucketCfg.Name, err)
		} else {
			logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket already exists. Ensuring configuration...")
			attrsToUpdate := BucketAttributesToUpdate{
				StorageClass:      &bucketCfg.StorageClass,
				VersioningEnabled: &bucketCfg.VersioningEnabled,
				Labels:            finalLabels,
			}

			if len(attrsToApply.LifecycleRules) > 0 {
				attrsToUpdate.LifecycleRules = &attrsToApply.LifecycleRules
			} else if existingAttrs.LifecycleRules != nil && len(existingAttrs.LifecycleRules) > 0 {
				attrsToUpdate.LifecycleRules = &[]LifecycleRule{}
			}

			if _, updateErr := bucketHandle.Update(ctx, attrsToUpdate); updateErr != nil {
				logger.Warn().Err(updateErr).Str("bucket_name", bucketCfg.Name).Msg("Failed to update bucket attributes")
			} else {
				logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket attributes updated/ensured.")
			}
		}
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("GCS Bucket setup completed successfully")
	return nil
}

// teardownGCSResources now uses the generic storage interfaces and types.
func (sm *ServiceManager) teardownGCSResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("GCS Teardown: %w", err)
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting GCS Bucket teardown")

	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		logger.Error().Str("environment", environment).Msg("Teardown protection is enabled for this environment for GCS. Manual intervention required or override.")
		return fmt.Errorf("teardown protection enabled for GCS in environment: %s", environment)
	}

	for i := len(cfg.Resources.GCSBuckets) - 1; i >= 0; i-- {
		bucketCfg := cfg.Resources.GCSBuckets[i]
		if bucketCfg.Name == "" {
			logger.Warn().Msg("Skipping GCS bucket with empty name during teardown")
			continue
		}

		bucketHandle := sm.gcsClient.Bucket(bucketCfg.Name)
		logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Attempting to delete bucket...")

		if _, errAttr := bucketHandle.Attrs(ctx); isBucketNotExist(errAttr) {
			logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket does not exist, skipping deletion.")
			continue
		} else if err != nil {
			logger.Error().Err(err).Str("bucket_name", bucketCfg.Name).Msg("Failed to get attributes before attempting delete, skipping.")
			continue
		}

		if errDel := bucketHandle.Delete(ctx); errDel != nil {
			if strings.Contains(errDel.Error(), "not empty") {
				logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket because it is not empty. Manual cleanup of objects required.")
			} else {
				logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket")
			}
		} else {
			logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket deleted successfully")
		}
	}

	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("GCS Bucket teardown completed")
	return nil
}

// Placeholder methods for BigQuery and PubSub management
func (sm *ServiceManager) setupBigQueryResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger, client BQClient) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("BigQuery Setup: %w", err)
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting BigQuery setup (placeholder)")
	return nil
}

func (sm *ServiceManager) teardownBigQueryResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger, client BQClient) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("BigQuery Teardown: %w", err)
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting BigQuery teardown (placeholder)")
	return nil
}

func (sm *ServiceManager) setupPubSubResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("Pub/Sub Setup: %w", err)
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub setup (placeholder)")
	return nil
}

func (sm *ServiceManager) teardownPubSubResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("Pub/Sub Teardown: %w", err)
	}
	logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub teardown (placeholder)")
	return nil
}

// SetupDataflow provisions all resources associated with a specific dataflow.
func (sm *ServiceManager) SetupDataflow(ctx context.Context, environment string, dataflowName string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting setup for specific dataflow")

	targetDataflow, err := sm.servicesDef.GetDataflow(dataflowName)
	if err != nil {
		return nil, err
	}
	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return nil, err
	}

	serviceSet := make(map[string]struct{})
	for _, serviceName := range targetDataflow.Services {
		serviceSet[serviceName] = struct{}{}
	}

	dataflowConfig := sm.filterConfigForServices(fullConfig, serviceSet)

	sm.logger.Info().
		Str("dataflow", dataflowName).
		Int("topics", len(dataflowConfig.Resources.PubSubTopics)).
		Int("subscriptions", len(dataflowConfig.Resources.PubSubSubscriptions)).
		Int("gcs_buckets", len(dataflowConfig.Resources.GCSBuckets)).
		Int("bq_tables", len(dataflowConfig.Resources.BigQueryTables)).
		Msg("Filtered resources for dataflow. Proceeding with setup.")

	return sm.SetupAll(ctx, dataflowConfig, environment)
}

// TeardownDataflow tears down all resources associated with a specific dataflow.
func (sm *ServiceManager) TeardownDataflow(ctx context.Context, environment string, dataflowName string) error {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting teardown for specific dataflow")

	targetDataflow, err := sm.servicesDef.GetDataflow(dataflowName)
	if err != nil {
		return err
	}

	if targetDataflow.Lifecycle == nil || targetDataflow.Lifecycle.Strategy != LifecycleStrategyEphemeral {
		sm.logger.Warn().
			Str("dataflow", dataflowName).
			Msg("Teardown skipped: Dataflow is not marked with an 'ephemeral' lifecycle strategy.")
		return nil
	}

	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return err
	}

	serviceSet := make(map[string]struct{})
	for _, serviceName := range targetDataflow.Services {
		serviceSet[serviceName] = struct{}{}
	}

	dataflowConfig := sm.filterConfigForServices(fullConfig, serviceSet)

	if targetDataflow.Lifecycle.KeepDatasetOnTest {
		sm.logger.Info().
			Str("dataflow", dataflowName).
			Msg("'KeepDatasetOnTest' is true. BigQuery datasets will be excluded from teardown.")
		dataflowConfig.Resources.BigQueryDatasets = nil
		dataflowConfig.Resources.BigQueryTables = nil
	}

	if targetDataflow.Lifecycle.KeepBucketOnTest {
		sm.logger.Info().
			Str("dataflow", dataflowName).
			Msg("'KeepGCSBucketOnTest' is true. GCS buckets will be excluded from teardown.")
		dataflowConfig.Resources.GCSBuckets = nil
	}

	sm.logger.Info().
		Str("dataflow", dataflowName).
		Str("lifecycle_strategy", string(targetDataflow.Lifecycle.Strategy)).
		Msg("Filtered resources for dataflow. Proceeding with teardown.")

	return sm.TeardownAll(ctx, dataflowConfig, environment)
}

// filterConfigForServices is a helper to generate a temporary config containing
// only the resources needed by a given set of services.
func (sm *ServiceManager) filterConfigForServices(fullConfig *TopLevelConfig, serviceSet map[string]struct{}) *TopLevelConfig {
	dataflowConfig := &TopLevelConfig{
		DefaultProjectID: fullConfig.DefaultProjectID,
		DefaultLocation:  fullConfig.DefaultLocation,
		Environments:     fullConfig.Environments,
		Services:         fullConfig.Services,
		Dataflows:        fullConfig.Dataflows,
		Resources:        ResourcesSpec{}, // Start with empty resources
	}

	for _, topic := range fullConfig.Resources.PubSubTopics {
		if _, ok := serviceSet[topic.ProducerService]; ok {
			dataflowConfig.Resources.PubSubTopics = append(dataflowConfig.Resources.PubSubTopics, topic)
		}
	}
	for _, sub := range fullConfig.Resources.PubSubSubscriptions {
		if _, ok := serviceSet[sub.ConsumerService]; ok {
			dataflowConfig.Resources.PubSubSubscriptions = append(dataflowConfig.Resources.PubSubSubscriptions, sub)
		}
	}
	for _, bucket := range fullConfig.Resources.GCSBuckets {
		for _, accessingService := range bucket.AccessingServices {
			if _, ok := serviceSet[accessingService]; ok {
				dataflowConfig.Resources.GCSBuckets = append(dataflowConfig.Resources.GCSBuckets, bucket)
				break
			}
		}
	}
	for _, table := range fullConfig.Resources.BigQueryTables {
		for _, accessingService := range table.AccessingServices {
			if _, ok := serviceSet[accessingService]; ok {
				dataflowConfig.Resources.BigQueryTables = append(dataflowConfig.Resources.BigQueryTables, table)
				break
			}
		}
	}

	requiredDatasets := make(map[string]struct{})
	for _, table := range dataflowConfig.Resources.BigQueryTables {
		requiredDatasets[table.Dataset] = struct{}{}
	}
	for _, dataset := range fullConfig.Resources.BigQueryDatasets {
		if _, ok := requiredDatasets[dataset.Name]; ok {
			dataflowConfig.Resources.BigQueryDatasets = append(dataflowConfig.Resources.BigQueryDatasets, dataset)
		}
	}

	return dataflowConfig
}
