package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"

	"golang.org/x/sync/errgroup"
)

// ProvisionedMessagingTopic holds details of a created Pub/Sub topic.
type ProvisionedMessagingTopic struct {
	Name string
}

// ProvisionedMessagingSubscription holds details of a created Pub/Sub subscription.
type ProvisionedMessagingSubscription struct {
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
	PubSubTopics        []ProvisionedMessagingTopic
	PubSubSubscriptions []ProvisionedMessagingSubscription
	GCSBuckets          []ProvisionedGCSBucket
	BigQueryDatasets    []ProvisionedBigQueryDataset
	BigQueryTables      []ProvisionedBigQueryTable
}

// ServiceManager coordinates all resource-specific operations by delegating to specialized managers.
type ServiceManager struct {
	messagingManager *MessagingManager
	storageManager   *StorageManager
	bigqueryManager  *BigQueryManager
	servicesDef      ServicesDefinition
	logger           zerolog.Logger
}

// NewServiceManager creates a new central manager, initializing all required clients and sub-managers.
func NewServiceManager(ctx context.Context, servicesDef ServicesDefinition, env string, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceManager, error) {
	projectID, err := servicesDef.GetProjectID(env)
	if err != nil {
		return nil, err
	}

	msgClient, err := CreateGoogleMessagingClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Messaging client: %w", err)
	}

	gcsClient, err := CreateGoogleGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bqClient, err := CreateGoogleBigQueryClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return NewServiceManagerFromClients(msgClient, gcsClient, bqClient, servicesDef, schemaRegistry, logger)
}

// NewServiceManagerFromClients creates a new central manager from pre-existing clients.
func NewServiceManagerFromClients(mc MessagingClient, sc StorageClient, bc BQClient, servicesDef ServicesDefinition, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceManager, error) {
	if mc == nil || sc == nil || bc == nil {
		return nil, errors.New("all managers and clients must be non-nil")
	}
	if servicesDef == nil {
		return nil, errors.New("services definition cannot be nil")
	}

	messagingManager, err := NewMessagingManager(mc, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Messaging manager: %w", err)
	}
	storageManager, err := NewStorageManager(sc, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Messaging manager: %w", err)
	}

	bigQueryManager, err := NewBigQueryManager(bc, logger, schemaRegistry)
	if err != nil {
		return nil, fmt.Errorf("failed to create Messaging manager: %w", err)
	}

	return &ServiceManager{
		messagingManager: messagingManager,
		storageManager:   storageManager,
		bigqueryManager:  bigQueryManager,
		servicesDef:      servicesDef,
		logger:           logger,
	}, nil
}

// SetupAll runs the setup process for all resource types in the provided config.
func (sm *ServiceManager) SetupAll(ctx context.Context, cfg *TopLevelConfig, environment string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment setup...")
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return sm.messagingManager.Setup(gCtx, cfg, environment)
	})

	g.Go(func() error {
		return sm.storageManager.Setup(gCtx, cfg, environment)
	})

	g.Go(func() error {
		bqManagerLogger := sm.logger.With().Str("component", "BigQuerySetup").Logger()
		return sm.setupBigQueryResources(gCtx, cfg, environment, bqManagerLogger)
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed during parallel setup: %w", err)
	}

	provResources := &ProvisionedResources{}
	for _, topic := range cfg.Resources.MessagingTopics {
		provResources.PubSubTopics = append(provResources.PubSubTopics, ProvisionedMessagingTopic{Name: topic.Name})
	}
	for _, sub := range cfg.Resources.MessagingSubscriptions {
		provResources.PubSubSubscriptions = append(provResources.PubSubSubscriptions, ProvisionedMessagingSubscription{Name: sub.Name, Topic: sub.Topic})
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
	if err := sm.teardownBigQueryResources(ctx, cfg, environment, bqManagerLogger); err != nil {
		sm.logger.Error().Err(err).Msg("Error during BigQuery teardown, continuing...")
	}

	if err := sm.storageManager.Teardown(ctx, cfg, environment); err != nil {
		sm.logger.Error().Err(err).Msg("Error during GCS teardown, continuing...")
	}

	if err := sm.messagingManager.Teardown(ctx, cfg, environment); err != nil {
		sm.logger.Error().Err(err).Msg("Error during Messaging teardown, continuing...")
	}

	sm.logger.Info().Str("environment", environment).Msg("Full environment teardown completed.")
	return nil
}

// setupBigQueryResources and teardownBigQueryResources are placeholders for future refactoring.
func (sm *ServiceManager) setupBigQueryResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger) error {
	return sm.bigqueryManager.Setup(ctx, cfg, environment)
}

func (sm *ServiceManager) teardownBigQueryResources(ctx context.Context, cfg *TopLevelConfig, environment string, logger zerolog.Logger) error {
	return sm.bigqueryManager.Teardown(ctx, cfg, environment)
}

// The Dataflow and filter methods remain the same.
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
	return sm.SetupAll(ctx, dataflowConfig, environment)
}

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
		return nil
	}

	serviceSet := make(map[string]struct{})
	for _, serviceName := range targetDataflow.Services {
		serviceSet[serviceName] = struct{}{}
	}

	dataflowConfig := sm.filterConfigForServices(fullConfig, serviceSet)
	return sm.TeardownAll(ctx, dataflowConfig, environment)
}

func (sm *ServiceManager) filterConfigForServices(fullConfig *TopLevelConfig, serviceSet map[string]struct{}) *TopLevelConfig {
	dataflowConfig := &TopLevelConfig{
		DefaultProjectID: fullConfig.DefaultProjectID,
		DefaultLocation:  fullConfig.DefaultLocation,
		Environments:     fullConfig.Environments,
		Services:         fullConfig.Services,
		Dataflows:        fullConfig.Dataflows,
		Resources:        ResourcesSpec{},
	}

	for _, topic := range fullConfig.Resources.MessagingTopics {
		if _, ok := serviceSet[topic.ProducerService]; ok {
			dataflowConfig.Resources.MessagingTopics = append(dataflowConfig.Resources.MessagingTopics, topic)
		}
	}
	for _, sub := range fullConfig.Resources.MessagingSubscriptions {
		if _, ok := serviceSet[sub.ConsumerService]; ok {
			dataflowConfig.Resources.MessagingSubscriptions = append(dataflowConfig.Resources.MessagingSubscriptions, sub)
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
