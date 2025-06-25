package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
)

// ProvisionedTopic holds details of a created topic.
type ProvisionedTopic struct {
	Name            string
	ProducerService string
}

// ProvisionedSubscription holds details of a created Pub/Sub subscription.
type ProvisionedSubscription struct {
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
	Topics           []ProvisionedTopic
	Subscriptions    []ProvisionedSubscription
	GCSBuckets       []ProvisionedGCSBucket
	BigQueryDatasets []ProvisionedBigQueryDataset
	BigQueryTables   []ProvisionedBigQueryTable
}

// ServiceManager coordinates all resource-specific operations by delegating to specialized managers.
type ServiceManager struct {
	messagingManager *MessagingManager
	storageManager   *StorageManager
	bigqueryManager  *BigQueryManager
	servicesDef      ServicesDefinition
	logger           zerolog.Logger
	schemaRegistry   map[string]interface{}
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
		return nil, errors.New("all clients must be non-nil")
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
		return nil, fmt.Errorf("failed to create Storage manager: %w", err)
	}

	bigqueryManager, err := NewBigQueryManager(bc, logger, schemaRegistry)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery manager: %w", err)
	}

	return &ServiceManager{
		messagingManager: messagingManager,
		storageManager:   storageManager,
		bigqueryManager:  bigqueryManager,
		servicesDef:      servicesDef,
		logger:           logger.With().Str("component", "ServiceManager").Logger(),
		schemaRegistry:   schemaRegistry,
	}, nil
}

// SetupAll runs the setup process for all dataflows defined in the configuration.
func (sm *ServiceManager) SetupAll(ctx context.Context, environment string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment setup for all dataflows...")

	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get top level config for SetupAll: %w", err)
	}

	allProvResources := &ProvisionedResources{}

	for _, dfSpec := range fullConfig.Dataflows {
		provRes, err := sm.SetupDataflow(ctx, environment, dfSpec.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to setup dataflow '%s': %w", dfSpec.Name, err)
		}
		// Append resources
		allProvResources.Topics = append(allProvResources.Topics, provRes.Topics...)
		allProvResources.Subscriptions = append(allProvResources.Subscriptions, provRes.Subscriptions...)
		allProvResources.GCSBuckets = append(allProvResources.GCSBuckets, provRes.GCSBuckets...)
		allProvResources.BigQueryDatasets = append(allProvResources.BigQueryDatasets, provRes.BigQueryDatasets...)
		allProvResources.BigQueryTables = append(allProvResources.BigQueryTables, provRes.BigQueryTables...)
	}

	sm.logger.Info().Str("environment", environment).Msg("Full environment setup completed successfully.")
	return allProvResources, nil
}

// TeardownAll runs the teardown process for all dataflows defined in the configuration.
func (sm *ServiceManager) TeardownAll(ctx context.Context, environment string) error {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment teardown for all dataflows...")

	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return fmt.Errorf("failed to get top level config for TeardownAll: %w", err)
	}

	for i := len(fullConfig.Dataflows) - 1; i >= 0; i-- {
		dfSpec := fullConfig.Dataflows[i]
		if err := sm.TeardownDataflow(ctx, environment, dfSpec.Name); err != nil {
			sm.logger.Error().Err(err).Str("dataflow", dfSpec.Name).Msg("Failed to teardown dataflow, continuing...")
		}
	}

	sm.logger.Info().Str("environment", environment).Msg("Full environment teardown completed.")
	return nil
}

// SetupDataflow creates resources for a *specific* dataflow.
func (sm *ServiceManager) SetupDataflow(ctx context.Context, environment string, dataflowName string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting setup for specific dataflow")

	dfm, err := sm.initDataflowManager(ctx, environment, dataflowName)
	if err != nil {
		return nil, err
	}

	return dfm.Setup(ctx)
}

// TeardownDataflow tears down resources for a *specific* dataflow.
func (sm *ServiceManager) TeardownDataflow(ctx context.Context, environment string, dataflowName string) error {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting teardown for specific dataflow")

	dfm, err := sm.initDataflowManager(ctx, environment, dataflowName)
	if err != nil {
		return err
	}

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

	teardownProtection := false
	if envSpec, ok := fullConfig.Environments[environment]; ok {
		teardownProtection = envSpec.TeardownProtection
	}

	return dfm.Teardown(ctx, teardownProtection)
}

// VerifyDataflow checks if all resources for a specific dataflow exist.
func (sm *ServiceManager) VerifyDataflow(ctx context.Context, environment string, dataflowName string) error {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting verification for specific dataflow")

	dfm, err := sm.initDataflowManager(ctx, environment, dataflowName)
	if err != nil {
		return err
	}

	return dfm.Verify(ctx)
}

// initDataflowManager is a helper to DRY up the dataflow manager instantiation.
// It now correctly uses the managers already held by the ServiceManager.
func (sm *ServiceManager) initDataflowManager(ctx context.Context, environment, dataflowName string) (*DataflowManager, error) {
	targetDataflow, err := sm.servicesDef.GetDataflow(dataflowName)
	if err != nil {
		return nil, fmt.Errorf("failed to get dataflow spec '%s': %w", dataflowName, err)
	}

	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get top-level config: %w", err)
	}

	projectID, err := sm.servicesDef.GetProjectID(environment)
	if err != nil {
		return nil, err
	}

	var defaultLocation string
	var defaultLabels map[string]string
	if envSpec, ok := fullConfig.Environments[environment]; ok {
		defaultLocation = envSpec.DefaultLocation
		defaultLabels = envSpec.DefaultLabels
	}
	if defaultLocation == "" {
		defaultLocation = fullConfig.DefaultLocation
	}

	// FIX: Use NewDataflowManagerFromManagers to pass down the existing, correctly configured managers.
	return NewDataflowManagerFromManagers(
		sm.messagingManager,
		sm.storageManager,
		sm.bigqueryManager,
		targetDataflow,
		projectID,
		defaultLocation,
		defaultLabels,
		environment,
		sm.logger,
	)
}
