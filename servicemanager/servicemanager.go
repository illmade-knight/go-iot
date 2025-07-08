package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	environment              Environment
	microserviceArchitecture *MicroserviceArchitecture
	messagingManager         *MessagingManager
	storageManager           *StorageManager
	bigqueryManager          *BigQueryManager
	logger                   zerolog.Logger
	schemaRegistry           map[string]interface{}
}

// NewServiceManager creates a new central manager, initializing all required clients and sub-managers.
func NewServiceManager(ctx context.Context, architecture *MicroserviceArchitecture, env string, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceManager, error) {

	msgClient, err := CreateGoogleMessagingClient(ctx, architecture.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Messaging client: %w", err)
	}

	gcsClient, err := CreateGoogleGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bqClient, err := CreateGoogleBigQueryClient(ctx, architecture.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return NewServiceManagerFromClients(msgClient, gcsClient, bqClient, architecture, schemaRegistry, logger)
}

// NewServiceManagerFromClients creates a new central manager from pre-existing clients.
func NewServiceManagerFromClients(mc MessagingClient, sc StorageClient, bc BQClient, microserviceArchitecture *MicroserviceArchitecture, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceManager, error) {
	environment := microserviceArchitecture.Environment

	messagingManager, err := NewMessagingManager(mc, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Messaging manager: %w", err)
	}
	storageManager, err := NewStorageManager(sc, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage manager: %w", err)
	}

	bigqueryManager, err := NewBigQueryManager(bc, logger, schemaRegistry, environment)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery manager: %w", err)
	}

	return &ServiceManager{
		messagingManager:         messagingManager,
		storageManager:           storageManager,
		bigqueryManager:          bigqueryManager,
		environment:              environment,
		microserviceArchitecture: microserviceArchitecture,
		logger:                   logger.With().Str("component", "ServiceManagerResources").Logger(),
		schemaRegistry:           schemaRegistry,
	}, nil
}

// SetupAll runs the setup process for all dataflows defined in the configuration.
// we return all the provisioned resources
func (sm *ServiceManager) SetupAll(ctx context.Context, environment string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment setup for all dataflows...")

	if sm.microserviceArchitecture.DeploymentEnvironments != nil {
		if ms, ok := sm.microserviceArchitecture.DeploymentEnvironments[environment]; ok {
			sm.environment = ms
		} else {

			sm.logger.Info().Str("deployment", environment).Msg("no deployment found, using base deployment")
		}
	} else {
		sm.logger.Info().Msg("no deployments, using base deployment")
	}

	allProvResources := &ProvisionedResources{}

	for _, dfSpec := range sm.microserviceArchitecture.Dataflows {
		provRes, err := sm.SetupDataflow(ctx, sm.environment, dfSpec.Name)
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
// It now collects errors from each dataflow teardown and returns them as a single aggregated error.
func (sm *ServiceManager) TeardownAll(ctx context.Context) error {
	sm.logger.Info().Str("environment", sm.environment.Name).Msg("Starting full environment teardown for all dataflows...")

	var errorMessages []string

	for _, dataflow := range sm.microserviceArchitecture.Dataflows {
		if err := sm.TeardownDataflow(ctx, dataflow.Name); err != nil {
			errorMessage := fmt.Sprintf("failed to teardown dataflow '%s': %v", dataflow.Name, err)
			sm.logger.Error().Err(err).Str("dataflow", dataflow.Name).Msg("Teardown error occurred, continuing...")
			errorMessages = append(errorMessages, errorMessage)
		}
	}

	sm.logger.Info().Str("environment", sm.microserviceArchitecture.Environment.Name).Msg("Full environment teardown completed.")

	// If any errors were collected, return them as a single error.
	if len(errorMessages) > 0 {
		return errors.New(strings.Join(errorMessages, "; "))
	}

	return nil
}

// SetupDataflow creates resources for a *specific* dataflow.
func (sm *ServiceManager) SetupDataflow(ctx context.Context, environment Environment, dataflowName string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment.Name).Msg("Starting setup for specific dataflow")

	dfm, err := sm.initDataflowManager()
	if err != nil {
		return nil, err
	}

	targetDataflow, ok := sm.microserviceArchitecture.Dataflows[dataflowName]
	if !ok {
		return nil, fmt.Errorf("failed to get dataflow spec '%s'", dataflowName)
	}

	return dfm.SetupResources(ctx, &targetDataflow)
}

// TeardownDataflow tears down resources for a *specific* dataflow.
func (sm *ServiceManager) TeardownDataflow(ctx context.Context, dataflowName string) error {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", sm.microserviceArchitecture.Environment.Name).Msg("Starting teardown for specific dataflow")

	dfm, err := sm.initDataflowManager()
	if err != nil {
		return err
	}

	targetResourceGroup := sm.microserviceArchitecture.Dataflows[dataflowName]

	if targetResourceGroup.Lifecycle == nil || targetResourceGroup.Lifecycle.Strategy != LifecycleStrategyEphemeral {
		sm.logger.Warn().
			Str("dataflow", dataflowName).
			Msg("Teardown skipped: Dataflow is not marked with an 'ephemeral' lifecycle strategy.")
		return nil
	}

	return dfm.TeardownResources(ctx, &targetResourceGroup)
}

// VerifyDataflow checks if all resources for a specific dataflow exist.
func (sm *ServiceManager) VerifyDataflow(ctx context.Context, dataflowName string) error {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", sm.environment.Name).Msg("Starting verification for specific dataflow")

	dfm, err := sm.initDataflowManager()
	if err != nil {
		return err
	}

	targetResourceGroup := sm.microserviceArchitecture.Dataflows[dataflowName]

	return dfm.Verify(ctx, &targetResourceGroup)
}

// initDataflowManager is a helper to DRY up the dataflow manager instantiation.
// It now correctly uses the managers already held by the ServiceManager.
func (sm *ServiceManager) initDataflowManager() (*DataflowManager, error) {

	return NewDataflowManagerFromManagers(
		sm.messagingManager,
		sm.storageManager,
		sm.bigqueryManager,
		sm.environment,
		sm.logger,
	)
}
