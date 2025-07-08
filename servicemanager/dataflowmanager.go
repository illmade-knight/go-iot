package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// DataflowManager handles the setup, teardown, and verification of resources.
// It orchestrates operations across different cloud resource types within a specific environment.
type DataflowManager struct {
	messagingManager *MessagingManager
	storageManager   *StorageManager
	bigqueryManager  *BigQueryManager
	environment      Environment // Environment is a property of the manager instance
	logger           zerolog.Logger
}

// NewDataflowManager creates a new DataflowManager by first creating
// all necessary clients and sub-managers.
func NewDataflowManager(
	ctx context.Context,
	environment Environment,
	schemaRegistry map[string]interface{},
	logger zerolog.Logger,
) (*DataflowManager, error) {

	// Logger for the DataflowManager itself, not tied to a specific dataflow instance name
	dfLogger := logger.With().Str("component", "DataflowManager").Logger()

	msgClient, err := CreateGoogleMessagingClient(ctx, environment.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create Messaging client: %w", err)
	}
	gcsClient, err := CreateGoogleGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create GCS client: %w", err)
	}
	bqClient, err := CreateGoogleBigQueryClient(ctx, environment.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create BigQuery client: %w", err)
	}

	msgManager, err := NewMessagingManager(msgClient, dfLogger)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create MessagingManager: %w", err)
	}
	storeManager, err := NewStorageManager(gcsClient, dfLogger)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create StorageManager: %w", err)
	}
	bqManager, err := NewBigQueryManager(bqClient, dfLogger, schemaRegistry, environment)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create BigQueryManager: %w", err)
	}

	return &DataflowManager{
		messagingManager: msgManager,
		storageManager:   storeManager,
		bigqueryManager:  bqManager,
		environment:      environment,
		logger:           dfLogger,
	}, nil
}

// NewDataflowManagerFromManagers is a constructor for testing, allowing pre-built managers to be injected.
func NewDataflowManagerFromManagers(
	messagingManager *MessagingManager,
	storageManager *StorageManager,
	bigqueryManager *BigQueryManager,
	environment Environment,
	logger zerolog.Logger,
) (*DataflowManager, error) {
	if messagingManager == nil || storageManager == nil || bigqueryManager == nil {
		return nil, fmt.Errorf("all managers must be non-nil")
	}
	return &DataflowManager{
		messagingManager: messagingManager,
		storageManager:   storageManager,
		bigqueryManager:  bigqueryManager,
		environment:      environment,
		logger:           logger.With().Str("component", "DataflowManager").Logger(), // Generic logger
	}, nil
}

// SetupResources creates all configured Pub/Sub topics, GCS buckets, and BigQuery resources for a given dataflow.
// The 'dataflowGroup' parameter should contain the full ResourceGroup for the specific dataflow being managed.
func (dfm *DataflowManager) SetupResources(ctx context.Context, dataflowGroup *ResourceGroup) (*ProvisionedResources, error) {
	dfm.logger.Info().Str("dataflow_name", dataflowGroup.Name).Msg("Starting resource setup for dataflow.")

	var allErrors []error
	newResources := &ProvisionedResources{}

	// Setup Messaging resources
	err := dfm.messagingManager.Setup(ctx, dfm.environment, dataflowGroup.Resources)
	if err != nil {
		allErrors = append(allErrors, fmt.Errorf("messaging setup failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during Messaging setup, continuing...")
	}

	// Setup Storage resources
	err = dfm.storageManager.Setup(ctx, dfm.environment, dataflowGroup.Resources)
	if err != nil {
		allErrors = append(allErrors, fmt.Errorf("GCS setup failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during GCS setup, continuing...")
	}

	// Setup BigQuery resources
	provisionedTables, provisionedDatasets, err := dfm.bigqueryManager.CreateResources(ctx, dataflowGroup.Resources)
	if err != nil {
		allErrors = append(allErrors, fmt.Errorf("BigQuery setup failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during BigQuery setup, continuing...")
	} else {
		newResources.BigQueryTables = provisionedTables
		newResources.BigQueryDatasets = provisionedDatasets
	}

	if len(allErrors) > 0 {
		dfm.logger.Error().Int("error_count", len(allErrors)).Msg("Resource setup completed with errors.")
		return nil, errors.Join(allErrors...)
	}

	dfm.logger.Info().Str("dataflow_name", dataflowGroup.Name).Msg("Resource setup completed successfully.")
	return newResources, nil
}

// TeardownResources deletes resources for a specific dataflow.
// The 'dataflowGroup' parameter should contain the full ResourceGroup for the specific dataflow being managed.
func (dfm *DataflowManager) TeardownResources(ctx context.Context, dataflowGroup *ResourceGroup) error {
	dfm.logger.Info().Str("dataflow_name", dataflowGroup.Name).Msg("Starting resource teardown for dataflow.")
	var errorMessages []string

	// Teardown sequentially to respect dependencies, collecting errors along the way.
	if err := dfm.bigqueryManager.Teardown(ctx, dataflowGroup.Resources); err != nil {
		errorMessages = append(errorMessages, fmt.Sprintf("BigQuery teardown failed: %v", err))
		dfm.logger.Error().Err(err).Msg("Error during BigQuery teardown, continuing...")
	}
	if err := dfm.storageManager.Teardown(ctx, dataflowGroup.Resources); err != nil {
		errorMessages = append(errorMessages, fmt.Sprintf("GCS teardown failed: %v", err))
		dfm.logger.Error().Err(err).Msg("Error during GCS teardown, continuing...")
	}
	if err := dfm.messagingManager.Teardown(ctx, dfm.environment, dataflowGroup.Resources); err != nil {
		errorMessages = append(errorMessages, fmt.Sprintf("Messaging teardown failed: %v", err))
		dfm.logger.Error().Err(err).Msg("Error during Messaging teardown, continuing...")
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("dataflow teardown completed with errors: %s", strings.Join(errorMessages, "; "))
	}

	dfm.logger.Info().Str("dataflow_name", dataflowGroup.Name).Msg("Resource teardown completed successfully.")
	return nil
}

// Verify checks if the specified Pub/Sub topics, GCS buckets, and BigQuery resources exist.
// The 'dataflowGroup' parameter should contain the full ResourceGroup for the specific dataflow being verified.
func (dfm *DataflowManager) Verify(ctx context.Context, dataflowGroup *ResourceGroup) error {
	dfm.logger.Info().Str("dataflow_name", dataflowGroup.Name).Msg("Starting resource verification for dataflow.")

	var allErrors []error

	// Verify Messaging resources
	if err := dfm.messagingManager.VerifyTopics(ctx, dataflowGroup.Resources.Topics); err != nil {
		allErrors = append(allErrors, fmt.Errorf("messaging topic verification failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during Messaging topic verification, continuing...")
	}
	if err := dfm.messagingManager.VerifySubscriptions(ctx, dataflowGroup.Resources.Subscriptions); err != nil {
		allErrors = append(allErrors, fmt.Errorf("messaging subscription verification failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during Messaging subscription verification, continuing...")
	}

	// Verify Storage resources
	if err := dfm.storageManager.VerifyBuckets(ctx, dataflowGroup.Resources.GCSBuckets); err != nil {
		allErrors = append(allErrors, fmt.Errorf("GCS verification failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during GCS verification, continuing...")
	}

	// Verify BigQuery resources

	if err := dfm.bigqueryManager.VerifyDatasets(ctx, dataflowGroup.Resources.BigQueryDatasets); err != nil {
		allErrors = append(allErrors, fmt.Errorf("BigQuery verification failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during BigQuery verification, continuing...")
	}

	if err := dfm.bigqueryManager.VerifyTables(ctx, dataflowGroup.Resources.BigQueryTables); err != nil {
		allErrors = append(allErrors, fmt.Errorf("BigQuery verification failed: %w", err))
		dfm.logger.Error().Err(err).Msg("Error during BigQuery verification, continuing...")
	}

	if len(allErrors) > 0 {
		dfm.logger.Error().Int("error_count", len(allErrors)).Msg("Resource verification completed with errors.")
		return errors.Join(allErrors...)
	}

	dfm.logger.Info().Str("dataflow_name", dataflowGroup.Name).Msg("Resource verification completed successfully.")
	return nil
}
