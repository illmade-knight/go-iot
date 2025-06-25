package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// DataflowManager handles the setup, teardown, and verification of resources
// specific to a particular dataflow. It operates on a self-contained ResourceGroup.
type DataflowManager struct {
	messagingManager *MessagingManager
	storageManager   *StorageManager
	bigqueryManager  *BigQueryManager
	dataflowSpec     *ResourceGroup // The specific, self-contained dataflow configuration.
	environment      string
	projectID        string
	defaultLocation  string
	defaultLabels    map[string]string
	logger           zerolog.Logger
}

// NewDataflowManager creates a new DataflowManager for a specific dataflow by first creating
// all necessary clients and sub-managers.
func NewDataflowManager(
	ctx context.Context,
	dataflowSpec *ResourceGroup,
	projectID string,
	defaultLocation string,
	defaultLabels map[string]string,
	environment string,
	schemaRegistry map[string]interface{},
	logger zerolog.Logger,
) (*DataflowManager, error) {

	dfLogger := logger.With().Str("component", "DataflowManager").Str("dataflow", dataflowSpec.Name).Logger()

	// Initialize Google Cloud clients for the specific project.
	msgClient, err := CreateGoogleMessagingClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create Messaging client for dataflow '%s': %w", dataflowSpec.Name, err)
	}
	gcsClient, err := CreateGoogleGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create GCS client for dataflow '%s': %w", dataflowSpec.Name, err)
	}
	bqClient, err := CreateGoogleBigQueryClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create BigQuery client for dataflow '%s': %w", dataflowSpec.Name, err)
	}

	// Create instances of the specialized resource managers.
	messagingManager, err := NewMessagingManager(msgClient, dfLogger)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create Messaging manager for dataflow '%s': %w", dataflowSpec.Name, err)
	}
	storageManager, err := NewStorageManager(gcsClient, dfLogger)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create Storage manager for dataflow '%s': %w", dataflowSpec.Name, err)
	}
	bigQueryManager, err := NewBigQueryManager(bqClient, dfLogger, schemaRegistry)
	if err != nil {
		return nil, fmt.Errorf("dataflowmanager: failed to create BigQuery manager for dataflow '%s': %w", dataflowSpec.Name, err)
	}

	// Delegate to the manager-based constructor.
	return NewDataflowManagerFromManagers(
		messagingManager,
		storageManager,
		bigQueryManager,
		dataflowSpec,
		projectID,
		defaultLocation,
		defaultLabels,
		environment,
		logger,
	)
}

// NewDataflowManagerFromManagers creates a new DataflowManager from pre-existing,
// fully-formed sub-managers. This is ideal for testing purposes.
func NewDataflowManagerFromManagers(
	messagingManager *MessagingManager,
	storageManager *StorageManager,
	bigqueryManager *BigQueryManager,
	dataflowSpec *ResourceGroup,
	projectID string,
	defaultLocation string,
	defaultLabels map[string]string,
	environment string,
	logger zerolog.Logger,
) (*DataflowManager, error) {
	if messagingManager == nil || storageManager == nil || bigqueryManager == nil {
		return nil, errors.New("all managers (Messaging, Storage, BigQuery) must be non-nil")
	}

	// Return the new DataflowManager instance.
	return &DataflowManager{
		messagingManager: messagingManager,
		storageManager:   storageManager,
		bigqueryManager:  bigqueryManager,
		dataflowSpec:     dataflowSpec,
		environment:      environment,
		projectID:        projectID,
		defaultLocation:  defaultLocation,
		defaultLabels:    defaultLabels,
		logger:           logger.With().Str("component", "DataflowManager").Str("dataflow", dataflowSpec.Name).Logger(),
	}, nil
}

// Verify checks if all resources required by this dataflow exist and have compatible configurations.
func (dfm *DataflowManager) Verify(ctx context.Context) error {
	dfm.logger.Info().Msg("Starting dataflow resource verification...")
	g, gCtx := errgroup.WithContext(ctx)
	resources := dfm.dataflowSpec.Resources

	// Verify Messaging resources
	g.Go(func() error {
		if err := dfm.messagingManager.VerifyTopics(gCtx, resources.Topics); err != nil {
			return fmt.Errorf("messaging topic verification failed: %w", err)
		}
		if err := dfm.messagingManager.VerifySubscriptions(gCtx, resources.MessagingSubscriptions); err != nil {
			return fmt.Errorf("messaging subscription verification failed: %w", err)
		}
		return nil
	})

	// Verify Storage resources
	g.Go(func() error {
		if err := dfm.storageManager.VerifyBuckets(gCtx, resources.GCSBuckets); err != nil {
			return fmt.Errorf("GCS bucket verification failed: %w", err)
		}
		return nil
	})

	// Verify BigQuery resources
	g.Go(func() error {
		if err := dfm.bigqueryManager.VerifyDatasets(gCtx, resources.BigQueryDatasets); err != nil {
			return fmt.Errorf("BigQuery dataset verification failed: %w", err)
		}
		if err := dfm.bigqueryManager.VerifyTables(gCtx, resources.BigQueryTables); err != nil {
			return fmt.Errorf("BigQuery table verification failed: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		dfm.logger.Error().Err(err).Msg("Dataflow resource verification failed.")
		return err
	}

	dfm.logger.Info().Msg("Dataflow resource verification completed successfully.")
	return nil
}

// Setup creates all resources for this specific dataflow.
// It now calls the simplified Setup methods on the underlying managers.
func (dfm *DataflowManager) Setup(ctx context.Context) (*ProvisionedResources, error) {
	dfm.logger.Info().Msg("Starting dataflow-specific setup...")

	g, gCtx := errgroup.WithContext(ctx)
	resources := dfm.dataflowSpec.Resources

	g.Go(func() error {
		return dfm.messagingManager.Setup(gCtx, dfm.projectID, resources)
	})
	g.Go(func() error {
		return dfm.storageManager.Setup(gCtx, dfm.projectID, dfm.defaultLocation, dfm.defaultLabels, resources)
	})
	g.Go(func() error {
		return dfm.bigqueryManager.Setup(gCtx, dfm.projectID, dfm.defaultLocation, resources)
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed during parallel dataflow setup: %w", err)
	}

	// Populate and return ProvisionedResources based on the dataflow's configured resources.
	provResources := &ProvisionedResources{}
	for _, topic := range resources.Topics {
		provResources.Topics = append(provResources.Topics, ProvisionedTopic{Name: topic.Name, ProducerService: topic.ProducerService})
	}
	for _, sub := range resources.MessagingSubscriptions {
		provResources.Subscriptions = append(provResources.Subscriptions, ProvisionedSubscription{Name: sub.Name, Topic: sub.Topic})
	}
	for _, bucket := range resources.GCSBuckets {
		provResources.GCSBuckets = append(provResources.GCSBuckets, ProvisionedGCSBucket{Name: bucket.Name})
	}
	for _, dataset := range resources.BigQueryDatasets {
		provResources.BigQueryDatasets = append(provResources.BigQueryDatasets, ProvisionedBigQueryDataset{Name: dataset.Name})
	}
	for _, table := range resources.BigQueryTables {
		provResources.BigQueryTables = append(provResources.BigQueryTables, ProvisionedBigQueryTable{Dataset: table.Dataset, Name: table.Name})
	}

	dfm.logger.Info().Msg("Dataflow-specific setup completed successfully.")
	return provResources, nil
}

// Teardown deletes resources for this specific dataflow.
// It now calls the simplified Teardown methods on the underlying managers.
func (dfm *DataflowManager) Teardown(ctx context.Context, teardownProtection bool) error {
	dfm.logger.Info().Msg("Starting dataflow-specific teardown...")

	resources := dfm.dataflowSpec.Resources

	// Teardown sequentially to respect dependencies.
	if err := dfm.bigqueryManager.Teardown(ctx, dfm.projectID, resources, teardownProtection); err != nil {
		dfm.logger.Error().Err(err).Msg("Error during BigQuery teardown for dataflow, continuing...")
	}
	if err := dfm.storageManager.Teardown(ctx, resources, teardownProtection); err != nil {
		dfm.logger.Error().Err(err).Msg("Error during GCS teardown for dataflow, continuing...")
	}
	if err := dfm.messagingManager.Teardown(ctx, dfm.projectID, resources, teardownProtection); err != nil {
		dfm.logger.Error().Err(err).Msg("Error during Messaging teardown for dataflow, continuing...")
	}

	dfm.logger.Info().Msg("Dataflow-specific teardown completed.")
	return nil
}
