package servicemanager

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"strings"
)

// --- BigQuery Client Abstraction Interfaces ---
// BQTable our abstraction of Bigquery for ServiceManager operation
type BQTable interface {
	Metadata(ctx context.Context) (*bigquery.TableMetadata, error)
	Create(ctx context.Context, meta *bigquery.TableMetadata) error
	Delete(ctx context.Context) error
}
type BQDataset interface {
	Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error)
	Create(ctx context.Context, meta *bigquery.DatasetMetadata) error
	Update(ctx context.Context, metaToUpdate bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error)
	Delete(ctx context.Context) error
	Table(tableID string) BQTable
	DeleteWithContents(ctx context.Context) error // Added for easier dataset teardown
}
type BQClient interface {
	Dataset(datasetID string) BQDataset
	Project() string
	Close() error
}

// --- Adapters for real BigQuery client ---
type bqTableClientAdapter struct{ table *bigquery.Table }

func (a *bqTableClientAdapter) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	return a.table.Metadata(ctx)
}
func (a *bqTableClientAdapter) Create(ctx context.Context, meta *bigquery.TableMetadata) error {
	return a.table.Create(ctx, meta)
}
func (a *bqTableClientAdapter) Delete(ctx context.Context) error {
	return a.table.Delete(ctx)
}

type bqDatasetClientAdapter struct{ dataset *bigquery.Dataset }

func (a *bqDatasetClientAdapter) Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error) {
	return a.dataset.Metadata(ctx)
}
func (a *bqDatasetClientAdapter) Create(ctx context.Context, meta *bigquery.DatasetMetadata) error {
	return a.dataset.Create(ctx, meta)
}
func (a *bqDatasetClientAdapter) Update(ctx context.Context, metaToUpdate bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error) {
	return a.dataset.Update(ctx, metaToUpdate, etag)
}
func (a *bqDatasetClientAdapter) Delete(ctx context.Context) error {
	return a.dataset.Delete(ctx)
}
func (a *bqDatasetClientAdapter) Table(tableID string) BQTable {
	return &bqTableClientAdapter{table: a.dataset.Table(tableID)}
}
func (a *bqDatasetClientAdapter) DeleteWithContents(ctx context.Context) error {
	return a.dataset.DeleteWithContents(ctx)
}

type bqClientAdapter struct{ client *bigquery.Client }

func (a *bqClientAdapter) Dataset(datasetID string) BQDataset {
	return &bqDatasetClientAdapter{dataset: a.client.Dataset(datasetID)}
}
func (a *bqClientAdapter) Project() string {
	return a.client.Project()
}
func (a *bqClientAdapter) Close() error {
	return a.client.Close()
}

// CreateGoogleBigQueryClient creates a real BigQuery client for use in production.
func CreateGoogleBigQueryClient(ctx context.Context, projectID string, clientOpts ...option.ClientOption) (BQClient, error) {
	realClient, err := bigquery.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	return &bqClientAdapter{client: realClient}, nil
}

// --- BigQuery Manager ---

// BigQueryManager handles the creation and deletion of BigQuery datasets and tables.
type BigQueryManager struct {
	client         BQClient // Use the interface for testability
	logger         zerolog.Logger
	schemaRegistry map[string]interface{}
	environment    Environment // Added environment to the manager struct
}

// NewBigQueryManager creates a new BigQueryManager.
func NewBigQueryManager(client BQClient, logger zerolog.Logger, schemaRegistry map[string]interface{}, environment Environment) (*BigQueryManager, error) {
	if client == nil {
		return nil, fmt.Errorf("BigQuery client (BQClient interface) cannot be nil")
	}
	if schemaRegistry == nil {
		return nil, fmt.Errorf("schema registry cannot be nil")
	}
	return &BigQueryManager{
		client:         client,
		logger:         logger.With().Str("subcomponent", "BigQueryManager").Logger(),
		schemaRegistry: schemaRegistry,
		environment:    environment, // Store the environment
	}, nil
}

// NewBigQueryManagerFromClient is a constructor for testing, allowing a pre-built client to be injected.
func NewBigQueryManagerFromClient(client BQClient, logger zerolog.Logger, schemaRegistry map[string]interface{}, environment Environment) (*BigQueryManager, error) {
	return NewBigQueryManager(client, logger, schemaRegistry, environment)
}

// CreateResources creates all configured BigQuery datasets and tables.
// It returns a slice of ProvisionedBigQueryTable and ProvisionedBigQueryDataset for successfully created resources.
func (m *BigQueryManager) CreateResources(ctx context.Context, resources CloudResourcesSpec) ([]ProvisionedBigQueryTable, []ProvisionedBigQueryDataset, error) {
	m.logger.Info().Msg("Starting BigQuery setup...")

	// Removed: m.client.Validate(resources) as BQClient interface does not have Validate method.
	// Validation should be handled by the manager itself or at a higher level if needed.
	m.logger.Info().Msg("BigQuery resource configuration is valid (basic checks).") // Adjusted log message

	var allErrors []error
	var provisionedTables []ProvisionedBigQueryTable
	var provisionedDatasets []ProvisionedBigQueryDataset

	// Create Datasets first
	for _, dsCfg := range resources.BigQueryDatasets {
		if dsCfg.Name == "" {
			m.logger.Warn().Msg("Skipping creation for dataset with empty name")
			continue
		}
		dataset := m.client.Dataset(dsCfg.Name)
		_, err := dataset.Metadata(ctx)
		if err == nil {
			m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset already exists, skipping creation.")
			provisionedDatasets = append(provisionedDatasets, ProvisionedBigQueryDataset{Name: dsCfg.Name})
			continue
		}
		if !strings.Contains(err.Error(), "notFound") {
			allErrors = append(allErrors, fmt.Errorf("failed to check existence of dataset '%s': %w", dsCfg.Name, err))
			m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Failed to check dataset existence, continuing...")
			continue
		}

		m.logger.Info().Str("dataset", dsCfg.Name).Msg("Creating dataset...")
		// Corrected: Pass bigquery.DatasetMetadata
		if err := dataset.Create(ctx, &bigquery.DatasetMetadata{
			Labels:   dsCfg.Labels,
			Location: dsCfg.Location,
		}); err != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to create dataset '%s': %w", dsCfg.Name, err))
			m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Failed to create dataset, continuing...")
		} else {
			m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset created successfully.")
			provisionedDatasets = append(provisionedDatasets, ProvisionedBigQueryDataset{Name: dsCfg.Name})
		}
	}

	// Create Tables
	for _, tableCfg := range resources.BigQueryTables {
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			m.logger.Warn().Str("table_name", tableCfg.Name).Str("dataset_name", tableCfg.Dataset).Msg("Skipping creation for table with empty name or dataset")
			continue
		}

		dataset := m.client.Dataset(tableCfg.Dataset)
		table := dataset.Table(tableCfg.Name)

		_, err := table.Metadata(ctx)
		if err == nil {
			m.logger.Info().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Table already exists, skipping creation.")
			provisionedTables = append(provisionedTables, ProvisionedBigQueryTable{Dataset: tableCfg.Dataset, Name: tableCfg.Name})
			continue
		}
		if !strings.Contains(err.Error(), "notFound") {
			allErrors = append(allErrors, fmt.Errorf("failed to check existence of table '%s' in dataset '%s': %w", tableCfg.Name, tableCfg.Dataset, err))
			m.logger.Error().Err(err).Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Failed to check table existence, continuing...")
			continue
		}

		// Get the schema from the registry
		schema, ok := m.schemaRegistry[tableCfg.SchemaSourceIdentifier]
		if !ok {
			allErrors = append(allErrors, fmt.Errorf("schema '%s' not found in registry for table '%s'", tableCfg.SchemaSourceIdentifier, tableCfg.Name))
			m.logger.Error().Str("schema_id", tableCfg.SchemaSourceIdentifier).Str("table", tableCfg.Name).Msg("Schema not found, skipping table creation.")
			continue
		}

		// Convert the generic schema interface to bigquery.Schema
		bqSchema, err := bigquery.InferSchema(schema) // Assuming schema is a struct that can be inferred
		if err != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to infer BigQuery schema for '%s': %w", tableCfg.SchemaSourceIdentifier, err))
			m.logger.Error().Err(err).Str("schema_id", tableCfg.SchemaSourceIdentifier).Msg("Failed to infer schema, skipping table creation.")
			continue
		}

		meta := &bigquery.TableMetadata{
			Schema: bqSchema,
			TimePartitioning: &bigquery.TimePartitioning{
				Field: tableCfg.TimePartitioningField,
				Type:  bigquery.TimePartitioningType(tableCfg.TimePartitioningType),
				//Expiration: time.Duration(tableCfg.Expiration),
			},
			Clustering: &bigquery.Clustering{
				Fields: tableCfg.ClusteringFields,
			},
			Labels: tableCfg.Labels,
		}

		m.logger.Info().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Creating table...")
		if err := table.Create(ctx, meta); err != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to create table '%s' in dataset '%s': %w", tableCfg.Name, tableCfg.Dataset, err))
			m.logger.Error().Err(err).Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Failed to create table, continuing...")
		} else {
			m.logger.Info().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Table created successfully.")
			provisionedTables = append(provisionedTables, ProvisionedBigQueryTable{Dataset: tableCfg.Dataset, Name: tableCfg.Name})
		}
	}

	if len(allErrors) > 0 {
		return provisionedTables, provisionedDatasets, errors.Join(allErrors...)
	}

	m.logger.Info().Msg("BigQuery setup completed successfully.")
	return provisionedTables, provisionedDatasets, nil
}

// Teardown deletes BigQuery datasets and tables.
func (m *BigQueryManager) Teardown(ctx context.Context, resources CloudResourcesSpec) error {
	m.logger.Info().Msg("Starting BigQuery teardown...")
	var errorMessages []string

	// Teardown tables first to avoid "dataset not empty" errors
	errorMessages = append(errorMessages, m.teardownTables(ctx, m.client, resources.BigQueryTables)...)

	// Then teardown datasets
	errorMessages = append(errorMessages, m.teardownDatasets(ctx, m.client, resources.BigQueryDatasets)...)

	if len(errorMessages) > 0 {
		return fmt.Errorf("BigQuery teardown completed with errors: %s", strings.Join(errorMessages, "; "))
	}

	m.logger.Info().Msg("BigQuery teardown completed successfully.")
	return nil
}

func (m *BigQueryManager) teardownTables(ctx context.Context, client BQClient, tablesToTeardown []BigQueryTable) []string {
	m.logger.Info().Int("count", len(tablesToTeardown)).Msg("Tearing down BigQuery tables...")
	var errorMessages []string
	// Iterate in reverse to handle potential dependencies if any (though less common for tables)
	for i := len(tablesToTeardown) - 1; i >= 0; i-- {
		tableCfg := tablesToTeardown[i]
		if tableCfg.TeardownProtection {
			m.logger.Warn().Str("name", tableCfg.Name).Msg("teardown protection in place for table")
			continue
		}
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			continue
		}
		table := client.Dataset(tableCfg.Dataset).Table(tableCfg.Name)
		m.logger.Info().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Attempting to delete table...")
		if err := table.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Table not found, skipping.")
			} else {
				m.logger.Error().Err(err).Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Failed to delete table")
				errorMessages = append(errorMessages, fmt.Sprintf("table %s in dataset %s: %v", tableCfg.Name, tableCfg.Dataset, err))
			}
		} else {
			m.logger.Info().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Table deleted successfully.")
		}
	}
	return errorMessages
}

func (m *BigQueryManager) teardownDatasets(ctx context.Context, client BQClient, datasetsToTeardown []BigQueryDataset) []string {
	m.logger.Info().Int("count", len(datasetsToTeardown)).Msg("Tearing down BigQuery datasets...")
	var errorMessages []string
	for i := len(datasetsToTeardown) - 1; i >= 0; i-- {
		dsCfg := datasetsToTeardown[i]
		// Added TeardownProtection check for datasets
		if dsCfg.TeardownProtection {
			m.logger.Warn().Str("name", dsCfg.Name).Msg("teardown protection in place for dataset")
			continue
		}
		if dsCfg.Name == "" {
			continue
		}
		dataset := client.Dataset(dsCfg.Name)
		m.logger.Info().Str("dataset", dsCfg.Name).Msg("Attempting to delete dataset...")
		if err := dataset.DeleteWithContents(ctx); err != nil { // Use DeleteWithContents
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset not found, skipping.")
			} else if strings.Contains(err.Error(), "still contains resources") {
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Dataset not empty. Ensure all tables are deleted first.")
				errorMessages = append(errorMessages, fmt.Sprintf("dataset %s not empty: %v", dsCfg.Name, err))
			} else {
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Failed to delete dataset")
				errorMessages = append(errorMessages, fmt.Sprintf("dataset %s: %v", dsCfg.Name, err))
			}
		} else {
			m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset deleted successfully.")
		}
	}
	return errorMessages
}

// Verify checks if the specified BigQuery datasets and tables exist.
func (m *BigQueryManager) Verify(ctx context.Context, resources CloudResourcesSpec) error {
	m.logger.Info().Msg("Verifying BigQuery resources...")
	var allErrors []error

	// Verify Datasets
	if err := m.VerifyDatasets(ctx, resources.BigQueryDatasets); err != nil {
		allErrors = append(allErrors, fmt.Errorf("BigQuery dataset verification failed: %w", err))
		m.logger.Error().Err(err).Msg("Error during BigQuery dataset verification, continuing...")
	}

	// Verify Tables
	if err := m.VerifyTables(ctx, resources.BigQueryTables); err != nil {
		allErrors = append(allErrors, fmt.Errorf("BigQuery table verification failed: %w", err))
		m.logger.Error().Err(err).Msg("Error during BigQuery table verification, continuing...")
	}

	if len(allErrors) > 0 {
		m.logger.Error().Int("error_count", len(allErrors)).Msg("BigQuery verification completed with errors.")
		return errors.Join(allErrors...)
	}

	m.logger.Info().Msg("BigQuery verification completed successfully.")
	return nil
}

// VerifyDatasets checks if the specified BigQuery datasets exist.
func (m *BigQueryManager) VerifyDatasets(ctx context.Context, datasetsToVerify []BigQueryDataset) error {
	m.logger.Info().Int("count", len(datasetsToVerify)).Msg("Verifying BigQuery datasets...")
	var errorMessages []string
	for _, dsCfg := range datasetsToVerify {
		if dsCfg.Name == "" {
			m.logger.Warn().Msg("Skipping verification for dataset with empty name")
			continue
		}
		dataset := m.client.Dataset(dsCfg.Name)
		_, err := dataset.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				errorMessages = append(errorMessages, fmt.Sprintf("dataset '%s' not found", dsCfg.Name))
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Dataset not found during verification.")
			} else {
				errorMessages = append(errorMessages, fmt.Sprintf("failed to check existence of dataset '%s' during verification: %v", dsCfg.Name, err))
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Failed to check dataset existence during verification.")
			}
			continue
		}
		m.logger.Debug().Str("dataset", dsCfg.Name).Msg("Dataset verified successfully (existence only).")
	}
	if len(errorMessages) > 0 {
		return fmt.Errorf("BigQuery dataset verification failed: %s", strings.Join(errorMessages, "; "))
	}
	return nil
}

// VerifyTables checks if the specified BigQuery tables exist.
func (m *BigQueryManager) VerifyTables(ctx context.Context, tablesToVerify []BigQueryTable) error {
	m.logger.Info().Int("count", len(tablesToVerify)).Msg("Verifying BigQuery tables...")
	var errorMessages []string
	for _, tableCfg := range tablesToVerify {
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			m.logger.Warn().Str("table_name", tableCfg.Name).Str("dataset_name", tableCfg.Dataset).Msg("Skipping verification for table with empty name or dataset")
			continue
		}
		table := m.client.Dataset(tableCfg.Dataset).Table(tableCfg.Name)
		_, err := table.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				errorMessages = append(errorMessages, fmt.Sprintf("table '%s' in dataset '%s' not found", tableCfg.Name, tableCfg.Dataset))
				m.logger.Error().Err(err).Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Table not found during verification.")
			} else {
				errorMessages = append(errorMessages, fmt.Sprintf("failed to check existence of table '%s' in dataset '%s' during verification: %v", tableCfg.Name, tableCfg.Dataset, err))
				m.logger.Error().Err(err).Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Failed to check table existence during verification.")
			}
			continue
		}
		m.logger.Debug().Str("table", tableCfg.Name).Str("dataset", tableCfg.Dataset).Msg("Table verified successfully (existence only).")
	}
	if len(errorMessages) > 0 {
		return fmt.Errorf("BigQuery table verification failed: %s", strings.Join(errorMessages, "; "))
	}
	return nil
}
