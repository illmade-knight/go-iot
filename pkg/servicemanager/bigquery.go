package servicemanager

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors" // Make sure errors is imported
	"fmt"
	telemetry "github.com/illmade-knight/go-iot/gen/go/protos/telemetry"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"

	"strings"
)

// --- BigQuery Client Abstraction Interfaces ---
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
func (a *bqTableClientAdapter) Delete(ctx context.Context) error { return a.table.Delete(ctx) }

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
func (a *bqDatasetClientAdapter) Delete(ctx context.Context) error { return a.dataset.Delete(ctx) }
func (a *bqDatasetClientAdapter) Table(tableID string) BQTable {
	return &bqTableClientAdapter{table: a.dataset.Table(tableID)}
}

type bqClientAdapter struct{ client *bigquery.Client }

func (a *bqClientAdapter) Dataset(datasetID string) BQDataset {
	return &bqDatasetClientAdapter{dataset: a.client.Dataset(datasetID)}
}
func (a *bqClientAdapter) Project() string { return a.client.Project() }
func (a *bqClientAdapter) Close() error    { return a.client.Close() }

func NewBigQueryClientAdapter(client *bigquery.Client) BQClient {
	if client == nil {
		return nil
	}
	return &bqClientAdapter{client: client}
}

var _ BQTable = &bqTableClientAdapter{}
var _ BQDataset = &bqDatasetClientAdapter{}
var _ BQClient = &bqClientAdapter{}

// BigQueryManager handles the creation and deletion of BigQuery datasets and tables.
type BigQueryManager struct {
	client         BQClient
	logger         zerolog.Logger
	schemaRegistry map[string]interface{}
}

// NewBigQueryManager creates a new BigQueryManager with an injected BQClient interface.
func NewBigQueryManager(client BQClient, logger zerolog.Logger, knownSchemas map[string]interface{}) (*BigQueryManager, error) {
	if client == nil {
		return nil, fmt.Errorf("BigQuery client (BQClient interface) cannot be nil")
	}
	if knownSchemas == nil {
		knownSchemas = make(map[string]interface{})
	}
	// Register the MeterReadingBQWrapper for schema inference.
	// The key MUST match exactly what's in the YAML's schema_source_identifier.
	knownSchemas["github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading"] = &telemetry.MeterReadingBQWrapper{}

	return &BigQueryManager{
		client:         client,
		logger:         logger.With().Str("component", "BigQueryManager").Logger(),
		schemaRegistry: knownSchemas,
	}, nil
}

// CreateGoogleBigQueryClient creates a real BigQuery client for use when not testing with mocks.
func CreateGoogleBigQueryClient(ctx context.Context, projectID string, opts ...option.ClientOption) (BQClient, error) {
	realClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	return NewBigQueryClientAdapter(realClient), nil
}

// Setup creates all configured BigQuery datasets and tables from a given resource specification.
func (m *BigQueryManager) Setup(ctx context.Context, projectID, defaultLocation string, resources ResourcesSpec) error {
	if m.client.Project() != projectID {
		return fmt.Errorf("injected BigQuery client is for project '%s', but setup is targeted for project '%s'", m.client.Project(), projectID)
	}
	m.logger.Info().Str("project_id", projectID).Msg("Starting BigQuery setup using injected client")

	if err := m.setupDatasets(ctx, m.client, resources.BigQueryDatasets, defaultLocation, projectID); err != nil {
		return err
	}
	if err := m.setupTables(ctx, m.client, resources.BigQueryTables, projectID); err != nil {
		return err
	}
	m.logger.Info().Str("project_id", projectID).Msg("BigQuery setup completed successfully")
	return nil
}

func (m *BigQueryManager) setupDatasets(ctx context.Context, client BQClient, datasetsToCreate []BigQueryDataset, defaultLocation, projectID string) error {
	m.logger.Info().Int("count", len(datasetsToCreate)).Msg("Setting up BigQuery datasets...")
	for _, dsCfg := range datasetsToCreate {
		if dsCfg.Name == "" {
			m.logger.Error().Msg("Skipping dataset with empty name")
			continue
		}
		dataset := client.Dataset(dsCfg.Name)
		_, err := dataset.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset not found, creating dataset...")
				meta := &bigquery.DatasetMetadata{Name: dsCfg.Name, Description: dsCfg.Description, Labels: dsCfg.Labels}
				if dsCfg.Location != "" {
					meta.Location = dsCfg.Location
				} else if defaultLocation != "" {
					meta.Location = defaultLocation
				} else {
					m.logger.Warn().Str("dataset_id", dsCfg.Name).Msg("Dataset location not specified, relying on BigQuery defaults.")
				}
				if err := dataset.Create(ctx, meta); err != nil {
					return fmt.Errorf("failed to create dataset '%s': %w", dsCfg.Name, err)
				}
				m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset created successfully")
			} else {
				return fmt.Errorf("failed to get metadata for dataset '%s': %w", dsCfg.Name, err)
			}
		} else {
			m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset already exists. Ensuring configuration...")
			update := bigquery.DatasetMetadataToUpdate{Description: dsCfg.Description}
			if dsCfg.Labels != nil {
				update.SetLabel("dummyKey", "")
				for k, v := range dsCfg.Labels {
					update.SetLabel(k, v)
				}
				update.DeleteLabel("dummyKey")
			}
			if _, err := dataset.Update(ctx, update, ""); err != nil {
				m.logger.Warn().Err(err).Str("dataset_id", dsCfg.Name).Msg("Failed to update dataset metadata")
			} else {
				m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset metadata updated/ensured.")
			}
		}
	}
	return nil
}

func (m *BigQueryManager) setupTables(ctx context.Context, client BQClient, tablesToCreate []BigQueryTable, projectID string) error {
	m.logger.Info().Int("count", len(tablesToCreate)).Msg("Setting up BigQuery tables...")
	for _, tableCfg := range tablesToCreate {
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			m.logger.Error().Str("table_name", tableCfg.Name).Str("dataset_name", tableCfg.Dataset).Msg("Skipping table with empty name or dataset")
			continue
		}
		dataset := client.Dataset(tableCfg.Dataset)
		table := dataset.Table(tableCfg.Name)
		_, err := table.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("table_id", tableCfg.Name).Str("dataset_id", tableCfg.Dataset).Msg("Table not found, creating table...")
				schema, errSchema := m.loadTableSchema(tableCfg)
				if errSchema != nil {
					return fmt.Errorf("load schema for '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, errSchema)
				}
				if schema == nil {
					return fmt.Errorf("schema nil for '%s.%s'", tableCfg.Dataset, tableCfg.Name)
				}
				meta := &bigquery.TableMetadata{Name: tableCfg.Name, Description: tableCfg.Description, Schema: schema}
				if tableCfg.TimePartitioningField != "" {
					meta.TimePartitioning = &bigquery.TimePartitioning{Field: tableCfg.TimePartitioningField}
					if tableCfg.TimePartitioningType != "" {
						switch strings.ToUpper(tableCfg.TimePartitioningType) {
						case "HOUR":
							meta.TimePartitioning.Type = bigquery.HourPartitioningType
						case "DAY":
							meta.TimePartitioning.Type = bigquery.DayPartitioningType
						case "MONTH":
							meta.TimePartitioning.Type = bigquery.MonthPartitioningType
						case "YEAR":
							meta.TimePartitioning.Type = bigquery.YearPartitioningType
						default:
							m.logger.Warn().Str("table", tableCfg.Name).Msg("Unsupported time partitioning type")
						}
					}
				}
				if len(tableCfg.ClusteringFields) > 0 {
					meta.Clustering = &bigquery.Clustering{Fields: tableCfg.ClusteringFields}
				}
				if err := table.Create(ctx, meta); err != nil {
					return fmt.Errorf("create table '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, err)
				}
				m.logger.Info().Str("table_id", tableCfg.Name).Msg("Table created successfully")
			} else {
				return fmt.Errorf("get metadata for '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, err)
			}
		} else {
			m.logger.Info().Str("table_id", tableCfg.Name).Msg("Table already exists.")
		}
	}
	return nil
}

func (m *BigQueryManager) loadTableSchema(tableCfg BigQueryTable) (bigquery.Schema, error) {
	m.logger.Info().Str("table", tableCfg.Name).Str("source_type", tableCfg.SchemaSourceType).Str("identifier", tableCfg.SchemaSourceIdentifier).Msg("Loading table schema")

	switch tableCfg.SchemaSourceType {
	case "go_struct":
		instance, ok := m.schemaRegistry[tableCfg.SchemaSourceIdentifier]
		if !ok {
			return nil, fmt.Errorf("unknown schema_source_identifier '%s' for go_struct type in table '%s'. Ensure it's registered in NewBigQueryManager.", tableCfg.SchemaSourceIdentifier, tableCfg.Name)
		}
		schema, err := bigquery.InferSchema(instance)
		if err != nil {
			return nil, fmt.Errorf("failed to infer schema from Go struct/saver '%s' for table '%s': %w", tableCfg.SchemaSourceIdentifier, tableCfg.Name, err)
		}
		m.logger.Info().Str("table", tableCfg.Name).Msg("Inferred schema using registered Go struct/saver. Column names will reflect Saver output (expected to be snake_case).")
		return schema, nil
	case "json_file":
		return nil, fmt.Errorf("json_file schema loading not implemented for '%s'", tableCfg.Name)
	default:
		return nil, fmt.Errorf("unsupported schema_source_type '%s' for '%s'", tableCfg.SchemaSourceType, tableCfg.Name)
	}
}

// VerifyDatasets checks if the specified BigQuery datasets exist and have compatible configurations.
func (m *BigQueryManager) VerifyDatasets(ctx context.Context, datasetsToVerify []BigQueryDataset) error {
	m.logger.Info().Int("count", len(datasetsToVerify)).Msg("Verifying BigQuery datasets...")
	for _, dsCfg := range datasetsToVerify {
		if dsCfg.Name == "" {
			m.logger.Warn().Msg("Skipping verification for dataset with empty name")
			continue
		}
		dataset := m.client.Dataset(dsCfg.Name)
		meta, err := dataset.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				return fmt.Errorf("dataset '%s' not found during verification", dsCfg.Name)
			}
			return fmt.Errorf("failed to get metadata for dataset '%s' during verification: %w", dsCfg.Name, err)
		}

		if dsCfg.Location != "" && strings.ToLower(meta.Location) != strings.ToLower(dsCfg.Location) {
			return fmt.Errorf("dataset '%s' location mismatch: expected '%s' (configured) vs '%s' (actual)", dsCfg.Name, dsCfg.Location, meta.Location)
		}
		if dsCfg.Description != "" && meta.Description != dsCfg.Description {
			return fmt.Errorf("dataset '%s' description mismatch: expected '%s' (configured) vs '%s' (actual)", dsCfg.Name, dsCfg.Description, meta.Description)
		}
		for k, v := range dsCfg.Labels {
			if meta.Labels == nil || meta.Labels[k] != v {
				return fmt.Errorf("dataset '%s' label '%s' mismatch: expected '%s' (configured) vs '%s' (actual)", dsCfg.Name, k, v, meta.Labels[k])
			}
		}

		m.logger.Debug().Str("dataset_id", dsCfg.Name).Msg("Dataset verified successfully.")
	}
	return nil
}

// VerifyTables checks if the specified BigQuery tables exist and have compatible configurations.
func (m *BigQueryManager) VerifyTables(ctx context.Context, tablesToVerify []BigQueryTable) error {
	m.logger.Info().Int("count", len(tablesToVerify)).Msg("Verifying BigQuery tables...")
	for _, tableCfg := range tablesToVerify {
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			m.logger.Warn().Str("table_name", tableCfg.Name).Str("dataset_name", tableCfg.Dataset).Msg("Skipping verification for table with empty name or dataset")
			continue
		}
		dataset := m.client.Dataset(tableCfg.Dataset)
		table := dataset.Table(tableCfg.Name)
		meta, err := table.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				return fmt.Errorf("table '%s.%s' not found during verification", tableCfg.Dataset, tableCfg.Name)
			}
			return fmt.Errorf("failed to get metadata for table '%s.%s' during verification: %w", tableCfg.Dataset, tableCfg.Name, err)
		}

		if tableCfg.Description != "" && meta.Description != tableCfg.Description {
			return fmt.Errorf("table '%s.%s' description mismatch: expected '%s' (configured) vs '%s' (actual)", tableCfg.Dataset, tableCfg.Name, tableCfg.Description, meta.Description)
		}

		expectedSchema, err := m.loadTableSchema(tableCfg)
		if err != nil {
			return fmt.Errorf("failed to load expected schema for table '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, err)
		}
		if expectedSchema == nil || len(expectedSchema) == 0 {
			return fmt.Errorf("expected schema for table '%s.%s' is empty", tableCfg.Dataset, tableCfg.Name)
		}
		if err := compareBigQuerySchemas(expectedSchema, meta.Schema); err != nil {
			return fmt.Errorf("table '%s.%s' schema mismatch: %w", tableCfg.Dataset, tableCfg.Name, err)
		}

		if tableCfg.TimePartitioningField != "" {
			if meta.TimePartitioning == nil {
				return fmt.Errorf("table '%s.%s' time partitioning expected on field '%s' but not found", tableCfg.Dataset, tableCfg.Name, tableCfg.TimePartitioningField)
			}
			if meta.TimePartitioning.Field != tableCfg.TimePartitioningField {
				return fmt.Errorf("table '%s.%s' time partitioning field mismatch: expected '%s' (configured) vs '%s' (actual)", tableCfg.Dataset, tableCfg.Name, tableCfg.TimePartitioningField, meta.TimePartitioning.Field)
			}
			if tableCfg.TimePartitioningType != "" {
				expectedType := parseBigQueryPartitioningType(tableCfg.TimePartitioningType)
				if meta.TimePartitioning.Type != expectedType {
					return fmt.Errorf("table '%s.%s' time partitioning type mismatch: expected '%s' (configured) vs '%s' (actual)", tableCfg.Dataset, tableCfg.Name, tableCfg.TimePartitioningType, meta.TimePartitioning.Type)
				}
			}
		} else if meta.TimePartitioning != nil {
			return fmt.Errorf("table '%s.%s' has unexpected time partitioning (none configured, exists)", tableCfg.Dataset, tableCfg.Name)
		}

		if len(tableCfg.ClusteringFields) > 0 {
			if meta.Clustering == nil || len(meta.Clustering.Fields) != len(tableCfg.ClusteringFields) {
				return fmt.Errorf("table '%s.%s' clustering fields count mismatch: expected %d (configured) vs %d (actual)", tableCfg.Dataset, tableCfg.Name, len(tableCfg.ClusteringFields), len(meta.Clustering.Fields))
			}
			for i, field := range tableCfg.ClusteringFields {
				if meta.Clustering.Fields[i] != field {
					return fmt.Errorf("table '%s.%s' clustering field mismatch at index %d: expected '%s' (configured) vs '%s' (actual)", tableCfg.Dataset, tableCfg.Name, i, field, meta.Clustering.Fields[i])
				}
			}
		} else if meta.Clustering != nil {
			return fmt.Errorf("table '%s.%s' has unexpected clustering fields (none configured, exists)", tableCfg.Dataset, tableCfg.Name)
		}

		m.logger.Debug().Str("table_id", tableCfg.Name).Msg("Table verified successfully.")
	}
	return nil
}

func parseBigQueryPartitioningType(s string) bigquery.TimePartitioningType {
	switch strings.ToUpper(s) {
	case "HOUR":
		return bigquery.HourPartitioningType
	case "DAY":
		return bigquery.DayPartitioningType
	case "MONTH":
		return bigquery.MonthPartitioningType
	case "YEAR":
		return bigquery.YearPartitioningType
	default:
		return "" // Invalid or unsupported
	}
}

func compareBigQuerySchemas(expected, actual bigquery.Schema) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("schema field count mismatch: expected %d, actual %d", len(expected), len(actual))
	}

	for i := range expected {
		expectedField := expected[i]
		actualField := actual[i]

		if expectedField.Name != actualField.Name {
			return fmt.Errorf("field %d name mismatch: expected '%s', actual '%s'", i, expectedField.Name, actualField.Name)
		}
		if expectedField.Type != actualField.Type {
			return fmt.Errorf("field '%s' type mismatch: expected '%s', actual '%s'", expectedField.Name, expectedField.Type, actualField.Type)
		}
		if expectedField.Required != actualField.Required {
			return fmt.Errorf("field '%s' required mismatch: expected %t, actual %t", expectedField.Name, expectedField.Required, actualField.Required)
		}
		if expectedField.Repeated != actualField.Repeated {
			return fmt.Errorf("field '%s' repeated mismatch: expected %t, actual %t", expectedField.Name, expectedField.Repeated, actualField.Repeated)
		}
		if expectedField.Description != actualField.Description {
			return fmt.Errorf("field '%s' description mismatch: expected '%s', actual '%s'", expectedField.Name, expectedField.Description, actualField.Description)
		}

		if len(expectedField.Schema) > 0 || len(actualField.Schema) > 0 {
			if err := compareBigQuerySchemas(expectedField.Schema, actualField.Schema); err != nil {
				return fmt.Errorf("field '%s' nested schema mismatch: %w", expectedField.Name, err)
			}
		}
	}
	return nil
}

// Teardown deletes all configured BigQuery resources from a given resource specification.
func (m *BigQueryManager) Teardown(ctx context.Context, projectID string, resources ResourcesSpec, teardownProtection bool) error {
	if m.client.Project() != projectID {
		return fmt.Errorf("injected client project '%s' != target project '%s'", m.client.Project(), projectID)
	}
	m.logger.Info().Str("project_id", projectID).Msg("Starting BigQuery teardown")
	if teardownProtection {
		return fmt.Errorf("teardown protection enabled for this operation")
	}

	var errorMessages []string
	if err := m.teardownTables(ctx, m.client, resources.BigQueryTables); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}
	if err := m.teardownDatasets(ctx, m.client, resources.BigQueryDatasets); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	m.logger.Info().Str("project_id", projectID).Msg("BigQuery teardown completed")
	if len(errorMessages) > 0 {
		return errors.New(strings.Join(errorMessages, "; "))
	}
	return nil
}

func (m *BigQueryManager) teardownTables(ctx context.Context, client BQClient, tablesToTeardown []BigQueryTable) error {
	m.logger.Info().Int("count", len(tablesToTeardown)).Msg("Tearing down BigQuery tables...")
	var errorMessages []string
	for i := len(tablesToTeardown) - 1; i >= 0; i-- {
		tableCfg := tablesToTeardown[i]
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			continue
		}
		table := client.Dataset(tableCfg.Dataset).Table(tableCfg.Name)
		m.logger.Info().Str("table", tableCfg.Name).Msg("Attempting to delete table...")
		if err := table.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("table", tableCfg.Name).Msg("Table not found, skipping.")
			} else {
				m.logger.Error().Err(err).Str("table", tableCfg.Name).Msg("Failed to delete table")
				errorMessages = append(errorMessages, fmt.Sprintf("table %s: %v", tableCfg.Name, err))
			}
		} else {
			m.logger.Info().Str("table", tableCfg.Name).Msg("Table deleted.")
		}
	}
	if len(errorMessages) > 0 {
		return errors.New(strings.Join(errorMessages, ", "))
	}
	return nil
}

func (m *BigQueryManager) teardownDatasets(ctx context.Context, client BQClient, datasetsToTeardown []BigQueryDataset) error {
	m.logger.Info().Int("count", len(datasetsToTeardown)).Msg("Tearing down BigQuery datasets...")
	var errorMessages []string
	for i := len(datasetsToTeardown) - 1; i >= 0; i-- {
		dsCfg := datasetsToTeardown[i]
		if dsCfg.Name == "" {
			continue
		}
		dataset := client.Dataset(dsCfg.Name)
		m.logger.Info().Str("dataset", dsCfg.Name).Msg("Attempting to delete dataset...")
		if err := dataset.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset not found, skipping.")
			} else if strings.Contains(err.Error(), "still contains resources") {
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Dataset not empty.")
				errorMessages = append(errorMessages, fmt.Sprintf("dataset %s not empty: %v", dsCfg.Name, err))
			} else {
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Failed to delete dataset")
				errorMessages = append(errorMessages, fmt.Sprintf("dataset %s: %v", dsCfg.Name, err))
			}
		} else {
			m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset deleted.")
		}
	}
	if len(errorMessages) > 0 {
		return errors.New(strings.Join(errorMessages, ", "))
	}
	return nil
}
