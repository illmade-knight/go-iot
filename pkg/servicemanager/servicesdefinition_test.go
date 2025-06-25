package servicemanager_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a temporary YAML file for testing.
func createTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "services.yaml")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err, "Failed to write temporary test YAML file")
	return filePath
}

// testYAMLContent is updated to use the new embedded ResourceGroup structure
// and the corrected field names.
const testYAMLContent = `
default_project_id: "default-proj"
default_location: "us-central1"

# These are global resources, part of the embedded ResourceGroup
resources:
  gcs_buckets:
    - name: "global-log-bucket"

environments:
  dev:
    project_id: "dev-proj"
  prod:
    project_id: "prod-proj"
    teardown_protection: true

# This is the top-level list of all service specifications
services:
  - name: "service-a"
    service_account: "sa-a@..."
  - name: "service-b"
    service_account: "sa-b@..."

# This is the list of named dataflows
dataflows:
  - name: "dataflow-1"
    # This 'services' key maps to 'ServiceNames' in the struct
    service_names:
      - "service-a"
    resources:
      topics:
        - name: "df1-topic"
          producer_service: "service-a"
`

// --- Test Cases ---

func TestNewYAMLServicesDefinition_Success(t *testing.T) {
	// Arrange
	filePath := createTestYAMLFile(t, testYAMLContent)

	// Act
	sd, err := servicemanager.NewYAMLServicesDefinition(filePath)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, sd)

	cfg, err := sd.GetTopLevelConfig()
	require.NoError(t, err)
	assert.Equal(t, "default-proj", cfg.DefaultProjectID)
	assert.Len(t, cfg.Services, 2)
	assert.Len(t, cfg.Dataflows, 1)

	// Verify the global/top-level resources from the embedded ResourceGroup
	require.Len(t, cfg.ResourceGroup.Resources.GCSBuckets, 1)
	assert.Equal(t, "global-log-bucket", cfg.ResourceGroup.Resources.GCSBuckets[0].Name)

	// Verify dataflow-1 and its specific resources
	df1, err := sd.GetDataflow("dataflow-1")
	require.NoError(t, err)
	assert.Equal(t, "dataflow-1", df1.Name)
	require.Len(t, df1.Resources.Topics, 1)
	assert.Equal(t, "df1-topic", df1.Resources.Topics[0].Name)
	require.Len(t, df1.ServiceNames, 1)
	assert.Equal(t, "service-a", df1.ServiceNames[0])

	// Verify service and project ID lookups still work
	svc, err := sd.GetService("service-b")
	require.NoError(t, err)
	assert.Equal(t, "service-b", svc.Name)

	devProj, err := sd.GetProjectID("dev")
	assert.NoError(t, err)
	assert.Equal(t, "dev-proj", devProj)
}

func TestNewInMemoryServicesDefinition_Success(t *testing.T) {
	// Arrange
	config := &servicemanager.TopLevelConfig{
		DefaultProjectID: "mem-default-proj",
		ResourceGroup: servicemanager.ResourceGroup{
			Resources: servicemanager.ResourcesSpec{
				GCSBuckets: []servicemanager.GCSBucket{{Name: "mem-global-bucket"}},
			},
		},
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "mem-test-proj"},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "mem-service"},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name: "mem-dataflow",
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{{Name: "mem-topic"}},
				},
			},
		},
	}

	// Act
	sd, err := servicemanager.NewInMemoryServicesDefinition(config)
	require.NoError(t, err)
	require.NotNil(t, sd)

	// Assert
	svc, err := sd.GetService("mem-service")
	require.NoError(t, err)
	assert.Equal(t, "mem-service", svc.Name)

	df, err := sd.GetDataflow("mem-dataflow")
	require.NoError(t, err)
	require.Len(t, df.Resources.Topics, 1)
	assert.Equal(t, "mem-topic", df.Resources.Topics[0].Name)
}

func TestServicesDefinition_ErrorCases(t *testing.T) {
	// Arrange
	sd, err := servicemanager.NewInMemoryServicesDefinition(&servicemanager.TopLevelConfig{})
	require.NoError(t, err)

	// Act & Assert
	_, err = sd.GetDataflow("not-found")
	assert.Error(t, err)

	_, err = sd.GetService("not-found")
	assert.Error(t, err)

	_, err = sd.GetProjectID("not-found")
	assert.Error(t, err)
}
