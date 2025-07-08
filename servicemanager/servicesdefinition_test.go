package servicemanager_test

import (
	servicemanager2 "github.com/illmade-knight/go-iot/servicemanager"
	"os"
	"path/filepath"
	"testing"

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
environment:
  project_id: "default-proj"
  location: "us-central1"

# These are global resources, part of the embedded ResourceGroup
service_manager_resources:
  gcs_buckets:
    - name: "global-log-bucket"

deployment_environments:
  dev:
    project_id: "dev-proj"
  prod:
    project_id: "prod-proj"
    teardown_protection: true

# This is the list of named dataflows
dataflows:
  - dataflow-1
    - name: "dataflow-1"
`

// --- Test Cases ---

func TestNewYAMLServicesDefinition_Success(t *testing.T) {
	// Arrange
	filePath := createTestYAMLFile(t, testYAMLContent)

	// Act
	sd, err := servicemanager2.NewYAMLServicesDefinition(filePath)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, sd)

	cfg, err := sd.GetMicroserviceArchitecture()
	require.NoError(t, err)
	assert.Equal(t, "default-proj", cfg.ProjectID)
}

func TestNewInMemoryServicesDefinition_Success(t *testing.T) {
	// Arrange
	config := &servicemanager2.MicroserviceArchitecture{
		Environment: servicemanager2.Environment{
			Name:               "",
			ProjectID:          "mem-default-proj",
			Labels:             nil,
			Location:           "",
			Region:             "",
			TeardownProtection: false,
		},
		ServiceManagerResources: servicemanager2.ResourceGroup{
			Resources: servicemanager2.CloudResourcesSpec{
				GCSBuckets: []servicemanager2.GCSBucket{{CloudResource: servicemanager2.CloudResource{Name: "mem-global-bucket"}}},
			},
		},
		DeploymentEnvironments: map[string]servicemanager2.Environment{
			"test": {ProjectID: "mem-test-proj"},
		},
		Dataflows: map[string]servicemanager2.ResourceGroup{
			"unique": {
				Name: "mem-dataflow",
				Resources: servicemanager2.CloudResourcesSpec{
					Topics: []servicemanager2.TopicConfig{{CloudResource: servicemanager2.CloudResource{Name: "mem-topic"}}},
				},
			},
		},
	}

	// Act
	sd, err := servicemanager2.NewInMemoryServicesDefinition(config)
	require.NoError(t, err)
	require.NotNil(t, sd)

}
