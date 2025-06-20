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
// It remains unexported as it's only used within this test file.
func createTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "services.yaml")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err, "Failed to write temporary test YAML file")
	return filePath
}

const testYAMLContent = `
default_project_id: "default-proj"
default_location: "us-central1"

environments:
  dev:
    project_id: "dev-proj"
  prod:
    project_id: "prod-proj"
    teardown_protection: true

services:
  - name: "service-a"
    service_account: "sa-a@..."
  - name: "service-b"
    service_account: "sa-b@..."

dataflows:
  - name: "dataflow-1"
    services:
      - "service-a"
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

	df, err := sd.GetDataflow("dataflow-1")
	require.NoError(t, err)
	assert.Equal(t, "dataflow-1", df.Name)

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
		Environments: map[string]servicemanager.EnvironmentSpec{
			"test": {ProjectID: "mem-test-proj"},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "mem-service"},
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
