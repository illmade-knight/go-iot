package servicemanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestYAMLFile is a helper function to create a temporary YAML file for testing.
func createTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_config.yaml")
	err := os.WriteFile(filePath, []byte(content), 0600)
	require.NoError(t, err, "Failed to write temporary YAML file")
	return filePath
}

func TestLoadAndValidateConfig(t *testing.T) {
	// Base valid YAML that includes a global resource.
	validBaseYAML := `
default_project_id: "default-project"
resources:
  gcs_buckets:
    - name: "global-bucket"
`

	testCases := []struct {
		name          string
		yamlContent   string
		expectError   bool
		errorContains string
		checkConfig   func(t *testing.T, cfg *TopLevelConfig)
	}{
		{
			name:        "Valid configuration with global resource",
			yamlContent: validBaseYAML,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Equal(t, "default-project", cfg.DefaultProjectID)
				require.Len(t, cfg.ResourceGroup.Resources.GCSBuckets, 1)
				assert.Equal(t, "global-bucket", cfg.ResourceGroup.Resources.GCSBuckets[0].Name)
			},
		},
		{
			name: "Valid configuration with dataflow resource",
			yamlContent: `
dataflows:
  - name: my-dataflow
    resources:
      topics:
        - name: "df-topic"
`,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				require.Len(t, cfg.Dataflows, 1)
				require.Len(t, cfg.Dataflows[0].Resources.Topics, 1)
				assert.Equal(t, "df-topic", cfg.Dataflows[0].Resources.Topics[0].Name)
			},
		},
		{
			name:          "No resources defined anywhere",
			yamlContent:   `default_project_id: "default-project"`,
			expectError:   true,
			errorContains: "no resources are defined",
		},
		{
			name: "Global bucket with no name",
			yamlContent: `
resources:
  gcs_buckets:
    - location: "us-central1"
`,
			expectError:   true,
			errorContains: "global gcs_buckets[0] is missing a name",
		},
		{
			name: "Dataflow bucket with no name",
			yamlContent: `
dataflows:
  - name: my-dataflow
    resources:
      gcs_buckets:
        - location: "us-central1"
`,
			expectError:   true,
			errorContains: "gcs_buckets[0] in dataflow 'my-dataflow' (index 0) is missing a name",
		},
		{
			name:        "File not found",
			yamlContent: "",
			expectError: true,
		},
		{
			name:        "Invalid YAML format",
			yamlContent: `default_project_id: "project": "nested"`,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var configPath string
			if tc.name == "File not found" {
				configPath = filepath.Join(t.TempDir(), "non_existent_config.yaml")
			} else {
				configPath = createTestYAMLFile(t, tc.yamlContent)
			}

			cfg, err := LoadAndValidateConfig(configPath)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.True(t, strings.Contains(err.Error(), tc.errorContains), "Expected error to contain '%s', but got '%s'", tc.errorContains, err.Error())
				}
			} else {
				require.NoError(t, err)
				if tc.checkConfig != nil {
					tc.checkConfig(t, cfg)
				}
			}
		})
	}
}
