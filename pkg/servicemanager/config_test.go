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
	// Base valid YAML that includes at least one resource.
	validBaseYAML := `
default_project_id: "default-project"
resources:
  messaging_topics:
    - name: "topic-a"
`

	testCases := []struct {
		name          string
		yamlContent   string
		expectError   bool
		errorContains string // Substring to check in the error message
		checkConfig   func(t *testing.T, cfg *TopLevelConfig)
	}{
		{
			name:        "Valid configuration with Messaging Topic",
			yamlContent: validBaseYAML,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Equal(t, "default-project", cfg.DefaultProjectID)
				assert.Len(t, cfg.Resources.MessagingTopics, 1)
				assert.Equal(t, "topic-a", cfg.Resources.MessagingTopics[0].Name)
			},
		},
		{
			name: "Valid configuration with GCS only",
			yamlContent: `
default_project_id: "default-project"
resources:
  gcs_buckets:
    - name: "my-gcs-bucket-only"
`,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Len(t, cfg.Resources.GCSBuckets, 1)
				assert.Equal(t, "my-gcs-bucket-only", cfg.Resources.GCSBuckets[0].Name)
				assert.Empty(t, cfg.Resources.MessagingTopics, "Should have no messaging topics")
			},
		},
		{
			name:          "File not found",
			yamlContent:   "", // Will use a non-existent path for this test case
			expectError:   true,
			errorContains: "failed to read config file",
		},
		{
			name:          "Invalid YAML format",
			yamlContent:   "default_project_id: project\n  badly_indented: true",
			expectError:   true,
			errorContains: "failed to unmarshal YAML",
		},
		{
			name: "Missing default_project_id (warning only)",
			yamlContent: `
resources:
  messaging_topics:
    - name: "topic-a"
`,
			expectError: false, // This is not a fatal error
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Empty(t, cfg.DefaultProjectID)
			},
		},
		{
			name: "No resources defined",
			yamlContent: `
default_project_id: "project"
resources: {}
`,
			expectError:   true,
			errorContains: "no resources (e.g., gcs_buckets, messaging_topics) defined",
		},
		{
			name: "GCS bucket missing name",
			yamlContent: `
default_project_id: "project"
resources:
  gcs_buckets:
    - location: "US"
`,
			expectError:   true,
			errorContains: "gcs_buckets[0] is missing a name",
		},
		{
			name: "Valid configuration with multiple resource types",
			yamlContent: `
default_project_id: "my-default-gcp-project"
environments:
  test:
    project_id: "my-test-gcp-project"
resources:
  messaging_topics:
    - name: "ingested-device-data"
  gcs_buckets:
    - name: "iot-device-archive-bucket"
`,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Equal(t, "my-default-gcp-project", cfg.DefaultProjectID)
				require.NotNil(t, cfg.Environments["test"])
				assert.Equal(t, "my-test-gcp-project", cfg.Environments["test"].ProjectID)
				require.Len(t, cfg.Resources.MessagingTopics, 1)
				assert.Equal(t, "ingested-device-data", cfg.Resources.MessagingTopics[0].Name)
				require.Len(t, cfg.Resources.GCSBuckets, 1)
				assert.Equal(t, "iot-device-archive-bucket", cfg.Resources.GCSBuckets[0].Name)
			},
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
				require.Error(t, err, "Expected an error but got nil")
				if tc.errorContains != "" {
					assert.True(t, strings.Contains(err.Error(), tc.errorContains), "Error message should contain '%s', got '%s'", tc.errorContains, err.Error())
				}
			} else {
				require.NoError(t, err, "Expected no error but got: %v", err)
				if tc.checkConfig != nil {
					tc.checkConfig(t, cfg)
				}
			}
		})
	}
}
