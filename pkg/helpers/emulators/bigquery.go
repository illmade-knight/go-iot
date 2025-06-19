package emulators

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"net/http"
	"strings"
	"testing"
	"time"
)

type BigQueryConfig struct {
	GCImageContainer
	DatasetTables map[string]string
	Schemas       map[string]interface{}
}

const (
	testBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testBigQueryGRPCPort      = "9060"
	testBigQueryRestPort      = "9050"
)

func GetDefaultBigQueryConfig(projectID string, datasetTables map[string]string, schemaMappings map[string]interface{}) BigQueryConfig {
	return BigQueryConfig{
		GCImageContainer: GCImageContainer{
			ImageContainer: ImageContainer{
				EmulatorImage:    testBigQueryEmulatorImage,
				EmulatorHTTPPort: testBigQueryRestPort,
				EmulatorGRPCPort: testBigQueryGRPCPort,
			},
			ProjectID:       projectID,
			SetEnvVariables: false,
		},
		DatasetTables: datasetTables,
		Schemas:       schemaMappings,
	}
}

func SetupBigQueryEmulator(t *testing.T, ctx context.Context, cfg BigQueryConfig) (opts []option.ClientOption, cleanupFunc func()) {
	t.Helper()
	httpPort := fmt.Sprintf("%s/tcp", cfg.EmulatorHTTPPort)
	grpcPort := fmt.Sprintf("%s/tcp", cfg.EmulatorGRPCPort)
	req := testcontainers.ContainerRequest{
		Image:        cfg.EmulatorImage,
		ExposedPorts: []string{httpPort, grpcPort},
		Cmd: []string{
			"--project=" + cfg.ProjectID,
			"--port=" + cfg.EmulatorHTTPPort,
			"--grpc-port=" + cfg.EmulatorGRPCPort,
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(nat.Port(httpPort)).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(nat.Port(grpcPort)).WithStartupTimeout(60*time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	mappedGrpcPort, err := container.MappedPort(ctx, nat.Port(grpcPort))
	require.NoError(t, err)
	mappedRestPort, err := container.MappedPort(ctx, nat.Port(httpPort))
	require.NoError(t, err)

	endpoint := fmt.Sprintf("http://%s:%s", host, mappedRestPort.Port())

	opts = []option.ClientOption{option.WithEndpoint(endpoint), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{})}

	if cfg.SetEnvVariables {
		t.Setenv("BIGQUERY_EMULATOR_HOST", fmt.Sprintf("%s:%s", host, mappedGrpcPort.Port()))
		t.Setenv("BIGQUERY_API_ENDPOINT", endpoint)
	}

	client, err := bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	require.NoError(t, err)
	defer client.Close()

	for k, v := range cfg.DatasetTables {
		err = client.Dataset(k).Create(ctx, &bigquery.DatasetMetadata{Name: k})
		if err != nil && !strings.Contains(err.Error(), "Already Exists") {
			require.NoError(t, err)
		}

		table := client.Dataset(k).Table(v)
		schemaType, ok := cfg.Schemas[v]
		require.True(t, ok)
		schema, err := bigquery.InferSchema(schemaType)
		require.NoError(t, err)
		err = table.Create(ctx, &bigquery.TableMetadata{Name: v, Schema: schema})
		if err != nil && !strings.Contains(err.Error(), "Already Exists") {
			require.NoError(t, err)
		}
	}

	return opts, func() { require.NoError(t, container.Terminate(ctx)) }
}
