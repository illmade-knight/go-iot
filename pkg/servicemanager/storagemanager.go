package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi" // Note: Still used for error checking, could be abstracted further.
)

// StorageManager handles the creation and deletion of storage buckets.
type StorageManager struct {
	client StorageClient // Use the interface for testability
	logger zerolog.Logger
}

// NewStorageManager creates a new StorageManager.
// It takes a StorageClient interface, allowing for a real or mock client to be injected.
func NewStorageManager(client StorageClient, logger zerolog.Logger) (*StorageManager, error) {
	if client == nil {
		return nil, fmt.Errorf("storage client (StorageClient interface) cannot be nil")
	}
	return &StorageManager{
		client: client,
		logger: logger.With().Str("component", "StorageManager").Logger(),
	}, nil
}

// isBucketNotExist checks if an error signifies that a bucket does not exist.
// This is a helper to abstract away provider-specific error types.
func isBucketNotExist(err error) bool {
	// The generic `Done` error from our adapter handles the iterator case.
	if errors.Is(err, Done) {
		return true
	}

	// For direct lookups, we might get other errors. GCS uses storage.ErrBucketNotExist,
	// but other providers will have different errors. A more robust solution could
	// involve the adapter returning a custom, standard error type.
	// For now, we check for common patterns.
	var gapiErr *googleapi.Error
	if errors.As(err, &gapiErr) && gapiErr.Code == 404 {
		return true
	}
	// This is a direct check for the GCS error, which we can keep for the GCS implementation,
	// but a fully generic manager would ideally not know about this. We'll leave it
	// for now as a pragmatic choice until other providers are added.
	if err != nil && strings.Contains(err.Error(), "storage: bucket doesn't exist") {
		return true
	}
	return false
}

// Setup creates or updates storage buckets as defined in the configuration.
func (sm *StorageManager) Setup(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("StorageManager.Setup: %w", err)
	}
	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Storage Bucket setup")

	defaultLocation := cfg.DefaultLocation
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLocation != "" {
		defaultLocation = envSpec.DefaultLocation
	}
	var defaultLabels map[string]string
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLabels != nil {
		defaultLabels = envSpec.DefaultLabels
	}

	for _, bucketCfg := range cfg.Resources.GCSBuckets { // This config struct could also be renamed/generalized
		if bucketCfg.Name == "" {
			sm.logger.Error().Msg("Skipping bucket with empty name")
			continue
		}

		sm.logger.Debug().Str("bucket_name", bucketCfg.Name).Msg("Processing bucket configuration")

		bucketHandle := sm.client.Bucket(bucketCfg.Name)
		existingAttrs, err := bucketHandle.Attrs(ctx)

		// Combine default and bucket-specific labels
		finalLabels := make(map[string]string)
		for k, v := range defaultLabels {
			finalLabels[k] = v
		}
		for k, v := range bucketCfg.Labels {
			finalLabels[k] = v
		}

		// Use our generic BucketAttributes struct
		attrsToApply := BucketAttributes{
			Name:              bucketCfg.Name,
			StorageClass:      bucketCfg.StorageClass,
			VersioningEnabled: bucketCfg.VersioningEnabled,
			Labels:            finalLabels,
		}

		if bucketCfg.Location != "" {
			attrsToApply.Location = strings.ToUpper(bucketCfg.Location)
		} else if defaultLocation != "" {
			attrsToApply.Location = strings.ToUpper(defaultLocation)
		} else {
			sm.logger.Warn().Str("bucket_name", bucketCfg.Name).Msg("Bucket location not specified, relying on provider defaults.")
		}

		if len(bucketCfg.LifecycleRules) > 0 {
			attrsToApply.LifecycleRules = make([]LifecycleRule, 0, len(bucketCfg.LifecycleRules))
			for _, ruleSpec := range bucketCfg.LifecycleRules {
				attrsToApply.LifecycleRules = append(attrsToApply.LifecycleRules, LifecycleRule{
					Action:    LifecycleAction{Type: ruleSpec.Action.Type},
					Condition: LifecycleCondition{AgeInDays: ruleSpec.Condition.AgeDays},
				})
			}
		}

		if isBucketNotExist(err) {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Str("location", attrsToApply.Location).Msg("Bucket not found, creating...")
			if errCreate := bucketHandle.Create(ctx, projectID, &attrsToApply); errCreate != nil {
				return fmt.Errorf("failed to create bucket '%s': %w", bucketCfg.Name, errCreate)
			}
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket created successfully")
		} else if err != nil {
			return fmt.Errorf("failed to get attributes for bucket '%s': %w", bucketCfg.Name, err)
		} else {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket already exists. Ensuring configuration...")

			// Use our generic BucketAttributesToUpdate struct
			attrsToUpdate := BucketAttributesToUpdate{
				StorageClass:      &bucketCfg.StorageClass,
				VersioningEnabled: bucketCfg.VersioningEnabled,
				Labels:            finalLabels, // Pass the full desired state
			}

			if len(attrsToApply.LifecycleRules) > 0 {
				attrsToUpdate.LifecycleRules = &attrsToApply.LifecycleRules
			} else if existingAttrs.LifecycleRules != nil && len(existingAttrs.LifecycleRules) > 0 {
				// If config has no rules, but bucket has, clear them by setting an empty slice.
				attrsToUpdate.LifecycleRules = &[]LifecycleRule{}
			}

			if _, updateErr := bucketHandle.Update(ctx, attrsToUpdate); updateErr != nil {
				// It's a warning because failing to update a bucket might not be a fatal error for the setup process.
				sm.logger.Warn().Err(updateErr).Str("bucket_name", bucketCfg.Name).Msg("Failed to update bucket attributes")
			} else {
				sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket attributes updated/ensured.")
			}
		}
	}
	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Storage Bucket setup completed successfully")
	return nil
}

// Teardown deletes storage buckets as defined in the configuration.
func (sm *StorageManager) Teardown(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("StorageManager.Teardown: %w", err)
	}
	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Storage Bucket teardown")

	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		sm.logger.Error().Str("environment", environment).Msg("Teardown protection is enabled for this environment. Manual intervention required.")
		return fmt.Errorf("teardown protection enabled for environment: %s", environment)
	}

	for i := len(cfg.Resources.GCSBuckets) - 1; i >= 0; i-- { // Delete in reverse order
		bucketCfg := cfg.Resources.GCSBuckets[i]
		if bucketCfg.Name == "" {
			sm.logger.Warn().Msg("Skipping bucket with empty name during teardown")
			continue
		}

		bucketHandle := sm.client.Bucket(bucketCfg.Name)
		sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Attempting to delete bucket...")

		if _, errAttr := bucketHandle.Attrs(ctx); isBucketNotExist(errAttr) {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket does not exist, skipping deletion.")
			continue
		} else if errAttr != nil {
			sm.logger.Error().Err(errAttr).Str("bucket_name", bucketCfg.Name).Msg("Failed to get attributes before delete, skipping.")
			continue // Skip this bucket on error
		}

		if errDel := bucketHandle.Delete(ctx); errDel != nil {
			// This error check is generic enough to likely work across providers.
			if strings.Contains(strings.ToLower(errDel.Error()), "not empty") {
				sm.logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket because it is not empty. Manual cleanup required.")
			} else {
				sm.logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket")
			}
			// Continue to the next bucket even if one fails.
		} else {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket deleted successfully")
		}
	}

	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Storage Bucket teardown completed")
	return nil
}
