package servicemanager

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi"
)

// StorageManager handles the creation and deletion of storage buckets.
type StorageManager struct {
	client StorageClient // Use the interface for testability
	logger zerolog.Logger
}

// NewStorageManager creates a new StorageManager.
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
func isBucketNotExist(err error) bool {
	if errors.Is(err, Done) {
		return true
	}
	var gapiErr *googleapi.Error
	if errors.As(err, &gapiErr) && gapiErr.Code == 404 {
		return true
	}
	if err != nil && strings.Contains(err.Error(), "storage: bucket doesn't exist") {
		return true
	}
	return false
}

// Setup creates or updates storage buckets as defined in the provided resource specification.
func (sm *StorageManager) Setup(ctx context.Context, projectID, defaultLocation string, defaultLabels map[string]string, resources ResourcesSpec) error {
	sm.logger.Info().Str("project_id", projectID).Msg("Starting Storage Bucket setup")

	for _, bucketCfg := range resources.GCSBuckets {
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

			attrsToUpdate := BucketAttributesToUpdate{
				StorageClass:      &bucketCfg.StorageClass,
				VersioningEnabled: bucketCfg.VersioningEnabled,
				Labels:            finalLabels,
			}

			if len(attrsToApply.LifecycleRules) > 0 {
				attrsToUpdate.LifecycleRules = &attrsToApply.LifecycleRules
			} else if existingAttrs.LifecycleRules != nil && len(existingAttrs.LifecycleRules) > 0 {
				attrsToUpdate.LifecycleRules = &[]LifecycleRule{}
			}

			if _, updateErr := bucketHandle.Update(ctx, attrsToUpdate); updateErr != nil {
				sm.logger.Warn().Err(updateErr).Str("bucket_name", bucketCfg.Name).Msg("Failed to update bucket attributes")
			} else {
				sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket attributes updated/ensured.")
			}
		}
	}
	sm.logger.Info().Str("project_id", projectID).Msg("Storage Bucket setup completed successfully")
	return nil
}

// VerifyBuckets checks if the specified GCS buckets exist and match the configuration.
func (sm *StorageManager) VerifyBuckets(ctx context.Context, bucketsToVerify []GCSBucket) error {
	sm.logger.Info().Int("count", len(bucketsToVerify)).Msg("Verifying GCS buckets...")
	for _, bucketCfg := range bucketsToVerify {
		if bucketCfg.Name == "" {
			sm.logger.Warn().Msg("Skipping verification for bucket with empty name")
			continue
		}

		sm.logger.Debug().Str("bucket_name", bucketCfg.Name).Msg("Processing bucket configuration for verification")

		bucketHandle := sm.client.Bucket(bucketCfg.Name)
		attrs, err := bucketHandle.Attrs(ctx)

		if isBucketNotExist(err) {
			return fmt.Errorf("bucket '%s' not found during verification", bucketCfg.Name)
		} else if err != nil {
			return fmt.Errorf("failed to get attributes for bucket '%s' during verification: %w", bucketCfg.Name, err)
		}

		if bucketCfg.Location != "" && !strings.EqualFold(attrs.Location, strings.ToUpper(bucketCfg.Location)) {
			return fmt.Errorf("bucket '%s' location mismatch: expected '%s' (configured) vs '%s' (actual)", bucketCfg.Name, strings.ToUpper(bucketCfg.Location), attrs.Location)
		}
		if bucketCfg.StorageClass != "" && !strings.EqualFold(attrs.StorageClass, bucketCfg.StorageClass) {
			return fmt.Errorf("bucket '%s' storage class mismatch: expected '%s' (configured) vs '%s' (actual)", bucketCfg.Name, bucketCfg.StorageClass, attrs.StorageClass)
		}
		if attrs.VersioningEnabled != bucketCfg.VersioningEnabled {
			return fmt.Errorf("bucket '%s' versioning mismatch: expected %t (configured) vs %t (actual)", bucketCfg.Name, bucketCfg.VersioningEnabled, attrs.VersioningEnabled)
		}
		for k, v := range bucketCfg.Labels {
			if attrs.Labels == nil || attrs.Labels[k] != v {
				return fmt.Errorf("bucket '%s' label '%s' mismatch: expected '%s' (configured) vs '%s' (actual)", bucketCfg.Name, k, v, attrs.Labels[k])
			}
		}

		if len(bucketCfg.LifecycleRules) > 0 {
			if attrs.LifecycleRules == nil || len(attrs.LifecycleRules) != len(bucketCfg.LifecycleRules) {
				return fmt.Errorf("bucket '%s' lifecycle rules count mismatch: expected %d (configured) vs %d (actual)", bucketCfg.Name, len(bucketCfg.LifecycleRules), len(attrs.LifecycleRules))
			}
		} else if attrs.LifecycleRules != nil && len(attrs.LifecycleRules) > 0 {
			return fmt.Errorf("bucket '%s' has unexpected lifecycle rules (none configured, some exist)", bucketCfg.Name)
		}

		sm.logger.Debug().Str("bucket_name", bucketCfg.Name).Msg("Bucket verified successfully.")
	}
	return nil
}

// Teardown deletes storage buckets as defined in the resource specification.
func (sm *StorageManager) Teardown(ctx context.Context, resources ResourcesSpec, teardownProtection bool) error {
	sm.logger.Info().Msg("Starting Storage Bucket teardown")

	if teardownProtection {
		return fmt.Errorf("teardown protection enabled for this operation")
	}

	for i := len(resources.GCSBuckets) - 1; i >= 0; i-- {
		bucketCfg := resources.GCSBuckets[i]
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
			continue
		}

		if errDel := bucketHandle.Delete(ctx); errDel != nil {
			if strings.Contains(strings.ToLower(errDel.Error()), "not empty") {
				sm.logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket because it is not empty. Manual cleanup required.")
			} else {
				sm.logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket")
			}
		} else {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket deleted successfully")
		}
	}

	sm.logger.Info().Msg("Storage Bucket teardown completed")
	return nil
}
