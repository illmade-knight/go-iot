package servicemanager

import (
	"cloud.google.com/go/iam"
	"context"
	"google.golang.org/api/iterator" // For the Done error
)

// LifecycleAction represents an action in a lifecycle rule.
type LifecycleAction struct {
	Type string // e.g., "Delete"
}

// LifecycleCondition represents the conditions for a lifecycle rule.
type LifecycleCondition struct {
	AgeInDays int
	// Other common conditions can be added here (e.g., CreatedBefore, Liveness).
}

// LifecycleRule combines an action and a condition.
type LifecycleRule struct {
	Action    LifecycleAction
	Condition LifecycleCondition
}

// BucketAttributes represents the generic attributes of a storage bucket.
// It's designed to be a common denominator for providers like GCS, S3, and Azure.
type BucketAttributes struct {
	Name              string
	Location          string
	StorageClass      string
	VersioningEnabled bool
	Labels            map[string]string // AWS/Azure call these "Tags"
	LifecycleRules    []LifecycleRule
}

// BucketAttributesToUpdate represents a set of attributes to update on a bucket.
// Using pointers allows distinguishing between a field to be cleared vs. not updated.
type BucketAttributesToUpdate struct {
	StorageClass      *string
	VersioningEnabled *bool
	Labels            map[string]string // The full, desired set of labels. The adapter will compute the diff.
	LifecycleRules    *[]LifecycleRule  // Pointer to a slice to allow clearing rules with an empty slice.
}

// StorageBucketHandle defines an interface for interacting with a storage bucket.
type StorageBucketHandle interface {
	Attrs(ctx context.Context) (*BucketAttributes, error)
	Create(ctx context.Context, projectID string, attrs *BucketAttributes) error
	Update(ctx context.Context, attrs BucketAttributesToUpdate) (*BucketAttributes, error)
	Delete(ctx context.Context) error
	IAM() *iam.Handle // Note: This is GCS-specific and would need abstraction for other providers.
}

// BucketIterator defines a generic interface for iterating over storage buckets.
type BucketIterator interface {
	// Next returns the next bucket's attributes in the sequence.
	// When there are no more buckets, it should return iterator.Done.
	Next() (*BucketAttributes, error)
}

// StorageClient defines a generic interface for a storage client.
type StorageClient interface {
	Bucket(name string) StorageBucketHandle
	Buckets(ctx context.Context, projectID string) BucketIterator
	Close() error
}

// Done is a sentinel error value returned by BucketIterator.Next when the iteration is finished.
// This is re-exported from the google iterator package to avoid a direct dependency in consumers.
var Done = iterator.Done
