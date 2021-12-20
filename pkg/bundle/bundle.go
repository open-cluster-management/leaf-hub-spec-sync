package bundle

import (
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NewBundle creates a new instance of bundle.
func NewBundle() *Bundle {
	return &Bundle{}
}

// Bundle is a bundle received from transport containing received Objects/DeletedObjects and transport BundleMetadata.
type Bundle struct {
	Objects        []*unstructured.Unstructured `json:"objects"`
	DeletedObjects []*unstructured.Unstructured `json:"deletedObjects"`
	transport.BundleMetadata
}

// WithMetadata adds metadata to the bundle object.
func (bundle *Bundle) WithMetadata(metadata transport.BundleMetadata) *Bundle {
	bundle.BundleMetadata = metadata
	return bundle
}
