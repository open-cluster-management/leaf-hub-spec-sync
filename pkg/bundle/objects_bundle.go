package bundle

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// ObjectsBundle holds information for bundles that are read from transport layer.
type ObjectsBundle struct {
	Objects        []*unstructured.Unstructured `json:"objects"`
	DeletedObjects []*unstructured.Unstructured `json:"deletedObjects"`
}
