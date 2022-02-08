package bundle

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// UnstructuredBundle bundle received from transport containing received unstructured Objects/DeletedObjects.
type UnstructuredBundle struct {
	Objects        []*unstructured.Unstructured `json:"objects"`
	DeletedObjects []*unstructured.Unstructured `json:"deletedObjects"`
}
