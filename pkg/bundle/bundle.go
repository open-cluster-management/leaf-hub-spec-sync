package bundle

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NewBundle creates a new instance of bundle.
func NewBundle() *Bundle {
	return &Bundle{}
}

// Bundle bundle received from transport containing received Objects/DeletedObjects.
type Bundle struct {
	Objects        []*unstructured.Unstructured `json:"objects"`
	DeletedObjects []*unstructured.Unstructured `json:"deletedObjects"`
}
