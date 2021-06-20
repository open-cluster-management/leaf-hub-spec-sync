package bundle

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

type ObjectsBundle struct {
	Objects        []*unstructured.Unstructured `json:"objects"`
	DeletedObjects []*unstructured.Unstructured `json:"deletedObjects"`
}
