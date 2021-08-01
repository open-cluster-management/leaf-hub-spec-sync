package objects

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// HubOfHubsObject holds information for objects that are read from transport layer
type HubOfHubsObject struct {
	Object  *unstructured.Unstructured `json:"object"`
	Deleted bool                       `json:"deleted"`
}
