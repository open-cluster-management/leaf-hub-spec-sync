package helpers

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	controllerName           = "leaf-hub-spec-sync"
	notFoundErrorSuffix      = "not found"
	alreadyExistsErrorSuffix = "already exists"
)

// UpdateObject function updates a given k8s object.
func UpdateObject(ctx context.Context, k8sClient client.Client, obj *unstructured.Unstructured) error {
	objectBytes, err := obj.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to update object - %w", err)
	}

	forceChanges := true

	if err := k8sClient.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, objectBytes), &client.PatchOptions{
		FieldManager: controllerName,
		Force:        &forceChanges,
	}); err != nil {
		return fmt.Errorf("failed to update object - %w", err)
	}

	return nil
}

// DeleteObject tries to delete the given object from k8s. returns error and true/false if object was deleted or not.
func DeleteObject(ctx context.Context, k8sClient client.Client, obj *unstructured.Unstructured) (bool, error) {
	if err := k8sClient.Delete(ctx, obj); err != nil {
		if !strings.HasSuffix(err.Error(), notFoundErrorSuffix) {
			return false, fmt.Errorf("failed to delete object - %w", err)
		}

		return false, nil
	}

	return true, nil
}

// CreateNamespaceIfNotExist creates a namespace in case it doesn't exist.
func CreateNamespaceIfNotExist(ctx context.Context, k8sClient client.Client, namespace string) error {
	namespaceObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}

	if err := k8sClient.Create(ctx, namespaceObj); err != nil &&
		!strings.HasSuffix(err.Error(), alreadyExistsErrorSuffix) {
		return fmt.Errorf("failed to create namespace - %w", err)
	}

	return nil
}

// LabelsField presents a "f:labels" field subfield of metadataField.
type LabelsField struct {
	Labels map[string]struct{} `json:"f:labels"`
}

// MetadataField presents a "f:metadata" field subfield of v1.FieldsV1.
type MetadataField struct {
	LabelsField `json:"f:metadata"`
}
