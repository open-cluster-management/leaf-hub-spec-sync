package helpers

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	metav2 "k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	controllerName      = "leaf-hub-spec-sync"
	notFoundErrorSuffix = "not found"
)

// UpdateObject function updates a given k8s object.
func UpdateObject(ctx context.Context, k8sClient client.Client, obj *unstructured.Unstructured) error {
	objectBytes, err := obj.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to update object - %w", err)
	}

	forceChanges := true

	// checks if the objects namespace already exists or not and creates it if it doesn't.
	err = createNamespaceIfNotExist(ctx, obj)
	if err != nil {
		return fmt.Errorf("failed creating new namespace - %w", err)
	}

	if err := k8sClient.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, objectBytes), &client.PatchOptions{
		FieldManager: controllerName,
		Force:        &forceChanges,
	}); err != nil {
		return fmt.Errorf("failed to update object - %w", err)
	}

	return nil
}

func createNamespaceIfNotExist(ctx context.Context, obj metav2.KMetadata) error {
	newConfig, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get config - %w", err)
	}

	clientSet, err := kubernetes.NewForConfig(newConfig)
	if err != nil {
		return fmt.Errorf("failed to get clientSet - %w", err)
	}

	nsName := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: obj.GetNamespace(),
		},
	}

	_, err = clientSet.CoreV1().Namespaces().Get(ctx, obj.GetNamespace(), metav1.GetOptions{})
	if err != nil {
		if !strings.HasSuffix(err.Error(), notFoundErrorSuffix) {
			return fmt.Errorf("failed to delete object - %w", err)
		}

		// if get didn't find the namespace then we need to create it
		helperLogger := ctrl.Log.WithName("Helper")
		helperLogger.Info(fmt.Sprintf("namespace %s for resource %s does not exist, creating new namespace.",
			obj.GetNamespace(), obj.GetName()))

		_, err = clientSet.CoreV1().Namespaces().Create(ctx, nsName, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed creating namespace - %w", err)
		}

		helperLogger.Info(fmt.Sprintf("namespace %s created", obj.GetNamespace()))
	}

	// otherwise, we found the namespace so we can return
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
