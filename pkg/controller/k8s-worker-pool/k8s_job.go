package k8sworkerpool

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sJobHandlerFunc is a function for running a k8s job by a k8s worker.
type K8sJobHandlerFunc func(context.Context, client.Client, *unstructured.Unstructured)

// NewK8sJob creates a new instance of K8sJob.
func NewK8sJob(obj *unstructured.Unstructured, handlerFunc K8sJobHandlerFunc) *K8sJob {
	return &K8sJob{
		obj:         obj,
		handlerFunc: handlerFunc,
	}
}

// K8sJob represents the job to be run by a k8sWorker from the pool.
type K8sJob struct {
	obj         *unstructured.Unstructured
	handlerFunc K8sJobHandlerFunc
}
