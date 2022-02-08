package controller

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/bundles"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddSpecSyncer adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncer(mgr ctrl.Manager, transportObj transport.Transport) error {
	k8sWorkerPool, err := k8sworkerpool.AddK8sWorkerPool(ctrl.Log.WithName("k8s-workers-pool"), mgr)
	if err != nil {
		return fmt.Errorf("failed to add k8s workers pool to runtime manager: %w", err)
	}

	addControllerFunctions := []struct {
		addControllerFunction func(logr.Logger, ctrl.Manager, transport.Transport, *k8sworkerpool.K8sWorkerPool) error
		loggerName            string
	}{
		{bundles.AddUnstructuredBundleSyncer, "unstructured-bundle-syncer"},
		{bundles.AddManagedClustersMetadataBundleSyncer, "managed-clusters-metadata-syncer"},
	}

	for _, controllerInitInfo := range addControllerFunctions {
		if err = controllerInitInfo.addControllerFunction(ctrl.Log.WithName(controllerInitInfo.loggerName), mgr,
			transportObj, k8sWorkerPool); err != nil {
			return fmt.Errorf("failed to add bundles spec syncer to runtime manager: %w", err)
		}
	}

	return nil
}
