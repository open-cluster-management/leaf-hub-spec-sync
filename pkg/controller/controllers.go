package controller

import (
	"fmt"

	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/bundles"
	k8sworkerpool "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddSpecSyncer adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncer(mgr ctrl.Manager, transport transport.Transport,
	bundleUpdatesChan chan *bundle.Bundle) error {
	k8sWorkerPool, err := k8sworkerpool.AddK8sWorkerPool(ctrl.Log.WithName("k8s-workers-pool"), mgr)
	if err != nil {
		return fmt.Errorf("failed to add k8s workers pool to runtime manager: %w", err)
	}

	if err = bundles.AddBundleSpecSync(ctrl.Log.WithName("bundle-syncer"), mgr, transport, bundleUpdatesChan,
		k8sWorkerPool); err != nil {
		return fmt.Errorf("failed to add bundles spec syncer to runtime manager: %w", err)
	}

	return nil
}
