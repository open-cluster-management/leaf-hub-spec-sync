package controller

import (
	"fmt"

	clustersv1 "github.com/open-cluster-management/api/cluster/v1"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/bundle"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/bundles"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddToScheme adds all Resources to the Scheme.
func AddToScheme(s *runtime.Scheme) error {
	if err := clustersv1.Install(s); err != nil {
		return fmt.Errorf("failed to install scheme: %w", err)
	}

	return nil
}

// AddSpecSyncers adds the controllers that get updates from transport layer to the Manager.
func AddSpecSyncers(mgr ctrl.Manager, transportObj transport.Transport,
	genericBundleUpdatesChan chan *bundle.GenericBundle) error {
	k8sWorkerPool, err := k8sworkerpool.AddK8sWorkerPool(ctrl.Log.WithName("k8s-workers-pool"), mgr)
	if err != nil {
		return fmt.Errorf("failed to add k8s workers pool to runtime manager: %w", err)
	}

	// add generic syncer to mgr
	if err = bundles.AddGenericBundleSyncer(ctrl.Log.WithName("generic-bundle-syncer"), mgr,
		genericBundleUpdatesChan, k8sWorkerPool); err != nil {
		return fmt.Errorf("failed to add bundles spec syncer to runtime manager: %w", err)
	}
	// add managed cluster labels syncer to mgr
	if err = bundles.AddManagedClusterLabelsBundleSyncer(ctrl.Log.WithName("managed-clusters-labels-syncer"), mgr,
		transportObj, k8sWorkerPool); err != nil {
		return fmt.Errorf("failed to add managed cluster labels syncer to runtime manager: %w", err)
	}

	return nil
}
