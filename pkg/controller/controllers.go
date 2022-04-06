package controller

import (
	"fmt"

	clusterv1 "github.com/open-cluster-management/api/cluster/v1"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/bundle"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/bundles"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	clusterv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddToScheme adds all Resources to the Scheme.
func AddToScheme(runtimeScheme *runtime.Scheme) error {
	schemeInstallationFuncs := []func(*runtime.Scheme) error{
		clusterv1alpha1.Install, clusterv1beta1.Install, clusterv1.Install,
	}

	for _, schemeInstallationFunc := range schemeInstallationFuncs {
		if err := schemeInstallationFunc(runtimeScheme); err != nil {
			return fmt.Errorf("failed to install scheme: %w", err)
		}
	}

	return nil
}

// AddSpecSyncer adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncer(mgr ctrl.Manager, transportObj transport.Transport,
	objectsBundleUpdatesChan chan *bundle.GenericBundle) error {
	k8sWorkerPool, err := k8sworkerpool.AddK8sWorkerPool(ctrl.Log.WithName("k8s-workers-pool"), mgr)
	if err != nil {
		return fmt.Errorf("failed to add k8s workers pool to runtime manager: %w", err)
	}

	// add generic syncer to mgr
	if err = bundles.AddGenericBundleSyncer(ctrl.Log.WithName("generic-bundle-syncer"), mgr,
		objectsBundleUpdatesChan, transportObj, k8sWorkerPool); err != nil {
		return fmt.Errorf("failed to add bundles spec syncer to runtime manager: %w", err)
	}
	// add managed cluster labels syncer to mgr
	if err = bundles.AddManagedClusterLabelsBundleSyncer(ctrl.Log.WithName("managed-clusters-metadata-syncer"), mgr,
		transportObj, k8sWorkerPool); err != nil {
		return fmt.Errorf("failed to add bundles spec syncer to runtime manager: %w", err)
	}

	return nil
}
