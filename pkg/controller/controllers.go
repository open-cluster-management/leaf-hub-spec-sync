package controller

import (
	"fmt"

	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/bundles"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddSpecSyncers adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncers(mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle) error {
	err := bundles.AddBundleSpecSync(ctrl.Log.WithName("bundle-syncer"), mgr, bundleUpdatesChan)
	if err != nil {
		return fmt.Errorf("failed to add bundles spec syncer: %w", err)
	}

	return nil
}
