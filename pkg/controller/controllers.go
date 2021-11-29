package controller

import (
	"fmt"

	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/bundles"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddSpecSyncers adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncers(mgr ctrl.Manager, transport transport.Transport,
	bundleUpdatesChan chan *bundle.Bundle) error {
	err := bundles.AddBundleSpecSync(ctrl.Log.WithName("bundle-syncer"), mgr, transport, bundleUpdatesChan)
	if err != nil {
		return fmt.Errorf("failed to add bundles spec syncer: %w", err)
	}

	return nil
}
