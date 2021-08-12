package controller

import (
	"fmt"

	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/bundles"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddSpecSyncers adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncers(mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle,
	transport transport.Transport) error {
	err := bundles.AddLeafHubBundlesSpecSync(ctrl.Log.WithName("bundles-syncer"), mgr,
		bundleUpdatesChan, transport)
	if err != nil {
		return fmt.Errorf("failed to add bundles spec syncer: %w", err)
	}

	return nil
}
