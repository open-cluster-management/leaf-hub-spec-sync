package controller

import (
	"fmt"

	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/bundles"
	objectsCtrl "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/objects"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/objects"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddSpecSyncers adds the controllers that get updates from transport layer and apply/delete CRs to the Manager.
func AddSpecSyncers(mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle,
	objectUpdatesChan chan *objects.HubOfHubsObject) error {
	err := bundles.AddLeafHubBundlesSpecSync(ctrl.Log.WithName("bundles-syncer"), mgr, bundleUpdatesChan)
	if err != nil {
		return fmt.Errorf("failed to add bundles spec syncer: %w", err)
	}

	err = objectsCtrl.AddLeafHubObjectsSpecSync(ctrl.Log.WithName("objects-syncer"), mgr, objectUpdatesChan)
	if err != nil {
		return fmt.Errorf("failed to add objects spec syncer: %w", err)
	}

	return nil
}
