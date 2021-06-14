package main

import (
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller"
	lhSyncService "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport/sync-service"
)

func main() {
	bundleUpdatesChan := make(chan *dataTypes.ObjectsBundle)
	// transport layer initialization
	syncServiceObj := lhSyncService.NewSyncService(bundleUpdatesChan)
	syncServiceObj.Start()
	defer syncServiceObj.Stop()

	specSyncController := controller.NewLeafHubSpecSync(syncServiceObj, bundleUpdatesChan)
	specSyncController.Start()
	defer specSyncController.Stop()

	// if we got here, program stopped
	close(bundleUpdatesChan)
}
