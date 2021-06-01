package main

import (
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller"
	dataTypes "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/data-types"
	lhSyncService "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport/sync-service"
)

func main() {
	policiesUpdateChan := make(chan *dataTypes.PoliciesBundle)
	// transport layer initialization
	syncServiceObj := lhSyncService.NewSyncService(policiesUpdateChan)
	syncServiceObj.Start()
	defer syncServiceObj.Stop()

	specSyncController := controller.NewLeafHubSpecSync(syncServiceObj, policiesUpdateChan)
	specSyncController.Start()
}