package objects

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/helpers"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/objects"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeafHubObjectsSpecSync syncs objects spec objects
type LeafHubObjectsSpecSync struct {
	log                logr.Logger
	k8sClient          client.Client
	objectsUpdatesChan chan *objects.HubOfHubsObject
}

// AddLeafHubObjectsSpecSync adds objects spec syncer to the manager.
func AddLeafHubObjectsSpecSync(mgr ctrl.Manager, objectsUpdatesChan chan *objects.HubOfHubsObject) error {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in cluster kubeconfig - %w", err)
	}

	k8sClient, err := client.New(config, client.Options{})
	if err != nil {
		return fmt.Errorf("failed to initialize k8s client - %w", err)
	}

	return mgr.Add(&LeafHubObjectsSpecSync{
		k8sClient:          k8sClient,
		objectsUpdatesChan: objectsUpdatesChan,
	})
}

// Start function starts objects spec syncer.
func (syncer *LeafHubObjectsSpecSync) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.sync(ctx)
	for {
		<-stopChannel // blocking wait for stop event
		syncer.log.Info("stopped objects syncer")
		cancelContext()

		return nil
	}
}

func (syncer *LeafHubObjectsSpecSync) sync(ctx context.Context) {
	syncer.log.Info("start objects syncing...")
	for {
		receivedObject := <-syncer.objectsUpdatesChan
		obj := receivedObject.Object
		if !receivedObject.Deleted { // need to update the object
			if err := helpers.UpdateObject(ctx, syncer.k8sClient, obj); err != nil {
				syncer.log.Error(err, "failed to update object", "name", obj.GetName(),
					"namespace", obj.GetNamespace(), "kind", obj.GetKind())
			} else {
				syncer.log.Info("object updated", "name", obj.GetName(), "namespace",
					obj.GetNamespace(), "kind", obj.GetKind())
			}
		} else { // deleted is true, need to delete the object
			if deleted, err := helpers.DeleteObject(ctx, syncer.k8sClient, obj); err != nil {
				syncer.log.Error(err, "failed to delete object", "name", obj.GetName(),
					"namespace", obj.GetNamespace(), "kind", obj.GetKind())
			} else if deleted {
				syncer.log.Info("object deleted", "name", obj.GetName(), "namespace",
					obj.GetNamespace(), "kind", obj.GetKind())
			}
		}
	}
}
