package bundles

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/helpers"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeafHubBundlesSpecSync syncs bundles spec objects
type LeafHubBundlesSpecSync struct {
	log               logr.Logger
	k8sClient         client.Client
	bundleUpdatesChan chan *bundle.ObjectsBundle
}

// AddLeafHubBundlesSpecSync adds bundles spec syncer to the manager.
func AddLeafHubBundlesSpecSync(mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle) error {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in cluster kubeconfig - %w", err)
	}

	k8sClient, err := client.New(config, client.Options{})
	if err != nil {
		return fmt.Errorf("failed to initialize k8s client - %w", err)
	}

	return mgr.Add(&LeafHubBundlesSpecSync{
		k8sClient:         k8sClient,
		bundleUpdatesChan: bundleUpdatesChan,
	})
}

// Start function starts bundles spec syncer.
func (syncer *LeafHubBundlesSpecSync) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.sync(ctx)
	for {
		<-stopChannel // blocking wait for stop event
		syncer.log.Info("stopped bundles syncer")
		cancelContext()

		return nil
	}
}

func (syncer *LeafHubBundlesSpecSync) sync(ctx context.Context) {
	syncer.log.Info("start bundles syncing...")
	for {
		receivedBundle := <-syncer.bundleUpdatesChan
		for _, obj := range receivedBundle.Objects {
			if err := helpers.UpdateObject(ctx, syncer.k8sClient, obj); err != nil {
				syncer.log.Error(err, "failed to update object", "name", obj.GetName(),
					"namespace", obj.GetNamespace(), "kind", obj.GetKind())
			} else {
				syncer.log.Info("object updated", "name", obj.GetName(), "namespace",
					obj.GetNamespace(), "kind", obj.GetKind())
			}
		}
		for _, obj := range receivedBundle.DeletedObjects {
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
