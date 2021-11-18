package bundles

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/helpers"
	k8sworkerpool "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AddBundleSpecSync adds BundleSpecSync to the manager.
func AddBundleSpecSync(log logr.Logger, mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle) error {
	// create k8s worker pool
	k8sWorkerPool, err := k8sworkerpool.NewK8sWorkerPool(log)
	if err != nil {
		return fmt.Errorf("failed to initialize bundle spec syncer - %w", err)
	}

	if err := mgr.Add(&BundleSpecSync{
		log:                          log,
		bundleUpdatesChan:            bundleUpdatesChan,
		k8sWorkerPool:                k8sWorkerPool,
		bundleProcessingWaitingGroup: sync.WaitGroup{},
	}); err != nil {
		return fmt.Errorf("failed to add bundles spec syncer - %w", err)
	}

	return nil
}

// BundleSpecSync syncs objects spec from received bundles.
type BundleSpecSync struct {
	log                          logr.Logger
	bundleUpdatesChan            chan *bundle.ObjectsBundle
	k8sWorkerPool                *k8sworkerpool.K8sWorkerPool
	bundleProcessingWaitingGroup sync.WaitGroup
}

// Start function starts bundles spec syncer.
func (syncer *BundleSpecSync) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	defer syncer.k8sWorkerPool.Stop() // verify that if pool start fails at any point it will get cleaned

	if err := syncer.k8sWorkerPool.Start(); err != nil {
		return fmt.Errorf("failed to start bundles spec syncer - %w", err)
	}

	go syncer.sync(ctx)

	<-stopChannel // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *BundleSpecSync) sync(ctx context.Context) {
	syncer.log.Info("start bundles syncing...")

	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case receivedBundle := <-syncer.bundleUpdatesChan: // handle the bundle
			syncer.bundleProcessingWaitingGroup.Add(len(receivedBundle.Objects) + len(receivedBundle.DeletedObjects))
			// send k8s jobs to workers to update objects
			for _, obj := range receivedBundle.Objects {
				syncer.k8sWorkerPool.RunAsync(k8sworkerpool.NewK8sJob(obj, func(ctx context.Context,
					k8sClient client.Client, obj *unstructured.Unstructured) {
					syncer.updateObject(ctx, k8sClient, obj)
					syncer.bundleProcessingWaitingGroup.Done()
				}))
			}
			// send k8s jobs to workers to delete objects
			for _, obj := range receivedBundle.DeletedObjects {
				syncer.k8sWorkerPool.RunAsync(k8sworkerpool.NewK8sJob(obj, func(ctx context.Context,
					k8sClient client.Client, obj *unstructured.Unstructured) {
					syncer.deleteObject(ctx, k8sClient, obj)
					syncer.bundleProcessingWaitingGroup.Done()
				}))
			}
			// ensure all updates and deletes have finished before reading next bundle
			syncer.bundleProcessingWaitingGroup.Wait()
		}
	}
}

func (syncer *BundleSpecSync) updateObject(ctx context.Context, k8sClient client.Client,
	obj *unstructured.Unstructured) {
	if err := helpers.UpdateObject(ctx, k8sClient, obj); err != nil {
		syncer.log.Error(err, "failed to update object", "name", obj.GetName(), "namespace",
			obj.GetNamespace(), "kind", obj.GetKind())
	} else {
		syncer.log.Info("object updated", "name", obj.GetName(), "namespace", obj.GetNamespace(),
			"kind", obj.GetKind())
	}
}

func (syncer *BundleSpecSync) deleteObject(ctx context.Context, k8sClient client.Client,
	obj *unstructured.Unstructured) {
	if deleted, err := helpers.DeleteObject(ctx, k8sClient, obj); err != nil {
		syncer.log.Error(err, "failed to delete object", "name", obj.GetName(), "namespace",
			obj.GetNamespace(), "kind", obj.GetKind())
	} else if deleted {
		syncer.log.Info("object deleted", "name", obj.GetName(), "namespace", obj.GetNamespace(),
			"kind", obj.GetKind())
	}
}
