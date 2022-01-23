package bundles

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/bundle"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/helpers"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AddBundleSpecSync adds BundleSpecSync to the manager.
func AddBundleSpecSync(log logr.Logger, mgr ctrl.Manager, transport transport.Transport,
	bundleUpdatesChan chan *bundle.Bundle, k8sWorkerPool *k8sworkerpool.K8sWorkerPool) error {
	if err := mgr.Add(&BundleSpecSync{
		log:                          log,
		transport:                    transport,
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
	transport                    transport.Transport
	bundleUpdatesChan            chan *bundle.Bundle
	k8sWorkerPool                *k8sworkerpool.K8sWorkerPool
	bundleProcessingWaitingGroup sync.WaitGroup
}

// Start function starts bundles spec syncer.
func (syncer *BundleSpecSync) Start(ctx context.Context) error {
	syncer.log.Info("started bundles syncer...")

	go syncer.sync(ctx)

	<-ctx.Done() // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *BundleSpecSync) sync(ctx context.Context) {
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
