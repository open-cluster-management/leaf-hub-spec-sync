package bundles

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/bundle"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/helpers"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AddUnstructuredBundleSyncer adds UnstructuredBundleSyncer to the manager.
func AddUnstructuredBundleSyncer(log logr.Logger, mgr ctrl.Manager, transport transport.Transport,
	k8sWorkerPool *k8sworkerpool.K8sWorkerPool) error {
	bundleUpdatesChan := make(chan interface{})

	if err := mgr.Add(&UnstructuredBundleSyncer{
		log:                          log,
		bundleUpdatesChan:            bundleUpdatesChan,
		k8sWorkerPool:                k8sWorkerPool,
		bundleProcessingWaitingGroup: sync.WaitGroup{},
	}); err != nil {
		close(bundleUpdatesChan)
		return fmt.Errorf("failed to add unstructured bundles spec syncer - %w", err)
	}

	unstructuredSpecBundlesKeys := []string{
		datatypes.Config,
		datatypes.PoliciesMsgKey,
		datatypes.PlacementRulesMsgKey,
		datatypes.PlacementBindingsMsgKey,
	}

	for _, unstructuredSpecBundleKey := range unstructuredSpecBundlesKeys {
		transport.Register(unstructuredSpecBundleKey, bundleUpdatesChan)
	}

	return nil
}

// UnstructuredBundleSyncer syncs objects spec from received bundles.
type UnstructuredBundleSyncer struct {
	log                          logr.Logger
	bundleUpdatesChan            chan interface{}
	k8sWorkerPool                *k8sworkerpool.K8sWorkerPool
	bundleProcessingWaitingGroup sync.WaitGroup
}

// Start function starts bundles spec syncer.
func (syncer *UnstructuredBundleSyncer) Start(ctx context.Context) error {
	syncer.log.Info("started bundles syncer...")

	go syncer.sync(ctx)

	<-ctx.Done() // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *UnstructuredBundleSyncer) sync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case transportedBundle := <-syncer.bundleUpdatesChan: // handle the bundle
			receivedBundle, ok := transportedBundle.(*bundle.UnstructuredBundle)
			if !ok {
				continue
			}

			syncer.bundleProcessingWaitingGroup.Add(len(receivedBundle.Objects) + len(receivedBundle.DeletedObjects))
			// send k8s jobs to workers to update objects
			for _, obj := range receivedBundle.Objects {
				syncer.k8sWorkerPool.RunAsync(k8sworkerpool.NewK8sJob(obj, func(ctx context.Context,
					k8sClient client.Client, obj interface{}) {
					syncer.updateObject(ctx, k8sClient, obj.(*unstructured.Unstructured))
					syncer.bundleProcessingWaitingGroup.Done()
				}))
			}
			// send k8s jobs to workers to delete objects
			for _, obj := range receivedBundle.DeletedObjects {
				syncer.k8sWorkerPool.RunAsync(k8sworkerpool.NewK8sJob(obj, func(ctx context.Context,
					k8sClient client.Client, obj interface{}) {
					syncer.deleteObject(ctx, k8sClient, obj.(*unstructured.Unstructured))
					syncer.bundleProcessingWaitingGroup.Done()
				}))
			}
			// ensure all updates and deletes have finished before reading next bundle
			syncer.bundleProcessingWaitingGroup.Wait()
		}
	}
}

func (syncer *UnstructuredBundleSyncer) updateObject(ctx context.Context, k8sClient client.Client,
	obj *unstructured.Unstructured) {
	if err := helpers.UpdateObject(ctx, k8sClient, obj); err != nil {
		syncer.log.Error(err, "failed to update object", "name", obj.GetName(), "namespace",
			obj.GetNamespace(), "kind", obj.GetKind())
	} else {
		syncer.log.Info("object updated", "name", obj.GetName(), "namespace", obj.GetNamespace(),
			"kind", obj.GetKind())
	}
}

func (syncer *UnstructuredBundleSyncer) deleteObject(ctx context.Context, k8sClient client.Client,
	obj *unstructured.Unstructured) {
	if deleted, err := helpers.DeleteObject(ctx, k8sClient, obj); err != nil {
		syncer.log.Error(err, "failed to delete object", "name", obj.GetName(), "namespace",
			obj.GetNamespace(), "kind", obj.GetKind())
	} else if deleted {
		syncer.log.Info("object deleted", "name", obj.GetName(), "namespace", obj.GetNamespace(),
			"kind", obj.GetKind())
	}
}
