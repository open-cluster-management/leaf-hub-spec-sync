package bundles

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/helpers"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeafHubBundlesSpecSync syncs bundles spec objects.
type LeafHubBundlesSpecSync struct {
	log                    logr.Logger
	bundleUpdatesChan      chan *bundle.ObjectsBundle
	k8sClients             []client.Client
	clientWorkersJobChan   chan *clientWorkerJob
	clientWorkersWaitGroup sync.WaitGroup
}

// handlerFunc is a clientWorkerJob's handler function.
type handlerFunc func(context.Context, client.Client, *unstructured.Unstructured)

// clientWorkerJob holds the object than need to be processed and the flag to which defines
// whether object need to be updated or delete.
type clientWorkerJob struct {
	handler handlerFunc
	obj     *unstructured.Unstructured
}

// AddLeafHubBundlesSpecSync adds bundles spec syncer to the manager.
func AddLeafHubBundlesSpecSync(log logr.Logger, mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle,
	numOfClients int) error {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in cluster kubeconfig - %w", err)
	}

	// prepare k8s clients
	k8sClients := make([]client.Client, numOfClients)

	for i := 0; i < numOfClients; i++ {
		k8sClient, err := client.New(config, client.Options{})
		if err != nil {
			return fmt.Errorf("failed to initialize k8s client - %w", err)
		}

		k8sClients[i] = k8sClient
	}

	// create client workers job channel
	clientWorkersJobChan := make(chan *clientWorkerJob, numOfClients)

	if err := mgr.Add(&LeafHubBundlesSpecSync{
		log:                  log,
		bundleUpdatesChan:    bundleUpdatesChan,
		k8sClients:           k8sClients,
		clientWorkersJobChan: clientWorkersJobChan,
	}); err != nil {
		return fmt.Errorf("failed to add bundles spec syncer - %w", err)
	}

	return nil
}

// Start function starts bundles spec syncer.
func (syncer *LeafHubBundlesSpecSync) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	// start workers
	for i := 0; i < len(syncer.k8sClients); i++ {
		go syncer.runClientWorker(ctx, syncer.k8sClients[i])
	}

	go syncer.sync(ctx)

	<-stopChannel // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *LeafHubBundlesSpecSync) sync(ctx context.Context) {
	syncer.log.Info("start bundles syncing...")

	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case receivedBundle := <-syncer.bundleUpdatesChan: // handle the bundle
			syncer.clientWorkersWaitGroup.Add(len(receivedBundle.Objects))

			// send "update" jobs to client workers
			for _, obj := range receivedBundle.Objects {
				syncer.clientWorkersJobChan <- &clientWorkerJob{handler: syncer.updateObject, obj: obj}
			}

			// ensure all updates have finished before processing DeletedObjects objects
			syncer.clientWorkersWaitGroup.Wait()

			syncer.clientWorkersWaitGroup.Add(len(receivedBundle.DeletedObjects))

			// send "delete" jobs to client workers
			for _, obj := range receivedBundle.DeletedObjects {
				syncer.clientWorkersJobChan <- &clientWorkerJob{handler: syncer.deleteObject, obj: obj}
			}

			// ensure all deletes have finished before receiving next bundle
			syncer.clientWorkersWaitGroup.Wait()
		}
	}
}

func (syncer *LeafHubBundlesSpecSync) runClientWorker(ctx context.Context, k8sClient client.Client) {
	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case job := <-syncer.clientWorkersJobChan: // handle the object
			job.handler(ctx, k8sClient, job.obj)
			syncer.clientWorkersWaitGroup.Done()
		}
	}
}

func (syncer *LeafHubBundlesSpecSync) updateObject(ctx context.Context, k8sClient client.Client,
	obj *unstructured.Unstructured) {
	if err := helpers.UpdateObject(ctx, k8sClient, obj); err != nil {
		syncer.log.Error(err, "failed to update object", "name", obj.GetName(),
			"namespace", obj.GetNamespace(), "kind", obj.GetKind())
	} else {
		syncer.log.Info("object updated", "name", obj.GetName(), "namespace",
			obj.GetNamespace(), "kind", obj.GetKind())
	}
}

func (syncer *LeafHubBundlesSpecSync) deleteObject(ctx context.Context, k8sClient client.Client,
	obj *unstructured.Unstructured) {
	if deleted, err := helpers.DeleteObject(ctx, k8sClient, obj); err != nil {
		syncer.log.Error(err, "failed to delete object", "name", obj.GetName(),
			"namespace", obj.GetNamespace(), "kind", obj.GetKind())
	} else if deleted {
		syncer.log.Info("object deleted", "name", obj.GetName(), "namespace",
			obj.GetNamespace(), "kind", obj.GetKind())
	}
}
