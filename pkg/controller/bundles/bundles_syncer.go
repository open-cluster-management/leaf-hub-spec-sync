package bundles

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/helpers"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const numOfClients = 10

// LeafHubBundlesSpecSync syncs bundles spec objects.
type LeafHubBundlesSpecSync struct {
	log                 logr.Logger
	bundleUpdatesChan   chan *bundle.ObjectsBundle
	k8sClients          []client.Client
	clientWorkerJobChan chan *clientWorkerJob
}

// handlerFunc is a clientWorkerJob's handler function.
type handlerFunc func(context.Context, client.Client, *unstructured.Unstructured)

// clientWorkerJob holds the object than need to be processed and the flag to which defines
// whether object need to be updated or delete.
type clientWorkerJob struct {
	handler handlerFunc
	obj     *unstructured.Unstructured
	wg      *sync.WaitGroup
}

// AddLeafHubBundlesSpecSync adds bundles spec syncer to the manager.
func AddLeafHubBundlesSpecSync(log logr.Logger, mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle) error {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in cluster kubeconfig - %w", err)
	}

	// prepare k8s clients
	k8sClients := make([]client.Client, 0)

	for i := 0; i < numOfClients; i++ {
		k8sClient, err := client.New(config, client.Options{})
		if err != nil {
			return fmt.Errorf("failed to initialize k8s client - %w", err)
		}

		k8sClients = append(k8sClients, k8sClient)
	}

	// create object data channel
	clientWorkerJobsChan := make(chan *clientWorkerJob, numOfClients)

	if err := mgr.Add(&LeafHubBundlesSpecSync{
		log:                 log,
		bundleUpdatesChan:   bundleUpdatesChan,
		k8sClients:          k8sClients,
		clientWorkerJobChan: clientWorkerJobsChan,
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
	for i := 0; i < numOfClients; i++ {
		go syncer.runClientWorker(ctx, syncer.k8sClients[i])
	}

	go syncer.sync()

	<-stopChannel // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *LeafHubBundlesSpecSync) sync() {
	var wg sync.WaitGroup

	syncer.log.Info("start bundles syncing...")

	for {
		receivedBundle := <-syncer.bundleUpdatesChan

		nowUpdated := time.Now()

		wg.Add(len(receivedBundle.Objects))

		// send "update" jobs to client workers
		for _, obj := range receivedBundle.Objects {
			syncer.clientWorkerJobChan <- &clientWorkerJob{
				handler: func(ctx context.Context, k8sClient client.Client, obj *unstructured.Unstructured) {
					if err := helpers.UpdateObject(ctx, k8sClient, obj); err != nil {
						syncer.log.Error(err, "failed to update object", "name", obj.GetName(),
							"namespace", obj.GetNamespace(), "kind", obj.GetKind())
					} else {
						syncer.log.Info("object updated", "name", obj.GetName(), "namespace",
							obj.GetNamespace(), "kind", obj.GetKind())
					}
				}, obj: obj, wg: &wg,
			}
		}

		wg.Wait()

		updateDuration := time.Since(nowUpdated)

		nowDeleted := time.Now()

		wg.Add(len(receivedBundle.DeletedObjects))

		// send "delete" jobs to client workers
		for _, obj := range receivedBundle.DeletedObjects {
			syncer.clientWorkerJobChan <- &clientWorkerJob{
				handler: func(ctx context.Context, k8sClient client.Client, obj *unstructured.Unstructured) {
					if deleted, err := helpers.DeleteObject(ctx, k8sClient, obj); err != nil {
						syncer.log.Error(err, "failed to delete object", "name", obj.GetName(),
							"namespace", obj.GetNamespace(), "kind", obj.GetKind())
					} else if deleted {
						syncer.log.Info("object deleted", "name", obj.GetName(), "namespace",
							obj.GetNamespace(), "kind", obj.GetKind())
					}
				}, obj: obj, wg: &wg,
			}
		}

		wg.Wait()

		deleteDuration := time.Since(nowDeleted)

		syncer.log.Info("multiple go routines", fmt.Sprintf("%d objects updated", len(receivedBundle.Objects)),
			fmt.Sprintf("%d ms", updateDuration.Milliseconds()), fmt.Sprintf("%d objects deleted",
				len(receivedBundle.DeletedObjects)), fmt.Sprintf("%d ms", deleteDuration.Milliseconds()))
	}
}

func (syncer *LeafHubBundlesSpecSync) runClientWorker(ctx context.Context, k8sClient client.Client) {
	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			close(syncer.clientWorkerJobChan)
			return

		case job := <-syncer.clientWorkerJobChan: // handle the object
			job.handler(ctx, k8sClient, job.obj)
			job.wg.Done()
		}
	}
}
