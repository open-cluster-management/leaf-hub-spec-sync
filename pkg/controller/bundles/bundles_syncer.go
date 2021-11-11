package bundles

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller/helpers"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	envVarK8sClientsPoolSize  = "K8S_CLIENTS_POOL_SIZE"
	defaultK8sClientsPoolSize = 10
)

// LeafHubBundlesSpecSync syncs bundles spec objects.
type LeafHubBundlesSpecSync struct {
	log                    logr.Logger
	bundleUpdatesChan      chan *bundle.ObjectsBundle
	k8sClientsPool         []client.Client
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
func AddLeafHubBundlesSpecSync(log logr.Logger, mgr ctrl.Manager, bundleUpdatesChan chan *bundle.ObjectsBundle) error {
	k8sClientsPoolSize := readK8sPoolSizeEnvVar(log)

	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in cluster kubeconfig - %w", err)
	}

	// prepare k8s clients pool
	k8sClientsPool := make([]client.Client, k8sClientsPoolSize)

	for i := 0; i < k8sClientsPoolSize; i++ {
		k8sClient, err := client.New(config, client.Options{})
		if err != nil {
			return fmt.Errorf("failed to initialize k8s client - %w", err)
		}

		k8sClientsPool[i] = k8sClient
	}

	// create client workers job channel
	clientWorkersJobChan := make(chan *clientWorkerJob, k8sClientsPoolSize)

	if err := mgr.Add(&LeafHubBundlesSpecSync{
		log:                  log,
		bundleUpdatesChan:    bundleUpdatesChan,
		k8sClientsPool:       k8sClientsPool,
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
	for i := 0; i < len(syncer.k8sClientsPool); i++ {
		go syncer.runClientWorker(ctx, syncer.k8sClientsPool[i])
	}

	go syncer.sync(ctx)

	<-stopChannel // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func readK8sPoolSizeEnvVar(log logr.Logger) int {
	envK8sClientsPoolSize, found := os.LookupEnv(envVarK8sClientsPoolSize)

	if !found {
		log.Info("readK8sPoolSizeEnvVar", fmt.Sprintf("env variable %s not found", envVarK8sClientsPoolSize),
			fmt.Sprintf("using default value %d", defaultK8sClientsPoolSize))

		return defaultK8sClientsPoolSize
	}

	value, err := strconv.Atoi(envK8sClientsPoolSize)
	if err != nil {
		log.Info("readK8sPoolSizeEnvVar", fmt.Sprintf("failed to convert variable %s, value: %s",
			envVarK8sClientsPoolSize, envK8sClientsPoolSize), fmt.Sprintf("using default value %d", defaultK8sClientsPoolSize))

		return defaultK8sClientsPoolSize
	}

	return value
}

func (syncer *LeafHubBundlesSpecSync) sync(ctx context.Context) {
	syncer.log.Info("start bundles syncing...")

	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case receivedBundle := <-syncer.bundleUpdatesChan: // handle the bundle
			syncer.clientWorkersWaitGroup.Add(len(receivedBundle.Objects) + len(receivedBundle.DeletedObjects))

			// send "update" jobs to client workers
			for _, obj := range receivedBundle.Objects {
				syncer.clientWorkersJobChan <- &clientWorkerJob{handler: syncer.updateObject, obj: obj}
			}

			// send "delete" jobs to client workers
			for _, obj := range receivedBundle.DeletedObjects {
				syncer.clientWorkersJobChan <- &clientWorkerJob{handler: syncer.deleteObject, obj: obj}
			}

			// ensure all updates and deletes have finished before receiving next bundle
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
