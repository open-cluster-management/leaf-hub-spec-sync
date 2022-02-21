package bundles

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	clusterv1 "github.com/open-cluster-management/api/cluster/v1"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/spec"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	periodicApplyInterval = 5 * time.Second
	defaultNamespace      = "default"
)

// AddManagedClusterLabelsBundleSyncer adds UnstructuredBundleSyncer to the manager.
func AddManagedClusterLabelsBundleSyncer(log logr.Logger, mgr ctrl.Manager, transportObj transport.Transport,
	k8sWorkerPool *k8sworkerpool.K8sWorkerPool) error {
	bundleUpdatesChan := make(chan interface{})

	if err := mgr.Add(&ManagedClusterLabelsBundleSyncer{
		log:                          log,
		bundleUpdatesChan:            bundleUpdatesChan,
		latestBundle:                 nil,
		managedClusterToTimestampMap: make(map[string]*time.Time),
		k8sWorkerPool:                k8sWorkerPool,
		bundleProcessingWaitingGroup: sync.WaitGroup{},
		latestBundleLock:             sync.Mutex{},
	}); err != nil {
		close(bundleUpdatesChan)
		return fmt.Errorf("failed to add unstructured bundles spec syncer - %w", err)
	}

	transportObj.Register(datatypes.ManagedClustersMetadataMsgKey, &transport.BundleRegistration{
		CreateBundleFunc: func() interface{} {
			return &spec.ManagedClusterLabelsSpecBundle{}
		},
		BundleUpdatesChan: bundleUpdatesChan,
	})

	return nil
}

// ManagedClusterLabelsBundleSyncer syncs managed clusters metadata from received bundles.
type ManagedClusterLabelsBundleSyncer struct {
	log               logr.Logger
	bundleUpdatesChan chan interface{}

	latestBundle                 *spec.ManagedClusterLabelsSpecBundle
	managedClusterToTimestampMap map[string]*time.Time

	k8sWorkerPool                *k8sworkerpool.K8sWorkerPool
	bundleProcessingWaitingGroup sync.WaitGroup
	latestBundleLock             sync.Mutex
}

// Start function starts bundles spec syncer.
func (syncer *ManagedClusterLabelsBundleSyncer) Start(ctx context.Context) error {
	syncer.log.Info("started bundles syncer...")

	go syncer.sync(ctx)
	go syncer.bundleHandler(ctx)

	<-ctx.Done() // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *ManagedClusterLabelsBundleSyncer) sync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case transportedBundle := <-syncer.bundleUpdatesChan: // handle the bundle
			receivedBundle, ok := transportedBundle.(*spec.ManagedClusterLabelsSpecBundle)
			if !ok {
				continue
			}

			syncer.setLatestBundle(receivedBundle) // uses latestBundleLock
		}
	}
}

func (syncer *ManagedClusterLabelsBundleSyncer) bundleHandler(ctx context.Context) {
	ticker := time.NewTicker(periodicApplyInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if syncer.latestBundle == nil {
				continue
			}

			syncer.handleBundle()
		}
	}
}

func (syncer *ManagedClusterLabelsBundleSyncer) setLatestBundle(newBundle *spec.ManagedClusterLabelsSpecBundle) {
	syncer.latestBundleLock.Lock()
	defer syncer.latestBundleLock.Unlock()

	syncer.latestBundle = newBundle
}

func (syncer *ManagedClusterLabelsBundleSyncer) handleBundle() {
	syncer.latestBundleLock.Lock()
	defer syncer.latestBundleLock.Unlock()

	for _, managedClusterMetadata := range syncer.latestBundle.Objects {
		lastProcessedTimestamp := syncer.getManagedClusterLastProcessedTimestamp(managedClusterMetadata.Name)
		if managedClusterMetadata.UpdateTimestamp.After(*lastProcessedTimestamp) { // handle (success) once
			syncer.bundleProcessingWaitingGroup.Add(1)
			syncer.updateManagedClusterAsync(managedClusterMetadata, lastProcessedTimestamp)
		}
	}

	// ensure all updates and deletes have finished before reading next bundle
	syncer.bundleProcessingWaitingGroup.Wait()
}

func (syncer *ManagedClusterLabelsBundleSyncer) updateManagedClusterAsync(labelsSpec *spec.ManagedClusterLabelsSpec,
	lastProcessedTimestampInMap *time.Time) {
	syncer.k8sWorkerPool.RunAsync(k8sworkerpool.NewK8sJob(labelsSpec, func(ctx context.Context,
		k8sClient client.Client, obj interface{},
	) {
		managedCluster := &clusterv1.ManagedCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKey{
			Name:      labelsSpec.Name,
			Namespace: defaultNamespace,
		}, managedCluster); k8serrors.IsNotFound(err) {
			syncer.log.Info("managed cluster ignored - not found", "name", labelsSpec.Name)
			syncer.managedClusterMarkUpdated(labelsSpec, lastProcessedTimestampInMap) // if not found then irrelevant

			return
		} else if err != nil {
			syncer.log.Error(err, "failed to get managed cluster", "name", labelsSpec.Name)
			syncer.bundleProcessingWaitingGroup.Done()

			return
		}

		// enforce received labels state (overwrite if exists)
		for key, value := range labelsSpec.Labels {
			managedCluster.Labels[key] = value
		}

		// delete labels by key
		for _, labelKey := range labelsSpec.DeletedLabelKeys {
			delete(managedCluster.Labels, labelKey)
		}

		// update CR with replace API: fails if CR was modified since client.get
		if err := k8sClient.Update(ctx, managedCluster, &client.UpdateOptions{}); err != nil {
			syncer.log.Error(err, "failed to update managed cluster", "name", labelsSpec.Name)
		}

		syncer.log.Info("managed cluster updated", "name", labelsSpec.Name)
		syncer.managedClusterMarkUpdated(labelsSpec, lastProcessedTimestampInMap)
	}))
}

func (syncer *ManagedClusterLabelsBundleSyncer) managedClusterMarkUpdated(labelsSpec *spec.ManagedClusterLabelsSpec,
	lastProcessedTimestampInMap *time.Time) {
	*lastProcessedTimestampInMap = labelsSpec.UpdateTimestamp

	syncer.bundleProcessingWaitingGroup.Done()
}

func (syncer *ManagedClusterLabelsBundleSyncer) getManagedClusterLastProcessedTimestamp(name string) *time.Time {
	timestamp, found := syncer.managedClusterToTimestampMap[name]
	if found {
		return timestamp
	}

	timestamp = &time.Time{}
	syncer.managedClusterToTimestampMap[name] = timestamp

	return timestamp
}
