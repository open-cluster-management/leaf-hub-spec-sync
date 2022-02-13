package bundles

import (
	"context"
	"errors"
	"fmt"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	clusterv1 "github.com/open-cluster-management/api/cluster/v1"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/spec"
	k8sworkerpool "github.com/stolostron/leaf-hub-spec-sync/pkg/controller/k8s-worker-pool"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	periodicApplyInterval = 5 * time.Second
	labelTokensSize       = 2
)

var errInvalidLabelFormat = errors.New("invalid label format, format should be key:value")

// AddManagedClustersMetadataBundleSyncer adds UnstructuredBundleSyncer to the manager.
func AddManagedClustersMetadataBundleSyncer(log logr.Logger, mgr ctrl.Manager, transport transport.Transport,
	k8sWorkerPool *k8sworkerpool.K8sWorkerPool) error {
	bundleUpdatesChan := make(chan interface{})

	if err := mgr.Add(&ManagedClustersMetadataBundleSyncer{
		log:                          log,
		bundleUpdatesChan:            bundleUpdatesChan,
		lastReceivedBundle:           nil,
		highestProcessedVersion:      0,
		k8sWorkerPool:                k8sWorkerPool,
		bundleProcessingWaitingGroup: sync.WaitGroup{},
		receivedBundleLock:           sync.Mutex{},
		versionLock:                  sync.Mutex{},
	}); err != nil {
		close(bundleUpdatesChan)
		return fmt.Errorf("failed to add unstructured bundles spec syncer - %w", err)
	}

	transport.Register(datatypes.ManagedClustersMetadataMsgKey, bundleUpdatesChan)

	return nil
}

// ManagedClustersMetadataBundleSyncer syncs managed clusters metadata from received bundles.
type ManagedClustersMetadataBundleSyncer struct {
	log               logr.Logger
	bundleUpdatesChan chan interface{}

	lastReceivedBundle      *spec.ManagedClustersMetadata
	highestProcessedVersion uint64

	k8sWorkerPool                *k8sworkerpool.K8sWorkerPool
	bundleProcessingWaitingGroup sync.WaitGroup
	receivedBundleLock           sync.Mutex
	versionLock                  sync.Mutex
}

// Start function starts bundles spec syncer.
func (syncer *ManagedClustersMetadataBundleSyncer) Start(ctx context.Context) error {
	syncer.log.Info("started bundles syncer...")

	go syncer.sync(ctx)
	go syncer.bundleHandler(ctx)

	<-ctx.Done() // blocking wait for stop event
	syncer.log.Info("stopped bundles syncer")

	return nil
}

func (syncer *ManagedClustersMetadataBundleSyncer) sync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			return

		case transportedBundle := <-syncer.bundleUpdatesChan: // handle the bundle
			receivedBundle, ok := transportedBundle.(*spec.ManagedClustersMetadata)
			if !ok {
				continue
			}

			syncer.updateLastReceivedBundle(receivedBundle) // uses receivedBundleLock
		}
	}
}

func (syncer *ManagedClustersMetadataBundleSyncer) bundleHandler(ctx context.Context) {
	ticker := time.NewTicker(periodicApplyInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if syncer.lastReceivedBundle == nil || syncer.lastReceivedBundle.Version == syncer.highestProcessedVersion {
				continue
			}

			syncer.handleBundle()
		}
	}
}

func (syncer *ManagedClustersMetadataBundleSyncer) updateLastReceivedBundle(newBundle *spec.ManagedClustersMetadata) {
	syncer.receivedBundleLock.Lock()
	defer syncer.receivedBundleLock.Unlock()

	syncer.lastReceivedBundle = newBundle
}

func (syncer *ManagedClustersMetadataBundleSyncer) handleBundle() {
	syncer.receivedBundleLock.Lock()
	defer syncer.receivedBundleLock.Unlock()

	receivedBundle := syncer.lastReceivedBundle

	for _, managedClusterMetadata := range receivedBundle.Objects {
		if managedClusterMetadata.Version > syncer.highestProcessedVersion { // handle (successfully) only once
			syncer.bundleProcessingWaitingGroup.Add(1)
			syncer.updateManagedClusterAsync(managedClusterMetadata)
		}
	}

	// ensure all updates and deletes have finished before reading next bundle
	syncer.bundleProcessingWaitingGroup.Wait()
}

func (syncer *ManagedClustersMetadataBundleSyncer) updateManagedClusterAsync(metadata *spec.ManagedClusterMetadata) {
	syncer.k8sWorkerPool.RunAsync(k8sworkerpool.NewK8sJob(metadata, func(ctx context.Context,
		k8sClient client.Client, obj interface{},
	) {
		managedCluster := &clusterv1.ManagedCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKey{
			Name:      metadata.Name,
			Namespace: metadata.Namespace,
		}, managedCluster); k8serrors.IsNotFound(err) {
			syncer.managedClusterMarkUpdated(metadata.Version) // if not found then irrelevant
			return
		} else if err != nil {
			syncer.log.Error(err, "failed to get managed cluster", "name", metadata.Name,
				"namespace", metadata.Namespace)
			syncer.bundleProcessingWaitingGroup.Done()

			return
		}

		// enforce received labels state (overwrite if exists)
		for _, labelPair := range metadata.Labels {
			keyValue := strings.Split(labelPair, ":") // label format key:value
			if len(keyValue) != labelTokensSize {
				syncer.log.Error(errInvalidLabelFormat, "skipped label enforcement", "label", labelPair)
				continue
			}

			managedCluster.Labels[keyValue[0]] = keyValue[1]
		}

		// delete labels by key
		for _, labelKey := range metadata.DeletedLabelKeys {
			delete(managedCluster.Labels, labelKey)
		}

		// update CR with replace API: fails if CR was modified since client.get
		if err := k8sClient.Update(ctx, managedCluster, &client.UpdateOptions{}); err != nil {
			syncer.log.Error(err, "failed to update managed cluster", "name", metadata.Name,
				"namespace", metadata.Namespace)
		}

		syncer.managedClusterMarkUpdated(metadata.Version)
	}))
}

func (syncer *ManagedClustersMetadataBundleSyncer) managedClusterMarkUpdated(version uint64) {
	syncer.versionLock.Lock()
	defer syncer.versionLock.Unlock()

	if version > syncer.highestProcessedVersion {
		syncer.highestProcessedVersion = version
	}

	syncer.bundleProcessingWaitingGroup.Done()
}
