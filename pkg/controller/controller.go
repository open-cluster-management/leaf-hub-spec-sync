package controller

import (
	"context"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"sync"
)

const (
	controllerName      = "leaf-hub-spec-sync"
	notFoundErrorSuffix = "not found"
)

type LeafHubSpecSync struct {
	k8sClient         client.Client
	transport         transport.Transport
	bundleUpdatesChan chan *bundle.ObjectsBundle
	forceChanges      bool
	stopChan          chan struct{}
	startOnce         sync.Once
	stopOnce          sync.Once
}

func NewLeafHubSpecSync(transport transport.Transport, updatesChan chan *bundle.ObjectsBundle) *LeafHubSpecSync {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("failed to get in cluster kubeconfig - %s", err)
	}

	k8sClient, err := client.New(config, client.Options{})

	return &LeafHubSpecSync{
		k8sClient:         k8sClient,
		transport:         transport,
		bundleUpdatesChan: updatesChan,
		forceChanges:      true,
		stopChan:          make(chan struct{}, 1),
	}
}

func (s *LeafHubSpecSync) Start() {
	s.startOnce.Do(func() {
		s.syncObjects()
	})

}

func (s *LeafHubSpecSync) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
	})
}

func (s *LeafHubSpecSync) syncObjects() {
	log.Println("start syncing...")
	for {
		select { // wait for incoming message to handle
		case <-s.stopChan:
			return
		case receivedBundle := <-s.bundleUpdatesChan:
			for _, updatedObject := range receivedBundle.Objects {
				objectBytes, _ := updatedObject.MarshalJSON()
				err := s.k8sClient.Patch(context.Background(), updatedObject,
					client.RawPatch(types.ApplyPatchType, objectBytes), &client.PatchOptions{
						FieldManager: controllerName,
						Force:        &(s.forceChanges),
					})
				if err != nil {
					log.Println(err)
					continue
				}
				log.Println("updated", updatedObject.GetKind(), "-", updatedObject.GetName())
			}
			for _, deletedObject := range receivedBundle.DeletedObjects {
				err := s.k8sClient.Delete(context.Background(), deletedObject)
				if err != nil {
					if !strings.HasSuffix(err.Error(), notFoundErrorSuffix) {
						log.Println(err) // if trying to delete object which doesn't exits, don't log any error.
					}
					continue
				}
				log.Println("deleted", deletedObject.GetKind(), "-", deletedObject.GetName())
			}
		}
	}
}
