package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

const (
	controllerName = "leaf-hub-spec-sync"
	notFoundSuffixError = "not found"
)

type LeafHubSpecSync struct {
	k8sDynamicClient    dynamic.Interface
	k8sRestMapper       *restmapper.DeferredDiscoveryRESTMapper
	transport           transport.Transport
	bundleUpdatesChan   chan *dataTypes.ObjectsBundle
	unstructuredDecoder runtime.Serializer
	forceChanges        bool
	stopChan            chan struct{}
	stopOnce            sync.Once
}

func NewLeafHubSpecSync(transport transport.Transport, updatesChan chan *dataTypes.ObjectsBundle) *LeafHubSpecSync {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("failed to get in cluster kubeconfig - %s", err)
	}
	// creates the k8s dynamic client and rest mapper to map resources to kind, and map kind and version to interfaces
	dynamicClient := dynamic.NewForConfigOrDie(config)
	discoveryClient := discovery.NewDiscoveryClientForConfigOrDie(config)
	k8sMapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	return &LeafHubSpecSync {
		k8sDynamicClient:    dynamicClient,
		k8sRestMapper:       k8sMapper,
		transport:           transport,
		bundleUpdatesChan:   updatesChan,
		unstructuredDecoder: yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme),
		forceChanges:        true,
	}
}

func (s *LeafHubSpecSync) Start() {
	s.syncObjects()

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
		case bundle := <-s.bundleUpdatesChan:
			for _, updatedObject := range bundle.Objects {
				name, kind, err := s.applyObject(updatedObject)
				if err != nil {
					log.Println(err)
					continue
				}
				log.Println("updated", kind, "-", name)
			}
			for _, deletedObject := range bundle.DeletedObjects {
				name, kind, deleted, err := s.deleteObject(deletedObject)
				if err != nil {
					log.Println(err)
					continue
				}
				if deleted { // not printing in case it wasn't deleted in this cycle
					log.Println("deleted", kind, "-", name)
				}
			}
		}
	}
}

func (s *LeafHubSpecSync) applyObject(object interface{}) (string, string, error) { // name, kind, error
	resourceInterface, unstructuredObj, data, err := s.processObject(object)
	if err != nil {
		return "", "", err
	}
	// Create or Update the object with SSA. types.ApplyPatchType indicates SSA. (Server Side Apply)
	// FieldManager specifies the field owner ID.
	_, err = resourceInterface.Patch(context.TODO(), unstructuredObj.GetName(), types.ApplyPatchType, data,
		metav1.PatchOptions {
			FieldManager: controllerName,
			Force: &s.forceChanges,
		})
	if err != nil {
		return "", "", errors.New(fmt.Sprintf("failed to apply object - %s - %s",err, data))
	}
	return unstructuredObj.GetName(), unstructuredObj.GetKind(), nil
}

func (s *LeafHubSpecSync) deleteObject(object interface{}) (string, string, bool, error) { //name, kind, deleted, error
	resourceInterface, unstructuredObj, data, err := s.processObject(object)
	if err != nil {
		return "", "", false, err
	}
	// Delete the object
	err = resourceInterface.Delete(context.TODO(), unstructuredObj.GetName(), metav1.DeleteOptions {})
	if err != nil {
		if strings.HasSuffix(err.Error(), notFoundSuffixError) { //trying to delete object which doesn't exist
			return unstructuredObj.GetName(), unstructuredObj.GetKind(), false, nil
		} else {
			return "", "", false, errors.New(fmt.Sprintf("failed to delete object - %s - %s", err, data))
		}
	}
	return unstructuredObj.GetName(), unstructuredObj.GetKind(), true, nil
}

func (s *LeafHubSpecSync) processObject(object interface{}) (dynamic.ResourceInterface, *unstructured.Unstructured, []byte, error) {
	objectBytes, err := json.Marshal(object)
	if err != nil {
		return nil, nil, nil, errors.New(fmt.Sprintf("failed to marshal the object into bytes - %s", err))
	}
	unstructuredObj := &unstructured.Unstructured{}
	// GVK stands for group, version, kind
	_, gvk, err := s.unstructuredDecoder.Decode(objectBytes, nil, unstructuredObj)
	if err != nil {
		return nil, nil, nil, errors.New(fmt.Sprintf("failed to decode object bytes to unstructured - %s", err))
	}

	// get GVR (group, version, resource) out of GVK
	mapping, err := s.k8sRestMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, nil, nil, errors.New(fmt.Sprintf("failed to get mapping of gvk %s- ",err))
	}
	// obtain REST interface for the GVR
	var resourceInterface dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		// namespaced resources should specify the namespace
		resourceInterface = s.k8sDynamicClient.Resource(mapping.Resource).Namespace(unstructuredObj.GetNamespace())
	} else {
		// for cluster-wide resources
		resourceInterface = s.k8sDynamicClient.Resource(mapping.Resource)
	}
	// marshal unstructuredObj into JSON
	data, err := json.Marshal(unstructuredObj)
	if err != nil {
		return nil, nil, nil, errors.New(fmt.Sprintf("failed to marshal unstructured object into json %s- ",err))
	}
	return resourceInterface, unstructuredObj, data, nil
}
