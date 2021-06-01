package controller

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	dataTypes "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/data-types"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	"log"
	"sync"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	namespace = "hoh-system"
)

type LeafHubSpecSync struct {
	k8sRestClient 			rest.Interface
	transport          	 	transport.Transport
	policiesUpdateChan 	 	chan *dataTypes.PoliciesBundle
	stopChan             	chan struct{}
	stopOnce             	sync.Once
}

func NewLeafHubSpecSync(transport transport.Transport,
	policiesUpdateChan chan *dataTypes.PoliciesBundle) *LeafHubSpecSync {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("failed to get in cluster kubeconfig - %s", err)
	}
	// creates the clientset
	k8sClientSet := kubernetes.NewForConfigOrDie(config)

	specSync := &LeafHubSpecSync {
		k8sRestClient: k8sClientSet.RESTClient(),
		transport: transport,
		policiesUpdateChan: policiesUpdateChan,
	}
	return specSync
}

func (s *LeafHubSpecSync) Start() {
	s.syncPolicies()

}

func (s *LeafHubSpecSync) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
	})
}

func (s *LeafHubSpecSync) syncPolicies() {
	log.Println("start syncing...")
	for {
		select { // wait for incoming message to handle
		case <-s.stopChan:
			return
		case policiesBundle := <-s.policiesUpdateChan:
			for _, policy := range policiesBundle.Policies {
				policy.Name = fmt.Sprintf("%s.%s",policy.Name, policy.Namespace)
				policy.Namespace = namespace
				s.updatePolicy(policy)
			}
			for _, policy := range policiesBundle.DeletedPolicies {
				policy.Name = fmt.Sprintf("%s.%s",policy.Name, policy.Namespace)
				policy.Namespace = namespace
				s.deletePolicy(policy)
			}
		}
	}
}

func (s *LeafHubSpecSync) updatePolicy(policy *v1.Policy) {
	log.Println("updating policy:", policy.Name)
	body, _ := json.Marshal(policy)
	data,err := s.k8sRestClient.
		Get().
		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies/%s",policy.APIVersion, namespace,policy.Name)).
		DoRaw(context.TODO())
	var request *rest.Request
	if err != nil { // object doesn't exist, need to create using POST
		request = s.k8sRestClient.Post()
	} else { // object exist, need to update using PUT
		request = s.k8sRestClient.Put()
	}
	data, err = request.
		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies",policy.APIVersion, namespace)).
		Body(body).
		DoRaw(context.TODO())
	if err != nil {
		log.Printf("failed to update policy - %s - %s", err, data)
	}
}

func (s *LeafHubSpecSync) deletePolicy(policy *v1.Policy) {
	log.Println("deleting policy:", policy.Name)
	data, err := s.k8sRestClient.
		Delete().
		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies",policy.APIVersion, namespace)).
		DoRaw(context.TODO())
	log.Println("deleted policy", policy.Name, "successfully.")
	if err != nil {
		log.Printf("failed to delete policy - %s - %s", err, data)
	}
}
