package controller

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	dataTypes "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/data-types"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	"log"
	"strings"
	"sync"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	namespace = "hoh-system"
	notFoundError = "the server could not find the requested resource"
	objectAlreadyExistErrorSuffix = "already exists"
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
				policy.SetName(fmt.Sprintf("%s.%s",policy.Name, policy.Namespace))
				policy.SetNamespace(namespace)
				s.updatePolicy(policy)
				//log.Println("creating policy -", policy.Name)
				//body, _ := json.Marshal(policy)
				//data, err := s.k8sRestClient.
				//	Post().
				//	AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies",policy.APIVersion, namespace)).
				//	Body(body).
				//	DoRaw(context.TODO())
				//if err != nil {
				//	log.Printf("failed to update policy - %s - %s", err, data)
				//}
			}
			for _, policy := range policiesBundle.DeletedPolicies {
				policy.SetName(fmt.Sprintf("%s.%s",policy.Name, policy.Namespace))
				policy.SetNamespace(namespace)
				s.deletePolicy(policy)
			}
		}
	}
}

func (s *LeafHubSpecSync) updatePolicy(policy *v1.Policy) {
	body, _ := json.Marshal(policy)
	result := s.k8sRestClient.
		Post().
		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies", policy.APIVersion, namespace)).
		Body(body).
		Do(context.TODO())
	if result.Error() != nil {
		errStr := fmt.Sprint(result.Error())
		if strings.HasSuffix(errStr, objectAlreadyExistErrorSuffix) {
			data, err := s.k8sRestClient.
				Put().
				AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies/%s", policy.APIVersion, namespace, policy.Name)).
				Body(body).
				DoRaw(context.TODO())
			if err != nil {
				log.Printf("failed to update policy - %s - %s", err, data)
			} else {
				log.Println("updated policy -", policy.Name)
			}
		}
	} else {
		log.Println("created policy -", policy.Name)
	}
}


	//body, _ := json.Marshal(policy)
	//data,err := s.k8sRestClient.
	//	Get().
	//	AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies/%s",policy.APIVersion, namespace,policy.Name)).
	//	DoRaw(context.TODO())
	//if err != nil { // object doesn't exist, need to create using POST
	//	log.Println("creating policy:", policy.Name)
	//	data, err = s.k8sRestClient.Post().
	//		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies",policy.APIVersion, namespace)).
	//		Body(body).
	//		DoRaw(context.TODO())
	//} else { // object exist, need to update using PUT
	//	log.Println("updating policy:", policy.Name)
	//	data, err = s.k8sRestClient.Put().
	//		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies",policy.APIVersion, namespace)).
	//		Body(body).
	//		DoRaw(context.TODO())
	//}
	//if err != nil {
	//	log.Printf("failed to update policy - %s - %s", err, data)
	//}
//}

func (s *LeafHubSpecSync) deletePolicy(policy *v1.Policy) {
	data, err := s.k8sRestClient.
		Delete().
		AbsPath(fmt.Sprintf("/apis/%s/namespaces/%s/policies/%s",policy.APIVersion, namespace, policy.Name)).
		DoRaw(context.TODO())
	if err != nil {
		if err.Error() != notFoundError {
			log.Printf("failed to delete policy - %s - %s", err, data)
		}
	} else {
		log.Println("deleted policy:", policy.Name)
	}
}
