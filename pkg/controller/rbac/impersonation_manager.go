package rbac

import (
	"encoding/base64"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// NoUserIdentity is used to mark no user identity is defined in a resource.
	NoUserIdentity = ""
	// UserIdentityAnnotation is the annotation that is used to store the user identity.
	UserIdentityAnnotation = "open-cluster-management.io/user-identity"
)

// NewImpersonationManager creates a new instance of ImpersonationManager.
func NewImpersonationManager(config *rest.Config) *ImpersonationManager {
	return &ImpersonationManager{
		k8sConfig: config,
	}
}

// ImpersonationManager manages the k8s clients for the various users and for the controller.
type ImpersonationManager struct {
	k8sConfig *rest.Config
}

// Impersonate gets the user identity and returns the k8s client that represents the requesting user.
func (manager *ImpersonationManager) Impersonate(userIdentity string) (client.Client, error) {
	newConfig := rest.CopyConfig(manager.k8sConfig)
	newConfig.Impersonate = rest.ImpersonationConfig{
		UserName: userIdentity,
		Groups:   nil,
		Extra:    nil,
	}

	userK8sClient, err := client.New(newConfig, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to create new k8s client for user - %w", err)
	}

	return userK8sClient, nil
}

// GetUserIdentity returns the user identity in the obj or NoUserIdentity in case the user-identity can't be found
// in the object.
func (manager *ImpersonationManager) GetUserIdentity(obj *unstructured.Unstructured) (string, error) {
	annotations := obj.GetAnnotations()
	if annotations == nil { // there are no annotations defined, therefore user identity annotation is not defined.
		return NoUserIdentity, nil
	}

	if base64UserIdentity, found := annotations[UserIdentityAnnotation]; found { // if annotation exists
		decodedUserIdentity, err := base64.StdEncoding.DecodeString(base64UserIdentity)
		if err != nil {
			return NoUserIdentity, fmt.Errorf("failed to decode base64 user identity - %w", err)
		}

		return string(decodedUserIdentity), nil
	}
	// otherwise, the annotation doesn't exist
	return NoUserIdentity, nil
}
