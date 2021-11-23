package transport

import "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"

// Transport is an interface for transport layer.
type Transport interface {
	// CommitAsync marks a transported bundle as processed.
	CommitAsync(*bundle.ObjectsBundle)
	// Start starts the transport.
	Start()
	// Stop stops the transport.
	Stop()
}
