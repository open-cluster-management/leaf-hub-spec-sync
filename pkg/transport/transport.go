package transport

// Transport is an interface for transport layer.
type Transport interface {
	CommitAsync(interface{})
	// Start starts the transport (requirement for Runnable).
	Start(stopChannel <-chan struct{}) error
}
