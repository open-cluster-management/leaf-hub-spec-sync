package transport

// Transport is an interface for transport layer.
type Transport interface {
	CommitAsync(interface{})
	// Start starts the transport.
	Start() error
	// Stop stops the transport.
	Stop()
}
