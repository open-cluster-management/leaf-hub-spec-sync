package transport

// Transport is an interface for transport layer.
type Transport interface {
	// Start starts the transport.
	Start()
	// Stop stops the transport.
	Stop()
}
