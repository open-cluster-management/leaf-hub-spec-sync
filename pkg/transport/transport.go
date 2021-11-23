package transport

// Transport is an interface for transport layer.
type Transport interface {
	// CommitAsync marks a transported bundle as received (or its offset).
	CommitAsync(interface{})
	// Start starts the transport.
	Start() error
	// Stop stops the transport.
	Stop()
}
