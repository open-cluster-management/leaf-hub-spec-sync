package transport

// Transport is an interface for transport layer.
type Transport interface {
	// Start starts the transport.
	Start()
	// Stop stops the transport.
	Stop()
	// Register function registers a bundles channel to a msgID.
	Register(msgID string, bundleUpdatesChan chan interface{})
}
