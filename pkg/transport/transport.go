package transport

// Transport is an interface for transport layer.
type Transport interface {
	// Start starts the transport.
	Start()
	// Stop stops the transport.
	Stop()
	// Register function registers a bundle ID to a BundleRegistration.
	Register(msgID string, bundleRegistration *BundleRegistration)
}
