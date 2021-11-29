package transport

// Transport is an interface for transport layer.
type Transport interface {
	// CommitAsync marks a transported bundle as processed.
	CommitAsync(metadata BundleMetadata)
	// Start starts the transport.
	Start()
	// Stop stops the transport.
	Stop()
}
