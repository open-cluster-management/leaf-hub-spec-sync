package transport

// Transport is an interface for transport layer.
type Transport interface {
	Start()
	Stop()
	CommitAsync(interface{})
}
