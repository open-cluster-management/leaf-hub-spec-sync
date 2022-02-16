package transport

// BundleRegistration abstract the registration for bundles according to bundle ID in transport layer.
type BundleRegistration struct {
	CreateBundleFunc  func() interface{}
	BundleUpdatesChan chan interface{}
}
