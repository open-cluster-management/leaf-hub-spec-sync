package bundle

import (
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
)

// Bundle is a bundle received from transport containing ObjectsBundle and transport BundleMetadata.
type Bundle struct {
	*ObjectsBundle
	transport.BundleMetadata
}
