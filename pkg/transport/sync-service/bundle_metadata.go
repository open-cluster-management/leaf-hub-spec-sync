package syncservice

import "github.com/open-horizon/edge-sync-service-client/client"

// BundleMetadata holds the data necessary to commit a received message.
type BundleMetadata struct {
	ObjectMetadata *client.ObjectMetaData
}
