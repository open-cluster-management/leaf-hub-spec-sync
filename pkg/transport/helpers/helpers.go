package helpers

import (
	"encoding/json"
	"fmt"

	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
)

// SyncCustomBundle writes a custom bundle to its respective syncer channel.
func SyncCustomBundle(customBundleRegistration *transport.CustomBundleRegistration, payload []byte) error {
	receivedBundle := customBundleRegistration.CreateBundleFunc()
	if err := json.Unmarshal(payload, &receivedBundle); err != nil {
		return fmt.Errorf("failed to parse bundle - %w", err)
	}

	customBundleRegistration.BundleUpdatesChan <- receivedBundle

	return nil
}
