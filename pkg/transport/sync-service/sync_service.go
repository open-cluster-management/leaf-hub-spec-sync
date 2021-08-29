package syncservice

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-horizon/edge-sync-service-client/client"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
)

var (
	errEnvVarNotFound        = errors.New("not found environment variable")
	errSyncServiceReadFailed = errors.New("sync service error")
)

// NewSyncService creates a new instance of SyncService.
func NewSyncService(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle) (*SyncService, error) {
	serverProtocol, host, port, pollingInterval, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	return &SyncService{
		log:                 log,
		client:              syncServiceClient,
		pollingInterval:     pollingInterval,
		bundlesMetaDataChan: make(chan *client.ObjectMetaData),
		bundlesUpdatesChan:  bundleUpdatesChan,
		stopChan:            make(chan struct{}, 1),
	}, nil
}

// SyncService abstracts Sync Service client.
type SyncService struct {
	log                 logr.Logger
	client              *client.SyncServiceClient
	pollingInterval     int
	bundlesMetaDataChan chan *client.ObjectMetaData
	bundlesUpdatesChan  chan *bundle.ObjectsBundle
	stopChan            chan struct{}
	startOnce           sync.Once
	stopOnce            sync.Once
}

func readEnvVars() (string, string, uint16, int, error) {
	protocol, found := os.LookupEnv(envVarSyncServiceProtocol)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServiceProtocol)
	}

	host, found := os.LookupEnv(envVarSyncServiceHost)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServiceHost)
	}

	portString, found := os.LookupEnv(envVarSyncServicePort)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServicePort)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w", envVarSyncServicePort,
			err)
	}

	pollingIntervalString, found := os.LookupEnv(envVarSyncServicePollingInterval)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServicePollingInterval)
	}

	pollingInterval, err := strconv.Atoi(pollingIntervalString)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w",
			envVarSyncServicePollingInterval, err)
	}

	return protocol, host, uint16(port), pollingInterval, nil
}

// CommitAsync commits a transported message that was processed locally.
func (s *SyncService) CommitAsync(_ interface{}) {}

// Start function starts sync service.
func (s *SyncService) Start() error {
	s.startOnce.Do(func() {
		go s.handleBundles()
	})

	return nil
}

// Stop function stops sync service.
func (s *SyncService) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
		close(s.bundlesMetaDataChan)
	})
}

func (s *SyncService) handleBundles() {
	// register for updates for spec bundles and config objects, includes all types of spec bundles.
	s.client.StartPollingForUpdates(datatypes.SpecBundle, s.pollingInterval, s.bundlesMetaDataChan)

	for {
		select {
		case <-s.stopChan:
			return
		case objectMetaData := <-s.bundlesMetaDataChan:
			var buf bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buf) {
				s.log.Error(errSyncServiceReadFailed, "failed to read bundle from sync service",
					"ObjectId", objectMetaData.ObjectID)
				continue
			}

			receivedBundle := &bundle.ObjectsBundle{}
			if err := json.Unmarshal(buf.Bytes(), receivedBundle); err != nil {
				s.log.Error(err, "failed to parse bundle", "ObjectId", objectMetaData.ObjectID)
				continue
			}

			s.bundlesUpdatesChan <- receivedBundle
			if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
				s.log.Error(err, "failed to report object received to sync service")
			}
		}
	}
}
