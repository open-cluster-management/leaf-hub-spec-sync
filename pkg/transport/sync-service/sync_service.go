package syncservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/objects"
	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/pkg/errors"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
)

// SyncService abstracts Sync Service client
type SyncService struct {
	log                 logr.Logger
	client              *client.SyncServiceClient
	pollingInterval     int
	objectsMetaDataChan chan *client.ObjectMetaData
	bundlesMetaDataChan chan *client.ObjectMetaData
	bundlesUpdatesChan  chan *bundle.ObjectsBundle
	objectsUpdatesChan  chan *objects.HubOfHubsObject
	stopChan            chan struct{}
	startOnce           sync.Once
	stopOnce            sync.Once
}

// NewSyncService creates a new instance of SyncService
func NewSyncService(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle,
	objectUpdatesChan chan *objects.HubOfHubsObject) (*SyncService, error) {
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
		objectsMetaDataChan: make(chan *client.ObjectMetaData),
		bundlesUpdatesChan:  bundleUpdatesChan,
		objectsUpdatesChan:  objectUpdatesChan,
		stopChan:            make(chan struct{}, 1),
	}, nil
}

func readEnvVars() (string, string, uint16, int, error) {
	protocol, found := os.LookupEnv(envVarSyncServiceProtocol)
	if !found {
		return "", "", 0, 0, fmt.Errorf("not found: environment variable %s", envVarSyncServiceProtocol)
	}

	host, found := os.LookupEnv(envVarSyncServiceHost)
	if !found {
		return "", "", 0, 0, fmt.Errorf("not found: environment variable %s", envVarSyncServiceHost)
	}

	portString, found := os.LookupEnv(envVarSyncServicePort)
	if !found {
		return "", "", 0, 0, fmt.Errorf("not found: environment variable %s", envVarSyncServicePort)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w", envVarSyncServicePort,
			err)
	}

	pollingIntervalString, found := os.LookupEnv(envVarSyncServicePollingInterval)
	if !found {
		return "", "", 0, 0, fmt.Errorf("not found: environment variable %s", envVarSyncServicePollingInterval)
	}

	pollingInterval, err := strconv.Atoi(pollingIntervalString)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w",
			envVarSyncServicePollingInterval, err)
	}

	return protocol, host, uint16(port), pollingInterval, nil
}

// Start function starts sync service
func (s *SyncService) Start() {
	s.startOnce.Do(func() {
		go s.handleBundles()
		go s.handleObjects()
	})
}

// Stop function stops sync service
func (s *SyncService) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
		close(s.bundlesMetaDataChan)
		close(s.objectsMetaDataChan)
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
				s.log.Error(errors.New("sync service error"), "failed to read bundle from sync service",
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

func (s *SyncService) handleObjects() {
	// register for config updates from hub of hubs.
	s.client.StartPollingForUpdates(datatypes.Config, s.pollingInterval, s.objectsMetaDataChan)
	for {
		select {
		case <-s.stopChan:
			return
		case objectMetaData := <-s.objectsMetaDataChan:
			var buf bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buf) {
				s.log.Error(errors.New("sync service error"), "failed to read object from sync service",
					"ObjectId", objectMetaData.ObjectID)
				continue
			}

			receivedObject := &objects.HubOfHubsObject{}
			if err := json.Unmarshal(buf.Bytes(), receivedObject); err != nil {
				s.log.Error(err, "failed to parse object", "ObjectId", objectMetaData.ObjectID)
				continue
			}

			s.objectsUpdatesChan <- receivedObject
			if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
				s.log.Error(err, "failed to report object received to sync service")
			}
		}
	}
}
