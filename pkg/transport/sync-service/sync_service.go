package sync_service

import (
	"bytes"
	"encoding/json"
	"fmt"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-horizon/edge-sync-service-client/client"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	syncServiceProtocol = "SYNC_SERVICE_PROTOCOL"
	syncServiceHost = "SYNC_SERVICE_HOST"
	syncServicePort = "SYNC_SERVICE_PORT"
	syncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
)

type SyncService struct {
	client              *client.SyncServiceClient
	pollingInterval     int
	objectsMetaDataChan chan *client.ObjectMetaData
	updatesChan         chan *dataTypes.ObjectsBundle
	stopChan            chan struct{}
	startOnce           sync.Once
	stopOnce            sync.Once
}

func NewSyncService(updatesChan chan *dataTypes.ObjectsBundle) *SyncService {
	serverProtocol, host, port, pollingInterval := readEnvVars()
	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)
	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")
	return &SyncService {
		client:              syncServiceClient,
		pollingInterval:     pollingInterval,
		objectsMetaDataChan: make(chan *client.ObjectMetaData),
		updatesChan:         updatesChan,
		stopChan:            make(chan struct{}, 1),
	}
}

func readEnvVars() (string, string, uint16, int) {
	protocol := os.Getenv(syncServiceProtocol)
	if protocol == "" {
		log.Fatalf("the expected var %s is not set in environment variables", syncServiceProtocol)
	}
	host := os.Getenv(syncServiceHost)
	if host == "" {
		log.Fatalf("the expected var %s is not set in environment variables", syncServiceHost)
	}
	portStr := os.Getenv(syncServicePort)
	if portStr == "" {
		log.Fatalf("the expected env var %s is not set in environment variables", syncServicePort)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("the expected env var %s is not from type uint", syncServicePort)
	}
	pollingIntervalStr := os.Getenv(syncServicePollingInterval)
	if pollingIntervalStr == "" {
		log.Fatalf("the expected env var %s is not set in environment variables", syncServicePollingInterval)
	}
	pollingInterval, err := strconv.Atoi(pollingIntervalStr)
	if err != nil {
		log.Fatalf("the expected env var %s is not from type int", syncServicePollingInterval)
	}
	return protocol, host, uint16(port), pollingInterval
}

func (s *SyncService) Start() {
	s.startOnce.Do(func() {
		go s.handleBundles()
	})
}

func (s *SyncService) Stop() {
	close(s.stopChan)
	close(s.objectsMetaDataChan)
}

func (s *SyncService) handleBundles() {
	// register for updates for spec bundle type, this include all types of objects each with different id.
	s.client.StartPollingForUpdates(dataTypes.SpecBundle, s.pollingInterval, s.objectsMetaDataChan)
	for {
		select {
		case <-s.stopChan:
			return
		case objectMetaData := <-s.objectsMetaDataChan:
			var buffer bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buffer) {
				log.Println(fmt.Sprintf("failed to read bundle object with id %s from sync service",
					objectMetaData.ObjectID))
				continue
			}
			bundle := &dataTypes.ObjectsBundle{}
			err := json.Unmarshal(buffer.Bytes(), bundle)
			if err != nil {
				log.Println(fmt.Sprintf("failed to parse bundle object with id %s from sync service",
					objectMetaData.ObjectID))
				continue
			}
			s.updatesChan <- bundle
			err = s.client.MarkObjectReceived(objectMetaData)
			if err != nil {
				log.Println("failed to report object received to sync service")
			}
		}
	}
}
