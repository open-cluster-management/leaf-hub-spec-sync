package sync_service

import (
	"bytes"
	"encoding/json"
	"fmt"
	dataTypes "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/data-types"
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
	client                	*client.SyncServiceClient
	pollingInterval			int
	policiesMeataDataChan 	chan *client.ObjectMetaData
	policiesUpdateChan 	 	chan *dataTypes.PoliciesBundle
	stopChan              	chan struct{}
	startOnce             	sync.Once
	stopOnce              	sync.Once
}

func NewSyncService(policiesUpdateChan chan *dataTypes.PoliciesBundle) *SyncService {
	serverProtocol, host, port, pollingInterval := readEnvVars()
	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)
	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")
	syncService := &SyncService{
		client:                	syncServiceClient,
		pollingInterval:		pollingInterval,
		policiesMeataDataChan: 	make(chan *client.ObjectMetaData),
		policiesUpdateChan: 	policiesUpdateChan,
		stopChan:              	make(chan struct{}, 1),
	}
	return syncService
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
		go s.handlePoliciesBundle()
	})
}

func (s *SyncService) Stop() {
	close(s.stopChan)
	close(s.policiesMeataDataChan)
	close(s.policiesUpdateChan)
}

func (s *SyncService) handlePoliciesBundle() {
	s.client.StartPollingForUpdates(dataTypes.PolicyMessageType, s.pollingInterval, s.policiesMeataDataChan)
	for {
		select {
		case <-s.stopChan:
			return
		case objectMetaData := <-s.policiesMeataDataChan:
			var buffer bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buffer) {
				log.Println(fmt.Sprintf("failed to read policy bundle object with id %s from sync service",
					objectMetaData.ObjectID))
				continue
			}

			policiesBundle := &dataTypes.PoliciesBundle{}
			err := json.Unmarshal(buffer.Bytes(), policiesBundle)
			if err != nil {
				log.Println(fmt.Sprintf("failed to parse policy bundle object with id %s from sync service",
					objectMetaData.ObjectID))
				continue
			}
			s.policiesUpdateChan <- policiesBundle
			err = s.client.MarkObjectReceived(objectMetaData)
			if err != nil {
				log.Println("failed to report object consumed to sync service")
			}
		}
	}
}
