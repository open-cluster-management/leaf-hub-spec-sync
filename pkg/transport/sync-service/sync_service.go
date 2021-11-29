package syncservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	compressor "github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
	compressionHeaderTokensLength    = 2
	committerInterval                = time.Second * 20
)

var (
	errEnvVarNotFound         = errors.New("environment variable not found")
	errMissingCompressionType = errors.New("compression type is missing from message description")
	errSyncServiceReadFailed  = errors.New("sync service error")
)

// NewSyncService creates a new instance of SyncService.
func NewSyncService(log logr.Logger, bundleUpdatesChan chan *bundle.Bundle) (*SyncService, error) {
	serverProtocol, host, port, pollingInterval, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &SyncService{
		log:                       log,
		client:                    syncServiceClient,
		compressorsMap:            make(map[string]compressors.Compressor),
		pollingInterval:           pollingInterval,
		bundlesMetaDataChan:       make(chan *client.ObjectMetaData),
		bundlesUpdatesChan:        bundleUpdatesChan,
		objectMetadataToCommitMap: make(map[string]*client.ObjectMetaData),
		ctx:                       ctx,
		cancelFunc:                cancelFunc,
		lock:                      sync.Mutex{},
	}, nil
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

// SyncService abstracts Sync Service client.
type SyncService struct {
	log                       logr.Logger
	client                    *client.SyncServiceClient
	compressorsMap            map[string]compressors.Compressor
	pollingInterval           int
	bundlesMetaDataChan       chan *client.ObjectMetaData
	bundlesUpdatesChan        chan *bundle.Bundle
	objectMetadataToCommitMap map[string]*client.ObjectMetaData // size limited at all times (low)

	ctx        context.Context
	cancelFunc context.CancelFunc
	startOnce  sync.Once
	stopOnce   sync.Once
	lock       sync.Mutex
}

// Start function starts sync service.
func (s *SyncService) Start() {
	s.startOnce.Do(func() {
		go s.handleCommits(s.ctx)
		go s.handleBundles(s.ctx)
	})
}

// Stop function stops sync service.
func (s *SyncService) Stop() {
	s.stopOnce.Do(func() {
		s.cancelFunc()
		close(s.bundlesMetaDataChan)
	})
}

// CommitAsync commits a transported message that was processed locally.
func (s *SyncService) CommitAsync(metadata transport.BundleMetadata) {
	objectMetadata, ok := metadata.(client.ObjectMetaData)
	if !ok {
		return // shouldn't happen
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.objectMetadataToCommitMap[fmt.Sprintf("%s.%s", objectMetadata.ObjectType,
		objectMetadata.ObjectID)] = &objectMetadata
}

func (s *SyncService) handleCommits(ctx context.Context) {
	ticker := time.NewTicker(committerInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			s.commitMappedObjectMetadata()
		}
	}
}

func (s *SyncService) commitMappedObjectMetadata() {
	if objectMetadataToCommit := s.getObjectMetadataToCommit(); objectMetadataToCommit != nil {
		// commit object-metadata
		for _, objectMetadata := range objectMetadataToCommit {
			if err := s.client.MarkObjectConsumed(objectMetadata); err != nil {
				// if one fails, we assume the rest will too
				s.log.Error(err, "failed to commit ObjectMetadata batch", "ObjectMetadata", objectMetadataToCommit)
			}
		}

		// update metadata map, delete what's been committed
		s.removeCommittedObjectMetadataFromMap(objectMetadataToCommit)
	}
}

func (s *SyncService) getObjectMetadataToCommit() []*client.ObjectMetaData {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.objectMetadataToCommitMap) == 0 {
		return nil
	}

	objectMetadataToCommit := make([]*client.ObjectMetaData, 0, len(s.objectMetadataToCommitMap))
	for _, objectMetadata := range s.objectMetadataToCommitMap {
		objectMetadataToCommit = append(objectMetadataToCommit, objectMetadata)
	}

	return objectMetadataToCommit
}

func (s *SyncService) removeCommittedObjectMetadataFromMap(committedObjectMetadata []*client.ObjectMetaData) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, objectMetadata := range committedObjectMetadata {
		objectIdentifier := fmt.Sprintf("%s.%s", objectMetadata.ObjectType, objectMetadata.ObjectID)
		if s.objectMetadataToCommitMap[objectIdentifier] == objectMetadata {
			// no new object processed for this type, delete from map
			delete(s.objectMetadataToCommitMap, objectIdentifier)
		}
	}

	s.log.Info("committed objects", "ObjectMetadata", committedObjectMetadata)
}

func (s *SyncService) handleBundles(ctx context.Context) {
	// register for updates for spec bundles and config objects, includes all types of spec bundles.
	s.client.StartPollingForUpdates(datatypes.SpecBundle, s.pollingInterval, s.bundlesMetaDataChan)

	for {
		select {
		case <-ctx.Done():
			return
		case objectMetaData := <-s.bundlesMetaDataChan:
			var buf bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buf) {
				s.logError(errSyncServiceReadFailed, "failed to read bundle from sync service", objectMetaData)
				continue
			}

			compressionTokens := strings.Split(objectMetaData.Description, ":") // obj desc is Content-Encoding:type
			if len(compressionTokens) != compressionHeaderTokensLength {
				s.logError(errMissingCompressionType, "missing compression header", objectMetaData)
				continue
			}

			decompressedPayload, err := s.decompressPayload(buf.Bytes(), compressionTokens[1])
			if err != nil {
				s.logError(err, "failed to decompress bundle bytes", objectMetaData)
				continue
			}

			receivedBundle := &bundle.ObjectsBundle{}
			if err := json.Unmarshal(decompressedPayload, receivedBundle); err != nil {
				s.logError(err, "failed to parse bundle", objectMetaData)
				continue
			}

			s.bundlesUpdatesChan <- &bundle.Bundle{
				ObjectsBundle:  receivedBundle,
				BundleMetadata: objectMetaData,
			}

			if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
				s.logError(err, "failed to report object received to sync service", objectMetaData)
			}
		}
	}
}

func (s *SyncService) logError(err error, errMsg string, objectMetaData *client.ObjectMetaData) {
	s.log.Error(err, errMsg, "ObjectID", objectMetaData.ObjectID, "ObjectType", objectMetaData.ObjectType,
		"ObjectDescription", objectMetaData.Description, "Version", objectMetaData.Version)
}

func (s *SyncService) decompressPayload(payload []byte, msgCompressorType string) ([]byte, error) {
	msgCompressor, found := s.compressorsMap[msgCompressorType]
	if !found {
		newCompressor, err := compressor.NewCompressor(compressor.CompressionType(msgCompressorType))
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		s.compressorsMap[msgCompressorType] = msgCompressor
	}

	decompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress message: %w", err)
	}

	return decompressedBytes, nil
}
