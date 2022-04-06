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

	"github.com/go-logr/logr"
	"github.com/open-horizon/edge-sync-service-client/client"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	compressor "github.com/stolostron/hub-of-hubs-message-compression"
	"github.com/stolostron/hub-of-hubs-message-compression/compressors"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
	compressionHeaderTokensLength    = 2
	defaultCompressionType           = compressor.NoOp
	unregisteredObjectBundlesID      = "*"
)

var (
	errEnvVarNotFound         = errors.New("environment variable not found")
	errMissingCompressionType = errors.New("compression type is missing from message description")
	errSyncServiceReadFailed  = errors.New("sync service error")
)

// NewSyncService creates a new instance of SyncService.
func NewSyncService(log logr.Logger, objectsBundleRegistration *transport.BundleRegistration) (*SyncService, error) {
	serverProtocol, host, port, pollingInterval, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &SyncService{
		log:                 log,
		client:              syncServiceClient,
		compressorsMap:      make(map[compressor.CompressionType]compressors.Compressor),
		pollingInterval:     pollingInterval,
		bundlesMetaDataChan: make(chan *client.ObjectMetaData),
		bundleIDToRegistrationMap: map[string]*transport.BundleRegistration{
			unregisteredObjectBundlesID: objectsBundleRegistration,
		},
		commitMap:  make(map[string]*client.ObjectMetaData),
		ctx:        ctx,
		cancelFunc: cancelFunc,
		lock:       sync.Mutex{},
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
	if err != nil || port < 0 {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w", envVarSyncServicePort,
			err)
	}

	pollingIntervalString, found := os.LookupEnv(envVarSyncServicePollingInterval)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServicePollingInterval)
	}

	pollingInterval, err := strconv.Atoi(pollingIntervalString)
	if err != nil || pollingInterval < 0 {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid interval - %w",
			envVarSyncServicePollingInterval, err)
	}

	return protocol, host, uint16(port), pollingInterval, nil
}

// SyncService abstracts Sync Service client.
type SyncService struct {
	log                       logr.Logger
	client                    *client.SyncServiceClient
	compressorsMap            map[compressor.CompressionType]compressors.Compressor
	pollingInterval           int
	bundlesMetaDataChan       chan *client.ObjectMetaData
	bundleIDToRegistrationMap map[string]*transport.BundleRegistration
	// map from object key to metadata. size limited at all times.
	commitMap map[string]*client.ObjectMetaData

	ctx        context.Context
	cancelFunc context.CancelFunc
	startOnce  sync.Once
	stopOnce   sync.Once
	lock       sync.Mutex
}

// Start function starts sync service.
func (s *SyncService) Start() {
	s.startOnce.Do(func() {
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

// Register function registers a bundles channel to a msgID.
func (s *SyncService) Register(msgID string, bundleRegistration *transport.BundleRegistration) {
	s.bundleIDToRegistrationMap[msgID] = bundleRegistration
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
			// sync-service does not need to check for leafHubName since we assume actual selective-distribution.
			compressionType := s.getObjectCompressionType(objectMetaData)

			decompressedPayload, err := s.decompressPayload(buf.Bytes(), compressionType)
			if err != nil {
				s.logError(err, "failed to decompress bundle bytes", objectMetaData)
				continue
			}

			bundleRegistration, found := s.bundleIDToRegistrationMap[objectMetaData.ObjectID]
			if !found {
				bundleRegistration = s.bundleIDToRegistrationMap[unregisteredObjectBundlesID]
			}

			receivedBundle := bundleRegistration.CreateBundleFunc()
			if err := json.Unmarshal(decompressedPayload, &receivedBundle); err != nil {
				s.logError(err, "failed to parse bundle", objectMetaData)
			}

			bundleRegistration.BundleUpdatesChan <- receivedBundle

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

func (s *SyncService) getObjectCompressionType(objectMetaData *client.ObjectMetaData) compressor.CompressionType {
	if objectMetaData.Description != "" { // obj desc is Content-Encoding:type
		compressionTokens := strings.Split(objectMetaData.Description, ":")
		if len(compressionTokens) != compressionHeaderTokensLength {
			s.logError(errMissingCompressionType, "invalid compression header (Description)",
				objectMetaData)

			return defaultCompressionType
		}

		return compressor.CompressionType(compressionTokens[1])
	}

	return defaultCompressionType
}

func (s *SyncService) decompressPayload(payload []byte, compressionType compressor.CompressionType) ([]byte, error) {
	msgCompressor, found := s.compressorsMap[compressionType]
	if !found {
		newCompressor, err := compressor.NewCompressor(compressionType)
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		s.compressorsMap[compressionType] = msgCompressor
	}

	decompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress message: %w", err)
	}

	return decompressedBytes, nil
}
