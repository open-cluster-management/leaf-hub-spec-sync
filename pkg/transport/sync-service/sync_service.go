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
	"github.com/stolostron/leaf-hub-spec-sync/pkg/bundle"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport/helpers"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
	compressionHeaderTokensLength    = 2
)

var (
	errEnvVarNotFound         = errors.New("environment variable not found")
	errMissingCompressionType = errors.New("compression type is missing from message description")
	errInvalidCompressionType = errors.New("invalid compression header (Description)")
	errSyncServiceReadFailed  = errors.New("sync service error")
)

// NewSyncService creates a new instance of SyncService.
func NewSyncService(log logr.Logger, genericBundlesUpdatesChan chan *bundle.GenericBundle) (*SyncService, error) {
	serverProtocol, host, port, pollingInterval, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &SyncService{
		log:                             log,
		client:                          syncServiceClient,
		compressorsMap:                  make(map[compressor.CompressionType]compressors.Compressor),
		pollingInterval:                 pollingInterval,
		bundlesMetaDataChan:             make(chan *client.ObjectMetaData),
		genericBundlesUpdatesChan:       genericBundlesUpdatesChan,
		customBundleIDToRegistrationMap: make(map[string]*transport.CustomBundleRegistration),
		commitMap:                       make(map[string]*client.ObjectMetaData),
		ctx:                             ctx,
		cancelFunc:                      cancelFunc,
		lock:                            sync.Mutex{},
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
	log                             logr.Logger
	client                          *client.SyncServiceClient
	compressorsMap                  map[compressor.CompressionType]compressors.Compressor
	pollingInterval                 int
	bundlesMetaDataChan             chan *client.ObjectMetaData
	genericBundlesUpdatesChan       chan *bundle.GenericBundle
	customBundleIDToRegistrationMap map[string]*transport.CustomBundleRegistration
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

// Register function registers a bundle ID to a CustomBundleRegistration.
func (s *SyncService) Register(msgID string, customBundleRegistration *transport.CustomBundleRegistration) {
	s.customBundleIDToRegistrationMap[msgID] = customBundleRegistration
}

func (s *SyncService) handleBundles(ctx context.Context) {
	// register for updates for spec bundles and config objects, includes all types of spec bundles.
	s.client.StartPollingForUpdates(datatypes.SpecBundle, s.pollingInterval, s.bundlesMetaDataChan)

	for {
		select {
		case <-ctx.Done():
			return
		case objectMetaData := <-s.bundlesMetaDataChan:
			if err := s.handleBundle(objectMetaData); err != nil {
				s.logError(err, "failed to handle bundle", objectMetaData)
			}
		}
	}
}

func (s *SyncService) handleBundle(objectMetaData *client.ObjectMetaData) error {
	var buf bytes.Buffer
	if !s.client.FetchObjectData(objectMetaData, &buf) {
		return errSyncServiceReadFailed
	}
	// sync-service does not need to check for leafHubName since we assume actual selective-distribution.
	decompressedPayload, err := s.decompressPayload(buf.Bytes(), objectMetaData)
	if err != nil {
		return fmt.Errorf("failed to decompress bundle bytes - %w", err)
	}

	// if selective distribution was applied msgID would contain "LH_ID." prefix
	msgID := strings.TrimPrefix(objectMetaData.ObjectID, fmt.Sprintf("%s.", objectMetaData.DestID))

	customBundleRegistration, found := s.customBundleIDToRegistrationMap[msgID]
	if !found { // received generic bundle
		if err := s.syncGenericBundle(decompressedPayload); err != nil {
			return fmt.Errorf("failed to sync generic bundle - %w", err)
		}
	} else {
		if err := helpers.SyncCustomBundle(customBundleRegistration, decompressedPayload); err != nil {
			return fmt.Errorf("failed to sync custom bundle - %w", err)
		}
	}
	// mark received
	if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
		return fmt.Errorf("failed to report object received to sync service - %w", err)
	}

	return nil
}

func (s *SyncService) syncGenericBundle(payload []byte) error {
	receivedBundle := bundle.NewGenericBundle()
	if err := json.Unmarshal(payload, receivedBundle); err != nil {
		return fmt.Errorf("failed to parse bundle - %w", err)
	}

	s.genericBundlesUpdatesChan <- receivedBundle

	return nil
}

func (s *SyncService) logError(err error, errMsg string, objectMetaData *client.ObjectMetaData) {
	s.log.Error(err, errMsg, "ObjectID", objectMetaData.ObjectID, "ObjectType", objectMetaData.ObjectType,
		"ObjectDescription", objectMetaData.Description, "Version", objectMetaData.Version)
}

func (s *SyncService) decompressPayload(payload []byte, objectMetaData *client.ObjectMetaData) ([]byte, error) {
	compressionType, err := s.getObjectCompressionType(objectMetaData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress message: %w", err)
	}

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

func (s *SyncService) getObjectCompressionType(objectMetaData *client.ObjectMetaData,
) (compressor.CompressionType, error) {
	if objectMetaData.Description == "" { // obj desc is Content-Encoding:type
		return "", errMissingCompressionType
	}

	compressionTokens := strings.Split(objectMetaData.Description, ":")
	if len(compressionTokens) != compressionHeaderTokensLength {
		return "", errInvalidCompressionType
	}

	return compressor.CompressionType(compressionTokens[1]), nil
}
