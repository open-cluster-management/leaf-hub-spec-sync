package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	kafka "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport/kafka"
	lhSyncService "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport/sync-service"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                        = "0.0.0.0"
	metricsPort                  int32 = 9435
	kafkaTransportTypeName             = "kafka"
	syncServiceTransportTypeName       = "sync-service"
	envVarControllerNamespace          = "POD_NAMESPACE"
	envVarTransportType                = "TRANSPORT_TYPE"
	leaderElectionLockName             = "leaf-hub-spec-sync-lock"
)

var errEnvVarIllegalValue = errors.New("environment variable illegal value")

func printVersion(log logr.Logger) {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	log.Info(fmt.Sprintf("Version of operator-sdk: %v", sdkVersion.Version))
}

// function to choose transport type based on env var.
func getTransport(transportType string, bundleUpdatesChan chan *bundle.ObjectsBundle) (transport.Transport, error) {
	switch transportType {
	case kafkaTransportTypeName:
		kafkaConsumer, err := kafka.NewConsumer(ctrl.Log.WithName("kafka"), bundleUpdatesChan)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka-consumer: %w", err)
		}

		return kafkaConsumer, nil
	case syncServiceTransportTypeName:
		syncService, err := lhSyncService.NewSyncService(ctrl.Log.WithName("sync-service"), bundleUpdatesChan)
		if err != nil {
			return nil, fmt.Errorf("failed to create sync-service: %w", err)
		}

		return syncService, nil
	default:
		return nil, fmt.Errorf("%w: %s", errEnvVarIllegalValue, transportType)
	}
}

// function to handle defers with exit, see https://stackoverflow.com/a/27629493/553720.
func doMain() int {
	pflag.CommandLine.AddFlagSet(zap.FlagSet())
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	ctrl.SetLogger(zap.Logger())
	log := ctrl.Log.WithName("cmd")

	printVersion(log)

	leaderElectionNamespace, found := os.LookupEnv(envVarControllerNamespace)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarControllerNamespace)
		return 1
	}

	transportType, found := os.LookupEnv(envVarTransportType)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarTransportType)
		return 1
	}

	// transport layer initialization
	bundleUpdatesChan := make(chan *bundle.ObjectsBundle)
	defer close(bundleUpdatesChan)

	transportObj, err := getTransport(transportType, bundleUpdatesChan)
	if err != nil {
		log.Error(err, "transport initialization error")
		return 1
	}

	mgr, err := createManager(leaderElectionNamespace, metricsHost, metricsPort, bundleUpdatesChan)
	if err != nil {
		log.Error(err, "Failed to create manager")
		return 1
	}

	if err := transportObj.Start(); err != nil {
		log.Error(err, "transport start failure")
		return 1
	}
	defer transportObj.Stop()

	log.Info("Starting the Cmd.")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager exited non-zero")
		return 1
	}

	return 0
}

func createManager(leaderElectionNamespace, metricsHost string, metricsPort int32,
	bundleUpdatesChan chan *bundle.ObjectsBundle) (ctrl.Manager, error) {
	options := ctrl.Options{
		MetricsBindAddress:      fmt.Sprintf("%s:%d", metricsHost, metricsPort),
		LeaderElection:          true,
		LeaderElectionID:        leaderElectionLockName,
		LeaderElectionNamespace: leaderElectionNamespace,
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		return nil, fmt.Errorf("failed to create a new manager: %w", err)
	}

	if err := controller.AddSpecSyncers(mgr, bundleUpdatesChan); err != nil {
		return nil, fmt.Errorf("failed to add spec syncers: %w", err)
	}

	return mgr, nil
}

func main() {
	os.Exit(doMain())
}
