package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller"
	lhSyncService "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport/sync-service"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                              = "0.0.0.0"
	metricsPort                        int32 = 9435
	envVarControllerNamespace                = "POD_NAMESPACE"
	envVarBundlesSyncerNumberOfClients       = "BUNDLES_SYNCER_NUMBER_OF_CLIENTS"
	leaderElectionLockName                   = "leaf-hub-spec-sync-lock"
	defaultNumberOfClients                   = 10
)

func printVersion(log logr.Logger) {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	log.Info(fmt.Sprintf("Version of operator-sdk: %v", sdkVersion.Version))
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

	numOfClients := defaultNumberOfClients

	if envNumOfClients, found := os.LookupEnv(envVarBundlesSyncerNumberOfClients); found {
		if value, err := strconv.Atoi(envNumOfClients); err != nil {
			log.Error(err, fmt.Sprintf("Failed to convert environment variable '%s', value: %s. Using default %d.",
				envVarBundlesSyncerNumberOfClients, envNumOfClients, defaultNumberOfClients))
		} else {
			numOfClients = value
		}
	}

	// transport layer initialization
	bundleUpdatesChan := make(chan *bundle.ObjectsBundle)
	defer close(bundleUpdatesChan)

	syncService, err := lhSyncService.NewSyncService(ctrl.Log.WithName("sync-service"), bundleUpdatesChan)
	if err != nil {
		log.Error(err, "initialization error", "failed to initialize", "SyncService")
		return 1
	}

	mgr, err := createManager(leaderElectionNamespace, metricsHost, metricsPort, bundleUpdatesChan, numOfClients)
	if err != nil {
		log.Error(err, "Failed to create manager")
		return 1
	}

	syncService.Start()
	defer syncService.Stop()

	log.Info("Starting the Cmd.")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager exited non-zero")
		return 1
	}

	return 0
}

func createManager(leaderElectionNamespace, metricsHost string, metricsPort int32,
	bundleUpdatesChan chan *bundle.ObjectsBundle, numOfClients int) (ctrl.Manager, error) {
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

	if err := controller.AddSpecSyncers(mgr, bundleUpdatesChan, numOfClients); err != nil {
		return nil, fmt.Errorf("failed to add spec syncers: %w", err)
	}

	return mgr, nil
}

func main() {
	os.Exit(doMain())
}
