package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/controller"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
	lhSyncService "github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport/sync-service"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                     = "0.0.0.0"
	metricsPort               int32 = 9435
	envVarControllerNamespace       = "POD_NAMESPACE"
	leaderElectionLockName          = "leaf-hub-spec-sync-lock"
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

	// transport layer initialization
	bundleUpdatesChan := make(chan *bundle.ObjectsBundle)
	defer close(bundleUpdatesChan)

	syncService, err := lhSyncService.NewSyncService(ctrl.Log.WithName("sync-service"), bundleUpdatesChan)
	if err != nil {
		log.Error(err, "initialization error", "failed to initialize", "SyncService")
		return 1
	}

	mgr, err := createManager(syncService, leaderElectionNamespace, metricsHost, metricsPort, bundleUpdatesChan)
	if err != nil {
		log.Error(err, "Failed to create manager")
		return 1
	}

	log.Info("Starting the Cmd.")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager exited non-zero")
		return 1
	}

	return 0
}

func createManager(transport transport.Transport, leaderElectionNamespace, metricsHost string, metricsPort int32,
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

	err = mgr.Add(transport)
	if err != nil {
		return nil, fmt.Errorf("failed to add transport: %w", err)
	}

	return mgr, nil
}

func main() {
	os.Exit(doMain())
}
