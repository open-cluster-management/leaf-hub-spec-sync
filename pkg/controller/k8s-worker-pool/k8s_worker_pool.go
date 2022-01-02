package k8sworkerpool

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	envVarK8sClientsPoolSize  = "K8S_CLIENTS_POOL_SIZE"
	defaultK8sClientsPoolSize = 10
)

// AddK8sWorkerPool adds k8s workers pool to the manager and returns it.
func AddK8sWorkerPool(log logr.Logger, mgr ctrl.Manager) (*K8sWorkerPool, error) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in cluster kubeconfig - %w", err)
	}

	k8sWorkerPoolSize := readEnvVars(log)

	k8sWorkerPool := &K8sWorkerPool{
		log:        log,
		kubeConfig: config,
		jobsQueue:  make(chan *K8sJob, k8sWorkerPoolSize), // each worker can handle at most one job at a time.
		poolSize:   k8sWorkerPoolSize,
	}

	if err := mgr.Add(k8sWorkerPool); err != nil {
		return nil, fmt.Errorf("failed to initialize k8s workers pool - %w", err)
	}

	return k8sWorkerPool, nil
}

func readEnvVars(log logr.Logger) int {
	k8sClientsPoolSize, found := os.LookupEnv(envVarK8sClientsPoolSize)
	if !found {
		log.Info(fmt.Sprintf("env variable %s not found, using default value %d", envVarK8sClientsPoolSize,
			defaultK8sClientsPoolSize))

		return defaultK8sClientsPoolSize
	}

	value, err := strconv.Atoi(k8sClientsPoolSize)
	if err != nil || value < 1 {
		log.Info(fmt.Sprintf("env var %s invalid value: %s, using default value %d", envVarK8sClientsPoolSize,
			k8sClientsPoolSize, defaultK8sClientsPoolSize))

		return defaultK8sClientsPoolSize
	}

	return value
}

// K8sWorkerPool pool that creates all k8s workers and the assigns k8s jobs to available workers.
type K8sWorkerPool struct {
	log        logr.Logger
	kubeConfig *rest.Config
	jobsQueue  chan *K8sJob
	poolSize   int
}

// Start function starts the k8s workers pool.
func (pool *K8sWorkerPool) Start(ctx context.Context) error {
	for i := 1; i <= pool.poolSize; i++ {
		worker, err := newK8sWorker(pool.log, i, pool.kubeConfig, pool.jobsQueue)
		if err != nil {
			return fmt.Errorf("failed to start k8s workers pool - %w", err)
		}

		worker.start(ctx)
	}

	<-ctx.Done() // blocking wait for stop event

	// context was cancelled, do cleanup
	close(pool.jobsQueue)

	return nil
}

// RunAsync inserts the K8sJob into the working queue.
func (pool *K8sWorkerPool) RunAsync(job *K8sJob) {
	pool.jobsQueue <- job
}
