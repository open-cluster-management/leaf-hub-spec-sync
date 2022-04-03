package k8sworkerpool

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/rbac"
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

	// pool size is used for controller workers.
	// for impersonation workers we have additional workers, one per impersonated user.
	poolSize := readEnvVars(log)

	k8sWorkerPool := &K8sWorkerPool{
		log:                        log,
		kubeConfig:                 config,
		jobsQueue:                  make(chan *K8sJob, poolSize), // each worker can handle at most one job at a time
		poolSize:                   poolSize,
		initializationWaitingGroup: sync.WaitGroup{},
		impersonationManager:       rbac.NewImpersonationManager(config),
		impersonationWorkersQueues: make(map[string]chan *K8sJob),
		impersonationWorkersLock:   sync.Mutex{},
	}

	k8sWorkerPool.initializationWaitingGroup.Add(1)

	if err := mgr.Add(k8sWorkerPool); err != nil {
		return nil, fmt.Errorf("failed to initialize k8s workers pool - %w", err)
	}

	return k8sWorkerPool, nil
}

func readEnvVars(log logr.Logger) int {
	k8sClientsPoolSizeString, found := os.LookupEnv(envVarK8sClientsPoolSize)
	if !found {
		log.Info(fmt.Sprintf("env variable %s not found, using default value %d", envVarK8sClientsPoolSize,
			defaultK8sClientsPoolSize))

		return defaultK8sClientsPoolSize
	}

	poolSize, err := strconv.Atoi(k8sClientsPoolSizeString)
	if err != nil || poolSize < 1 {
		log.Info(fmt.Sprintf("env var %s invalid value: %s, using default value %d", envVarK8sClientsPoolSize,
			k8sClientsPoolSizeString, defaultK8sClientsPoolSize))

		return defaultK8sClientsPoolSize
	}

	return poolSize
}

// K8sWorkerPool pool that creates all k8s workers and the assigns k8s jobs to available workers.
type K8sWorkerPool struct {
	ctx                        context.Context
	log                        logr.Logger
	kubeConfig                 *rest.Config
	jobsQueue                  chan *K8sJob
	poolSize                   int
	initializationWaitingGroup sync.WaitGroup
	impersonationManager       *rbac.ImpersonationManager
	impersonationWorkersQueues map[string]chan *K8sJob
	impersonationWorkersLock   sync.Mutex
}

// Start function starts the k8s workers pool.
func (pool *K8sWorkerPool) Start(ctx context.Context) error {
	pool.ctx = ctx
	pool.initializationWaitingGroup.Done() // once context is saved, it's safe to let RunAsync work with no concerns.

	for i := 1; i <= pool.poolSize; i++ {
		worker, err := newK8sWorker(pool.log, i, pool.kubeConfig, pool.jobsQueue)
		if err != nil {
			return fmt.Errorf("failed to start k8s workers pool - %w", err)
		}

		worker.start(ctx)
	}

	<-ctx.Done() // blocking wait for stop event

	// context was cancelled, do cleanup
	for _, workerQueue := range pool.impersonationWorkersQueues {
		close(workerQueue)
	}

	close(pool.jobsQueue)

	return nil
}

// RunAsync inserts the K8sJob into the working queue.
func (pool *K8sWorkerPool) RunAsync(job *K8sJob) {
	pool.initializationWaitingGroup.Wait() // start running jobs only after some initialization steps have finished.

	userIdentity, err := pool.impersonationManager.GetUserIdentity(job.obj)
	if err != nil {
		pool.log.Error(err, "failed to get user identity from obj")
		return
	}
	// if it doesn't contain impersonation info, let the controller worker pool handle it.
	if userIdentity == rbac.NoIdentity {
		pool.jobsQueue <- job
		return
	}
	// otherwise, need to impersonate and use the specific worker to enforce permissions.
	base64UserGroups, userGroups, err := pool.impersonationManager.GetUserGroups(job.obj)
	if err != nil {
		pool.log.Error(err, "failed to get user groups from obj")
		return
	}

	pool.impersonationWorkersLock.Lock()

	workerIdentifier := fmt.Sprintf("%s.%s", userIdentity, base64UserGroups)

	if _, found := pool.impersonationWorkersQueues[workerIdentifier]; !found {
		if err := pool.createUserWorker(userIdentity, userGroups, workerIdentifier); err != nil {
			pool.log.Error(err, "failed to create user worker", "user", userIdentity)
			return
		}
	}
	// push the job to the queue of the specific worker that uses the user identity
	workerQueue := pool.impersonationWorkersQueues[workerIdentifier]

	pool.impersonationWorkersLock.Unlock()
	workerQueue <- job // since this call might get blocking, first Unlock, then try to insert job into queue
}

func (pool *K8sWorkerPool) createUserWorker(userIdentity string, userGroups []string, workerIdentifier string) error {
	k8sClient, err := pool.impersonationManager.Impersonate(userIdentity, userGroups)
	if err != nil {
		return fmt.Errorf("failed to impersonate - %w", err)
	}

	workerQueue := make(chan *K8sJob, pool.poolSize)
	worker := newK8sWorkerWithClient(pool.log.WithName(fmt.Sprintf("impersonation-%s", userIdentity)),
		1, k8sClient, workerQueue)
	worker.start(pool.ctx)
	pool.impersonationWorkersQueues[workerIdentifier] = workerQueue

	return nil
}
