package k8sworkerpool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/helpers"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/controller/rbac"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	envVarEnforceHohRbac      = "ENFORCE_HOH_RBAC"
	envVarK8sClientsPoolSize  = "K8S_CLIENTS_POOL_SIZE"
	defaultK8sClientsPoolSize = 10
)

var (
	errEnvVarNotFound = errors.New("environment variable not found")
	errEnvVarInvalid  = errors.New("environment variable has invalid value")
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
	enforceHohRbac, k8sWorkerPoolSize, err := readEnvVars(log)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s workers pool - %w", err)
	}

	k8sWorkerPool := &K8sWorkerPool{
		log:                        log,
		kubeConfig:                 config,
		jobsQueue:                  make(chan *K8sJob, k8sWorkerPoolSize), // each worker can handle at most one job at a time
		poolSize:                   k8sWorkerPoolSize,
		enforceHohRbac:             enforceHohRbac,
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

func readEnvVars(log logr.Logger) (bool, int, error) {
	enforceHohRbacString, found := os.LookupEnv(envVarEnforceHohRbac)
	if !found {
		return false, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarEnforceHohRbac)
	}

	enforceHohRbac, err := strconv.ParseBool(enforceHohRbacString)
	if err != nil {
		return false, 0, fmt.Errorf("%w: %s", errEnvVarInvalid, envVarEnforceHohRbac)
	}

	k8sClientsPoolSizeString, found := os.LookupEnv(envVarK8sClientsPoolSize)
	if !found {
		log.Info(fmt.Sprintf("env variable %s not found, using default value %d", envVarK8sClientsPoolSize,
			defaultK8sClientsPoolSize))

		return enforceHohRbac, defaultK8sClientsPoolSize, nil
	}

	poolSize, err := strconv.Atoi(k8sClientsPoolSizeString)
	if err != nil || poolSize < 1 {
		log.Info(fmt.Sprintf("env var %s invalid value: %s, using default value %d", envVarK8sClientsPoolSize,
			k8sClientsPoolSizeString, defaultK8sClientsPoolSize))

		poolSize = defaultK8sClientsPoolSize
	}

	return enforceHohRbac, poolSize, nil
}

// K8sWorkerPool pool that creates all k8s workers and the assigns k8s jobs to available workers.
type K8sWorkerPool struct {
	ctx                        context.Context
	log                        logr.Logger
	kubeConfig                 *rest.Config
	jobsQueue                  chan *K8sJob
	poolSize                   int
	enforceHohRbac             bool
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
	if userIdentity == rbac.NoUserIdentity {
		pool.jobsQueue <- pool.wrapK8sJob(job) // handle the ns doesn't exist use case.
		return
	}
	// otherwise, need to impersonate and use the specific worker to enforce permissions.
	pool.impersonationWorkersLock.Lock()

	if _, found := pool.impersonationWorkersQueues[userIdentity]; !found {
		if err := pool.createUserWorker(userIdentity); err != nil {
			pool.log.Error(err, "failed to create user worker", "user", userIdentity)
			return
		}
	}
	// push the job to the queue of the specific worker that uses the user identity
	workerQueue := pool.impersonationWorkersQueues[userIdentity]

	pool.impersonationWorkersLock.Unlock()
	// since this call might get blocking, first Unlock, then try to insert job into queue
	workerQueue <- pool.wrapK8sJob(job)
}

func (pool *K8sWorkerPool) createUserWorker(userIdentity string) error {
	k8sClient, err := pool.impersonationManager.Impersonate(userIdentity)
	if err != nil {
		return fmt.Errorf("failed to impersonate - %w", err)
	}

	workerQueue := make(chan *K8sJob, pool.poolSize)
	worker := newK8sWorkerWithClient(pool.log.WithName(fmt.Sprintf("impersonation-%s", userIdentity)),
		1, k8sClient, workerQueue)
	worker.start(pool.ctx)
	pool.impersonationWorkersQueues[userIdentity] = workerQueue

	return nil
}

func (pool *K8sWorkerPool) wrapK8sJob(job *K8sJob) *K8sJob {
	if pool.enforceHohRbac { // if enforce hoh rbac is true, do not handle failures using the controllers workers.
		return job
	}
	// else, HoH RBAC shouldn't be enforced. need to take care of namespace not exist or insufficient permissions
	return NewK8sJob(job.obj, func(ctx context.Context, impersonationClient client.Client,
		obj *unstructured.Unstructured) error {
		err := job.handlerFunc(ctx, impersonationClient, obj) // try first to invoke job as is using the user identity.
		if err == nil {                                       // no error, ns exists + user has sufficient permissions.
			return nil // no need to do anything.
		}
		// otherwise, create a new K8s job to be run by one of the controllers workers (using the controller identity).
		ctrlJob := NewK8sJob(obj, func(ctx context.Context, ctrlClient client.Client,
			obj *unstructured.Unstructured) error {
			if err := helpers.CreateNamespaceIfNotExist(ctx, ctrlClient, obj.GetNamespace()); err != nil {
				return fmt.Errorf("failed to handle k8s job - %w", err)
			}

			return job.handlerFunc(ctx, ctrlClient, obj)
		})
		// this has the retry effect, but with the controller worker.
		pool.jobsQueue <- ctrlJob

		return nil
	})
}
