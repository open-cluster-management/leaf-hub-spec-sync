package k8sworkerpool

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newK8sWorker(log logr.Logger, workerID int, kubeConfig *rest.Config, jobsQueue chan *K8sJob) (*k8sWorker, error) {
	k8sClient, err := client.New(kubeConfig, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s worker - %w", err)
	}

	return newK8sWorkerWithClient(log, workerID, k8sClient, jobsQueue), nil
}

func newK8sWorkerWithClient(log logr.Logger, workerID int, k8sClient client.Client, jobsQueue chan *K8sJob) *k8sWorker {
	return &k8sWorker{
		log:       log,
		workerID:  workerID,
		k8sClient: k8sClient,
		jobsQueue: jobsQueue,
	}
}

// k8sWorker worker within the K8s Worker pool. runs as a goroutine and invokes K8sJobs.
type k8sWorker struct {
	log       logr.Logger
	workerID  int
	k8sClient client.Client
	jobsQueue chan *K8sJob
}

func (worker *k8sWorker) start(ctx context.Context) {
	go func() {
		worker.log.Info("started worker", "WorkerID", worker.workerID)

		for {
			select {
			case <-ctx.Done(): // we have received a signal to stop
				return

			case job := <-worker.jobsQueue: // k8sWorker received a job request.
				job.handlerFunc(ctx, worker.k8sClient, job.obj)
			}
		}
	}()
}
