/*
Copyright 2022 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubequota

import (
	"context"
	"fmt"
	"sync"
	"time"

	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
	kcpkubernetesinformers "github.com/kcp-dev/client-go/informers"
	kcpcorev1informers "github.com/kcp-dev/client-go/informers/core/v1"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"
	quota "k8s.io/apiserver/pkg/quota/v1"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/quota/v1/generic"

	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics/prometheus/ratelimiter"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/resourcequota"

	corev1alpha1 "github.com/kcp-dev/kcp/pkg/apis/core/v1alpha1"
	corev1alpha1informers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions/core/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/informer"
	"github.com/kcp-dev/kcp/pkg/logging"
)

const (
	ControllerName = "kcp-kube-quota"
)

type scopeableInformerFactory interface {
	Cluster(logicalcluster.Name) kcpkubernetesinformers.ScopedDynamicSharedInformerFactory
}

// Controller manages per-workspace resource quota controllers.
type Controller struct {
	queue workqueue.RateLimitingInterface

	dynamicDiscoverySharedInformerFactory *informer.DiscoveringDynamicSharedInformerFactory
	kubeClusterClient                     kcpkubernetesclientset.ClusterInterface
	informersStarted                      <-chan struct{}

	// quotaRecalculationPeriod controls how often a full quota recalculation is performed
	quotaRecalculationPeriod time.Duration
	// fullResyncPeriod controls how often the dynamic informers do a full resync
	fullResyncPeriod time.Duration

	workersPerLogicalCluster int

	// lock guards the fields in this group
	lock        sync.RWMutex
	cancelFuncs map[logicalcluster.Name]func()

	resourceQuotaClusterInformer        kcpcorev1informers.ResourceQuotaClusterInformer
	scopingGenericSharedInformerFactory scopeableInformerFactory
	kubeInformerFactory                 kcpkubernetesinformers.SharedInformerFactory

	// For better testability
	getLogicalCluster func(clusterName logicalcluster.Name) (*corev1alpha1.LogicalCluster, error)
}

// NewController creates a new Controller.
func NewController(
	logicalClusterInformer corev1alpha1informers.LogicalClusterClusterInformer,
	kubeClusterClient kcpkubernetesclientset.ClusterInterface,
	kubeInformerFactory kcpkubernetesinformers.SharedInformerFactory,
	dynamicDiscoverySharedInformerFactory *informer.DiscoveringDynamicSharedInformerFactory,
	quotaRecalculationPeriod time.Duration,
	fullResyncPeriod time.Duration,
	workersPerLogicalCluster int,
	informersStarted <-chan struct{},
) (*Controller, error) {
	c := &Controller{
		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), ControllerName),

		dynamicDiscoverySharedInformerFactory: dynamicDiscoverySharedInformerFactory,
		kubeClusterClient:                     kubeClusterClient,
		informersStarted:                      informersStarted,

		quotaRecalculationPeriod: quotaRecalculationPeriod,
		fullResyncPeriod:         fullResyncPeriod,

		workersPerLogicalCluster: workersPerLogicalCluster,

		cancelFuncs: map[logicalcluster.Name]func(){},

		scopingGenericSharedInformerFactory: dynamicDiscoverySharedInformerFactory,
		resourceQuotaClusterInformer:        kubeInformerFactory.Core().V1().ResourceQuotas(),
		kubeInformerFactory:                 kubeInformerFactory,
		getLogicalCluster: func(clusterName logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
			return logicalClusterInformer.Lister().Cluster(clusterName).Get(corev1alpha1.LogicalClusterName)
		},
	}

	logicalClusterInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: c.enqueue,
			UpdateFunc: func(oldObj, newObj interface{}) {
				c.enqueue(oldObj)
			},
			DeleteFunc: c.enqueue,
		},
	)

	return c, nil
}

// enqueue adds the key for a Workspace to the queue.
func (c *Controller) enqueue(obj interface{}) {
	key, err := kcpcache.DeletionHandlingMetaClusterNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	logger := logging.WithQueueKey(logging.WithReconciler(klog.Background(), ControllerName), key)
	logger.V(2).Info("queueing Workspace")
	c.queue.Add(key)
}

// Start starts the controller.
func (c *Controller) Start(ctx context.Context, numThreads int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := logging.WithReconciler(klog.FromContext(ctx), ControllerName)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("Starting controller")
	defer logger.Info("Shutting down controller")

	for i := 0; i < numThreads; i++ {
		go wait.UntilWithContext(ctx, c.startWorker, time.Second)
	}

	<-ctx.Done()
}

// startWorker runs a single worker goroutine.
func (c *Controller) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem waits for the queue to have a key available and then processes it.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	// Wait until there is a new item in the working queue
	raw, quit := c.queue.Get()
	if quit {
		return false
	}
	key := raw.(string)

	logger := logging.WithQueueKey(klog.FromContext(ctx), key)
	ctx = klog.NewContext(ctx, logger)
	logger.V(1).Info("processing key")

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(key)

	if err := c.process(ctx, key); err != nil {
		utilruntime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", ControllerName, key, err))
		c.queue.AddRateLimited(key)
		return true
	}

	c.queue.Forget(key)

	return true
}

// process processes a single key from the queue.
func (c *Controller) process(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	cluster, _, _, err := kcpcache.SplitMetaClusterNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(err)
		return nil
	}
	clusterName := logicalcluster.Name(cluster.String()) // TODO: remove when SplitMetaClusterNamespaceKey returns tenancy.Name

	logger = logger.WithValues("logicalCluster", clusterName.String())

	ws, err := c.getLogicalCluster(clusterName)
	if err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(2).Info("Workspace not found - stopping quota controller for it (if needed)")

			c.lock.Lock()
			cancel, ok := c.cancelFuncs[clusterName]
			if ok {
				cancel()
				delete(c.cancelFuncs, clusterName)
			}
			c.lock.Unlock()

			c.dynamicDiscoverySharedInformerFactory.Unsubscribe("quota-" + clusterName.String())

			return nil
		}

		return err
	}
	logger = logging.WithObject(logger, ws)

	c.lock.Lock()
	defer c.lock.Unlock()

	_, found := c.cancelFuncs[clusterName]
	if found {
		logger.V(4).Info("quota controller already exists")
		return nil
	}

	logger.V(2).Info("starting quota controller")

	ctx, cancel := context.WithCancel(ctx)
	ctx = klog.NewContext(ctx, logger)
	c.cancelFuncs[clusterName] = cancel

	if err := c.startQuotaForLogicalCluster(ctx, clusterName); err != nil {
		cancel()
		return fmt.Errorf("error starting quota controller for cluster %q: %w", clusterName, err)
	}

	return nil
}

func (c *Controller) startQuotaForLogicalCluster(ctx context.Context, clusterName logicalcluster.Name) error {
	logger := klog.FromContext(ctx)
	resourceQuotaControllerClient := c.kubeClusterClient.Cluster(clusterName.Path())
	// TODO(ncdc): find a way to support the default configuration. For now, don't use it, because it is difficult
	// to get support for the special evaluators for pods/services/pvcs.
	listerFuncForResource := func(namespace string) ([]runtime.Object, error) {
		deployments, err := c.kubeClusterClient.Cluster(clusterName.Path()).AppsV1().Deployments(namespace).List(ctx, v1.ListOptions{})
		if err != nil {
			return nil, err
		}
		objs := []runtime.Object{}
		for _, deploy := range deployments.Items {
			objs = append(objs, &deploy)
		}
		return objs, err
	}
	quotaConfiguration := NewQuotaConfigurationForControllers(listerFuncForResource)

	resourceQuotaControllerOptions := &resourcequota.ControllerOptions{
		QuotaClient:           resourceQuotaControllerClient.CoreV1(),
		ResourceQuotaInformer: c.resourceQuotaClusterInformer.Cluster(clusterName),
		ResyncPeriod:          controller.StaticResyncPeriodFunc(c.quotaRecalculationPeriod),
		InformerFactory:       c.scopingGenericSharedInformerFactory.Cluster(clusterName),
		ReplenishmentResyncPeriod: func() time.Duration {
			return c.fullResyncPeriod
		},
		DiscoveryFunc:        c.dynamicDiscoverySharedInformerFactory.ServerPreferredResources,
		IgnoredResourcesFunc: quotaConfiguration.IgnoredResources,
		InformersStarted:     c.informersStarted,
		Registry:             generic.NewRegistry(quotaConfiguration.Evaluators()),
		ClusterName:          clusterName,
	}
	if resourceQuotaControllerClient.CoreV1().RESTClient().GetRateLimiter() != nil {
		if err := ratelimiter.RegisterMetricAndTrackRateLimiterUsage(clusterName.String()+"-resource_quota_controller", resourceQuotaControllerClient.CoreV1().RESTClient().GetRateLimiter()); err != nil {
			return err
		}
	}

	resourceQuotaController, err := resourcequota.NewController(resourceQuotaControllerOptions)
	if err != nil {
		return err
	}

	// Here we diverge from what upstream does. Upstream starts a goroutine that retrieves discovery every 30 seconds,
	// starting/stopping dynamic informers as needed based on the updated discovery data. We know that kcp contains
	// the combination of built-in types plus CRDs. We use that information to drive what quota evaluates.

	quotaController := quotaController{
		clusterName: clusterName,
		queue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "quota-"+clusterName.String()),
		work: func(ctx context.Context) {
			resourceQuotaController.UpdateMonitors(ctx, c.dynamicDiscoverySharedInformerFactory.ServerPreferredResources)
		},
	}
	go quotaController.Start(ctx)

	apisChanged := c.dynamicDiscoverySharedInformerFactory.Subscribe("quota-" + clusterName.String())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-apisChanged:
				logger.V(4).Info("got API change notification")
				quotaController.queue.Add("resync") // this queue only ever has one key in it, as long as it's constant we are OK
			}
		}
	}()

	// Do this in a goroutine to avoid holding up a worker in the event UpdateMonitors stalls for whatever reason
	go func() {
		// Make sure the monitors are synced at least once
		resourceQuotaController.UpdateMonitors(ctx, c.dynamicDiscoverySharedInformerFactory.ServerPreferredResources)

		go resourceQuotaController.Run(ctx, c.workersPerLogicalCluster)
	}()

	return nil
}

type quotaController struct {
	clusterName    logicalcluster.Name
	queue          workqueue.RateLimitingInterface
	work           func(context.Context)
	previousCancel func()
}

// Start starts the controller, which stops when ctx.Done() is closed.
func (c *quotaController) Start(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := logging.WithReconciler(klog.FromContext(ctx), ControllerName+"-"+c.clusterName.String()+"-monitors")
	ctx = klog.NewContext(ctx, logger)
	logger.Info("Starting controller")
	defer logger.Info("Shutting down controller")

	go wait.UntilWithContext(ctx, c.startWorker, time.Second)
	<-ctx.Done()
	if c.previousCancel != nil {
		c.previousCancel()
	}
}

func (c *quotaController) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *quotaController) processNextWorkItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	if c.previousCancel != nil {
		c.previousCancel()
	}

	ctx, c.previousCancel = context.WithCancel(ctx)
	c.work(ctx)
	c.queue.Forget(key)
	return true
}

// NewQuotaConfigurationForControllers returns a quota configuration for controllers.
func NewQuotaConfigurationForControllers(f generic.ListFuncByNamespace) quota.Configuration {
	evaluators := NewEvaluators(f)
	return generic.NewConfiguration(evaluators, DefaultIgnoredResources())
}

// ignoredResources are ignored by quota by default
var ignoredResources = map[schema.GroupResource]struct{}{
	// virtual resources that aren't stored and shouldn't be quota-ed
	{Group: "", Resource: "bindings"}:                                      {},
	{Group: "", Resource: "componentstatuses"}:                             {},
	{Group: "authentication.k8s.io", Resource: "tokenreviews"}:             {},
	{Group: "authorization.k8s.io", Resource: "subjectaccessreviews"}:      {},
	{Group: "authorization.k8s.io", Resource: "selfsubjectaccessreviews"}:  {},
	{Group: "authorization.k8s.io", Resource: "localsubjectaccessreviews"}: {},
	{Group: "authorization.k8s.io", Resource: "selfsubjectrulesreviews"}:   {},

	// events haven't been quota-ed before
	{Group: "", Resource: "events"}: {},
}

// DefaultIgnoredResources returns the default set of resources that quota system
// should ignore. This is exposed so downstream integrators can have access to them.
func DefaultIgnoredResources() map[schema.GroupResource]struct{} {
	return ignoredResources
}
