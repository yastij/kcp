package kubequota

import (
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"strings"
	"time"

	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/utils/clock"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	api "k8s.io/kubernetes/pkg/apis/apps"
	k8s_apps_v1 "k8s.io/kubernetes/pkg/apis/apps/v1"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
)

// legacyObjectCountAliases are what we used to do simple object counting quota with mapped to alias
var legacyObjectCountAliases = map[schema.GroupVersionResource]corev1.ResourceName{
	corev1.SchemeGroupVersion.WithResource("configmaps"):             corev1.ResourceConfigMaps,
	corev1.SchemeGroupVersion.WithResource("resourcequotas"):         corev1.ResourceQuotas,
	corev1.SchemeGroupVersion.WithResource("replicationcontrollers"): corev1.ResourceReplicationControllers,
	corev1.SchemeGroupVersion.WithResource("secrets"):                corev1.ResourceSecrets,
}

// NewEvaluators returns the list of static evaluators that manage more than counts
func NewEvaluators(f generic.ListFuncByNamespace) []quota.Evaluator {
	// these evaluators have special logic
	result := []quota.Evaluator{
		NewDeploymentEvaluator(f, clock.RealClock{}),
	}
	// these evaluators require an alias for backwards compatibility
	for gvr, alias := range legacyObjectCountAliases {
		result = append(result,
			generic.NewObjectCountEvaluator(gvr.GroupResource(), f, alias))
	}
	return result
}

// the name used for object count quota
var deploymentObjectCountName = generic.ObjectCountQuotaResourceNameFor(appsv1.SchemeGroupVersion.WithResource("deployments").GroupResource())

// podResources are the set of resources managed by quota associated with pods.
var DeploymentResources = []corev1.ResourceName{
	deploymentObjectCountName,
	corev1.ResourceCPU,
	corev1.ResourceMemory,
	corev1.ResourceEphemeralStorage,
	corev1.ResourceRequestsCPU,
	corev1.ResourceRequestsMemory,
	corev1.ResourceRequestsEphemeralStorage,
	corev1.ResourceLimitsCPU,
	corev1.ResourceLimitsMemory,
	corev1.ResourceLimitsEphemeralStorage,
	corev1.ResourceName("deployments"),
}

// podResourcePrefixes are the set of prefixes for resources (Hugepages, and other
// potential extended resources with specific prefix) managed by quota associated with pods.
var deploymentResourcePrefixes = []string{
	corev1.ResourceHugePagesPrefix,
	corev1.ResourceRequestsHugePagesPrefix,
}

// requestedResourcePrefixes are the set of prefixes for resources
// that might be declared in pod's Resources.Requests/Limits
var requestedResourcePrefixes = []string{
	corev1.ResourceHugePagesPrefix,
}

// maskResourceWithPrefix mask resource with certain prefix
// e.g. hugepages-XXX -> requests.hugepages-XXX
func maskResourceWithPrefix(resource corev1.ResourceName, prefix string) corev1.ResourceName {
	return corev1.ResourceName(fmt.Sprintf("%s%s", prefix, string(resource)))
}

// isExtendedResourceNameForQuota returns true if the extended resource name
// has the quota related resource prefix.
func isExtendedResourceNameForQuota(name corev1.ResourceName) bool {
	// As overcommit is not supported by extended resources for now,
	// only quota objects in format of "requests.resourceName" is allowed.
	return !helper.IsNativeResource(name) && strings.HasPrefix(string(name), corev1.DefaultResourceRequestsPrefix)
}

// NOTE: it was a mistake, but if a quota tracks cpu or memory related resources,
// the incoming pod is required to have those values set.  we should not repeat
// this mistake for other future resources (gpus, ephemeral-storage,etc).
// do not add more resources to this list!
var validationSet = sets.NewString(
	string(corev1.ResourceCPU),
	string(corev1.ResourceMemory),
	string(corev1.ResourceRequestsCPU),
	string(corev1.ResourceRequestsMemory),
	string(corev1.ResourceLimitsCPU),
	string(corev1.ResourceLimitsMemory),
)

// NewPodEvaluator returns an evaluator that can evaluate pods
func NewDeploymentEvaluator(f generic.ListFuncByNamespace, clock clock.Clock) quota.Evaluator {
	//listFuncByNamespace := generic.ListResourceUsingListerFunc(f, appsv1.SchemeGroupVersion.WithResource("deployments"))

	deploymentEvaluator := &deploymentEvaluator{listFuncByNamespace: f, clock: clock}
	return deploymentEvaluator
}

// deploymentEvaluator knows how to measure usage of pods.
type deploymentEvaluator struct {
	// knows how to list pods
	listFuncByNamespace generic.ListFuncByNamespace
	// used to track time
	clock clock.Clock
}

// Constraints verifies that all required resources are present on the pod
// In addition, it validates that the resources are valid (i.e. requests < limits)
func (p *deploymentEvaluator) Constraints(required []corev1.ResourceName, item runtime.Object) error {
	deploy, err := toExternalDeployOrError(item)
	if err != nil {
		return err
	}

	// BACKWARD COMPATIBILITY REQUIREMENT: if we quota cpu or memory, then each container
	// must make an explicit request for the resource.  this was a mistake.  it coupled
	// validation with resource counting, but we did this before QoS was even defined.
	// let's not make that mistake again with other resources now that QoS is defined.
	requiredSet := quota.ToSet(required).Intersection(validationSet)
	missingSetResourceToContainerNames := make(map[string]sets.String)
	for i := range deploy.Spec.Template.Spec.Containers {
		enforceDeploymentContainerConstraints(&deploy.Spec.Template.Spec.Containers[i], requiredSet, missingSetResourceToContainerNames)
	}
	for i := range deploy.Spec.Template.Spec.InitContainers {
		enforceDeploymentContainerConstraints(&deploy.Spec.Template.Spec.InitContainers[i], requiredSet, missingSetResourceToContainerNames)
	}
	if len(missingSetResourceToContainerNames) == 0 {
		return nil
	}
	var resources = sets.NewString()
	for resource := range missingSetResourceToContainerNames {
		resources.Insert(resource)
	}
	var errorMessages = make([]string, 0, len(missingSetResourceToContainerNames))
	for _, resource := range resources.List() {
		errorMessages = append(errorMessages, fmt.Sprintf("%s for: %s", resource, strings.Join(missingSetResourceToContainerNames[resource].List(), ",")))
	}
	return fmt.Errorf("must specify %s", strings.Join(errorMessages, "; "))
}

// GroupResource that this evaluator tracks
func (p *deploymentEvaluator) GroupResource() schema.GroupResource {
	return appsv1.SchemeGroupVersion.WithResource("deployments").GroupResource()
}

// Handles returns true if the evaluator should handle the specified attributes.
func (p *deploymentEvaluator) Handles(a admission.Attributes) bool {
	op := a.GetOperation()
	if op == admission.Create || op == admission.Update {
		return true
	}
	return false
}

// Matches returns true if the evaluator matches the specified quota with the provided input item
func (p *deploymentEvaluator) Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error) {
	val, err := generic.Matches(resourceQuota, item, p.MatchingResources, deployMatchesScopeFunc)
	return val, err
}

// MatchingResources takes the input specified list of resources and returns the set of resources it matches.
func (p *deploymentEvaluator) MatchingResources(input []corev1.ResourceName) []corev1.ResourceName {
	result := quota.Intersection(input, DeploymentResources)
	for _, resource := range input {
		// for resources with certain prefix, e.g. hugepages
		if quota.ContainsPrefix(deploymentResourcePrefixes, resource) {
			result = append(result, resource)
		}
		// for extended resources
		if isExtendedResourceNameForQuota(resource) {
			result = append(result, resource)
		}
	}

	return result
}

// MatchingScopes takes the input specified list of scopes and pod object. Returns the set of scope selectors pod matches.
func (p *deploymentEvaluator) MatchingScopes(item runtime.Object, scopeSelectors []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	matchedScopes := []corev1.ScopedResourceSelectorRequirement{}
	for _, selector := range scopeSelectors {
		match, err := deployMatchesScopeFunc(selector, item)
		if err != nil {
			return []corev1.ScopedResourceSelectorRequirement{}, fmt.Errorf("error on matching scope %v: %v", selector, err)
		}
		if match {
			matchedScopes = append(matchedScopes, selector)
		}
	}
	return matchedScopes, nil
}

// UncoveredQuotaScopes takes the input matched scopes which are limited by configuration and the matched quota scopes.
// It returns the scopes which are in limited scopes but don't have a corresponding covering quota scope
func (p *deploymentEvaluator) UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	uncoveredScopes := []corev1.ScopedResourceSelectorRequirement{}
	for _, selector := range limitedScopes {
		isCovered := false
		for _, matchedScopeSelector := range matchedQuotaScopes {
			if matchedScopeSelector.ScopeName == selector.ScopeName {
				isCovered = true
				break
			}
		}

		if !isCovered {
			uncoveredScopes = append(uncoveredScopes, selector)
		}
	}
	return uncoveredScopes, nil
}

// Usage knows how to measure usage associated with pods
func (p *deploymentEvaluator) Usage(item runtime.Object) (corev1.ResourceList, error) {
	// delegate to normal usage
	return PodUsageFunc(item, p.clock)
}

// UsageStats calculates aggregate usage for the object.
func (p *deploymentEvaluator) UsageStats(options quota.UsageStatsOptions) (quota.UsageStats, error) {
	//items, _ := p.listFuncByNamespace(options.Namespace)
	//panic(items)
	quant, err := generic.CalculateUsageStats(options, p.listFuncByNamespace, deployMatchesScopeFunc, p.Usage)
	//panic(quant)
	return quant, err
}

// verifies we implement the required interface.
var _ quota.Evaluator = &deploymentEvaluator{}

// enforcePodContainerConstraints checks for required resources that are not set on this container and
// adds them to missingSet.
func enforceDeploymentContainerConstraints(container *corev1.Container, requiredSet sets.String, missingSetResourceToContainerNames map[string]sets.String) {
	requests := container.Resources.Requests
	limits := container.Resources.Limits
	containerUsage := podComputeUsageHelper(requests, limits)
	containerSet := quota.ToSet(quota.ResourceNames(containerUsage))
	if !containerSet.Equal(requiredSet) {
		if difference := requiredSet.Difference(containerSet); difference.Len() != 0 {
			for _, diff := range difference.List() {
				if _, ok := missingSetResourceToContainerNames[diff]; !ok {
					missingSetResourceToContainerNames[diff] = sets.NewString(container.Name)
				} else {
					missingSetResourceToContainerNames[diff].Insert(container.Name)
				}
			}
		}
	}
}

// podComputeUsageHelper can summarize the pod compute quota usage based on requests and limits
func podComputeUsageHelper(requests corev1.ResourceList, limits corev1.ResourceList) corev1.ResourceList {
	result := corev1.ResourceList{}
	result["deployments"] = resource.MustParse("1")
	if request, found := requests[corev1.ResourceCPU]; found {
		result[corev1.ResourceCPU] = request
		result[corev1.ResourceRequestsCPU] = request
	}
	if limit, found := limits[corev1.ResourceCPU]; found {
		result[corev1.ResourceLimitsCPU] = limit
	}
	if request, found := requests[corev1.ResourceMemory]; found {
		result[corev1.ResourceMemory] = request
		result[corev1.ResourceRequestsMemory] = request
	}
	if limit, found := limits[corev1.ResourceMemory]; found {
		result[corev1.ResourceLimitsMemory] = limit
	}
	if request, found := requests[corev1.ResourceEphemeralStorage]; found {
		result[corev1.ResourceEphemeralStorage] = request
		result[corev1.ResourceRequestsEphemeralStorage] = request
	}
	if limit, found := limits[corev1.ResourceEphemeralStorage]; found {
		result[corev1.ResourceLimitsEphemeralStorage] = limit
	}
	for resource, request := range requests {
		// for resources with certain prefix, e.g. hugepages
		if quota.ContainsPrefix(requestedResourcePrefixes, resource) {
			result[resource] = request
			result[maskResourceWithPrefix(resource, corev1.DefaultResourceRequestsPrefix)] = request
		}
		// for extended resources
		if helper.IsExtendedResourceName(resource) {
			// only quota objects in format of "requests.resourceName" is allowed for extended resource.
			result[maskResourceWithPrefix(resource, corev1.DefaultResourceRequestsPrefix)] = request
		}
	}

	return result
}

func toExternalDeployOrError(obj runtime.Object) (*appsv1.Deployment, error) {
	deploy := &appsv1.Deployment{}
	data := &unstructured.Unstructured{}
	switch t := obj.(type) {
	case *appsv1.Deployment:
		deploy = t
	case *api.Deployment:
		if err := k8s_apps_v1.Convert_apps_Deployment_To_v1_Deployment(t, deploy, nil); err != nil {
			return nil, err
		}
	default:
		data = obj.(*unstructured.Unstructured)
		bytes, err := data.MarshalJSON()
		if err != nil {
			return deploy, err
		}
		json.Unmarshal(bytes, deploy)
	}
	return deploy, nil
}

// podMatchesScopeFunc is a function that knows how to evaluate if a pod matches a scope
func deployMatchesScopeFunc(selector corev1.ScopedResourceSelectorRequirement, object runtime.Object) (bool, error) {
	deploy, err := toExternalDeployOrError(object)
	if err != nil {
		return false, err
	}

	switch selector.ScopeName {
	case corev1.ResourceQuotaScopeBestEffort:
		return isBestEffort(deploy), nil
	case corev1.ResourceQuotaScopeNotBestEffort:
		return !isBestEffort(deploy), nil
	case corev1.ResourceQuotaScopePriorityClass:
		return podMatchesSelector(deploy, selector)
	case corev1.ResourceQuotaScopeCrossNamespacePodAffinity:
		return usesCrossNamespacePodAffinity(deploy), nil
	}
	return false, nil
}

// PodUsageFunc returns the quota usage for a pod.
// A pod is charged for quota if the following are not true.
//   - pod has a terminal phase (failed or succeeded)
//   - pod has been marked for deletion and grace period has expired
func PodUsageFunc(obj runtime.Object, clock clock.Clock) (corev1.ResourceList, error) {
	deploy, err := toExternalDeployOrError(obj)
	if err != nil {
		return corev1.ResourceList{}, err
	}

	// always quota the object count (even if the pod is end of life)
	// object count quotas track all objects that are in storage.
	// where "pods" tracks all pods that have not reached a terminal state,
	// count/pods tracks all pods independent of state.
	result := corev1.ResourceList{
		deploymentObjectCountName: *(resource.NewQuantity(1, resource.DecimalSI)),
	}

	// by convention, we do not quota compute resources that have reached end-of life
	// note: the "pods" resource is considered a compute resource since it is tied to life-cycle.
	if !QuotaV1AppsDeployment(deploy, clock) {
		return result, nil
	}

	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}
	requestsFinal := corev1.ResourceList{}
	limitsFinal := corev1.ResourceList{}

	// TODO: ideally, we have pod level requests and limits in the future.
	for i := range deploy.Spec.Template.Spec.Containers {
		requests = quota.Add(requests, deploy.Spec.Template.Spec.Containers[i].Resources.Requests)
		limits = quota.Add(limits, deploy.Spec.Template.Spec.Containers[i].Resources.Limits)
	}
	// InitContainers are run sequentially before other containers start, so the highest
	// init container resource is compared against the sum of app containers to determine
	// the effective usage for both requests and limits.
	for i := range deploy.Spec.Template.Spec.InitContainers {
		requests = quota.Max(requests, deploy.Spec.Template.Spec.InitContainers[i].Resources.Requests)
		limits = quota.Max(limits, deploy.Spec.Template.Spec.InitContainers[i].Resources.Limits)
	}

	requests = quota.Add(requests, deploy.Spec.Template.Spec.Overhead)
	limits = quota.Add(limits, deploy.Spec.Template.Spec.Overhead)
	for i := 1; i <= int(*deploy.Spec.Replicas); i++ {
		requestsFinal = quota.Add(requestsFinal, requests)
		limitsFinal = quota.Add(limitsFinal, limits)
	}
	result = quota.Add(result, podComputeUsageHelper(requestsFinal, limitsFinal))
	return result, nil
}

func isBestEffort(deploy *appsv1.Deployment) bool {
	pod := &corev1.Pod{
		Spec: deploy.Spec.Template.Spec,
	}
	return qos.GetPodQOS(pod) == corev1.PodQOSBestEffort
}

func isTerminating(deploy *appsv1.Deployment) bool {
	if deploy.Spec.Template.Spec.ActiveDeadlineSeconds != nil && *deploy.Spec.Template.Spec.ActiveDeadlineSeconds >= int64(0) {
		return true
	}
	return false
}

func podMatchesSelector(deployment *appsv1.Deployment, selector corev1.ScopedResourceSelectorRequirement) (bool, error) {
	labelSelector, err := helper.ScopedResourceSelectorRequirementsAsSelector(selector)
	if err != nil {
		return false, fmt.Errorf("failed to parse and convert selector: %v", err)
	}
	var m map[string]string
	if len(deployment.Spec.Template.Spec.PriorityClassName) != 0 {
		m = map[string]string{string(corev1.ResourceQuotaScopePriorityClass): deployment.Spec.Template.Spec.PriorityClassName}
	}
	if labelSelector.Matches(labels.Set(m)) {
		return true, nil
	}
	return false, nil
}

func crossNamespacePodAffinityTerm(term *corev1.PodAffinityTerm) bool {
	return len(term.Namespaces) != 0 || term.NamespaceSelector != nil
}

func crossNamespacePodAffinityTerms(terms []corev1.PodAffinityTerm) bool {
	for _, t := range terms {
		if crossNamespacePodAffinityTerm(&t) {
			return true
		}
	}
	return false
}

func crossNamespaceWeightedPodAffinityTerms(terms []corev1.WeightedPodAffinityTerm) bool {
	for _, t := range terms {
		if crossNamespacePodAffinityTerm(&t.PodAffinityTerm) {
			return true
		}
	}
	return false
}

func usesCrossNamespacePodAffinity(deployment *appsv1.Deployment) bool {
	if deployment == nil || deployment.Spec.Template.Spec.Affinity == nil {
		return false
	}

	affinity := deployment.Spec.Template.Spec.Affinity.PodAffinity
	if affinity != nil {
		if crossNamespacePodAffinityTerms(affinity.RequiredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
		if crossNamespaceWeightedPodAffinityTerms(affinity.PreferredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
	}

	antiAffinity := deployment.Spec.Template.Spec.Affinity.PodAntiAffinity
	if antiAffinity != nil {
		if crossNamespacePodAffinityTerms(antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
		if crossNamespaceWeightedPodAffinityTerms(antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
	}

	return false
}

// QuotaV1Pod returns true if the pod is eligible to track against a quota
// if it's not in a terminal state according to its phase.
func QuotaV1AppsDeployment(deployment *appsv1.Deployment, clock clock.Clock) bool {
	// if pods are stuck terminating (for example, a node is lost), we do not want
	// to charge the user for that pod in quota because it could prevent them from
	// scaling up new pods to service their application.
	if deployment.DeletionTimestamp != nil && deployment.DeletionGracePeriodSeconds != nil {
		now := clock.Now()
		deletionTime := deployment.DeletionTimestamp.Time
		gracePeriod := time.Duration(*deployment.DeletionGracePeriodSeconds) * time.Second
		if now.After(deletionTime.Add(gracePeriod)) {
			return false
		}
	}
	return true
}
