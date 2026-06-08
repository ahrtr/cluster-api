/*
Copyright 2026 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	controlplanev1 "sigs.k8s.io/cluster-api/api/controlplane/kubeadm/v1beta2"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal/etcd"
)

const defaultMinDefragIntervalSeconds int32 = 3600 // 1 hour

// reconcileEtcdDefragmentation evaluates the configured defrag rule for each managed etcd
// member and, when the rule is satisfied and the per-member minimum interval has elapsed,
// defragments one member per reconcile cycle.
//
// Members are processed in a safe order: followers first, the leader last. Defragmenting
// only one member per reconcile and then requeuing ensures that the cluster is never
// exposed to more than one unavailable member at a time.
//
// The minimum interval (minDefragIntervalSeconds) is enforced independently per member:
// all members in a cluster can be defragmented in rapid succession and each member will
// not be defragmented again until its own timer has expired.
func (r *KubeadmControlPlaneReconciler) reconcileEtcdDefragmentation(ctx context.Context, controlPlane *internal.ControlPlane) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// No-op when defrag is not configured or etcd is not managed by KCP.
	if controlPlane.KCP.Spec.EtcdMaintenance == nil ||
		controlPlane.KCP.Spec.EtcdMaintenance.DefragRule == "" ||
		!controlPlane.IsEtcdManaged() {
		return ctrl.Result{}, nil
	}

	// No-op when the etcd member list has not been populated yet.
	if len(controlPlane.EtcdMembers) == 0 {
		return ctrl.Result{}, nil
	}

	// A zero value means "use the default".
	minIntervalSeconds := controlPlane.KCP.Spec.EtcdMaintenance.MinDefragIntervalSeconds
	if minIntervalSeconds == 0 {
		minIntervalSeconds = defaultMinDefragIntervalSeconds
	}
	minInterval := time.Duration(minIntervalSeconds) * time.Second

	workloadCluster, err := controlPlane.GetWorkloadCluster(ctx)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to get workload cluster client for etcd defragmentation")
	}

	rule := controlPlane.KCP.Spec.EtcdMaintenance.DefragRule
	etcdLocal := &controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local

	// Collect members whose defrag rule evaluates to true AND whose per-member interval
	// has elapsed, together with their status so we can sort them without a second round
	// of Status() calls.
	type candidate struct {
		member *etcd.Member
		status *etcd.MemberStatus
	}
	var candidates []candidate
	// earliestRequeue tracks the soonest a currently-throttled member will become eligible,
	// so the controller can requeue exactly when the next member is ready.
	var earliestRequeue time.Duration

	for _, member := range controlPlane.EtcdMembers {
		// Skip members that are not yet named or are learners; they cannot be defragmented safely.
		if member.Name == "" || member.IsLearner {
			continue
		}

		// Enforce the per-member minimum interval.
		if last := memberDefragTime(controlPlane.KCP.Status.EtcdMemberDefragTimes, member.Name); last != nil {
			elapsed := time.Since(last.Time)
			if elapsed < minInterval {
				remaining := minInterval - elapsed
				log.V(4).Info("Skipping etcd defragmentation for member: minimum interval not elapsed",
					"member", member.Name,
					"elapsed", elapsed.Round(time.Second),
					"minInterval", minInterval,
					"requeueAfter", remaining.Round(time.Second))
				if earliestRequeue == 0 || remaining < earliestRequeue {
					earliestRequeue = remaining
				}
				continue
			}
		}

		status, err := workloadCluster.EtcdMemberStatus(ctx, member.Name)
		if err != nil {
			log.Error(err, "Failed to get etcd member status, skipping defragmentation check for member", "member", member.Name)
			continue
		}

		quota := resolveEtcdQuota(status.DbSizeQuota, etcdLocal)
		needsDefrag, err := etcd.EvaluateDefragRule(
			rule,
			float64(status.DbSize),
			float64(status.DbSizeInUse),
			float64(quota),
		)
		if err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to evaluate defrag rule for etcd member %s", member.Name)
		}

		if needsDefrag {
			candidates = append(candidates, candidate{member: member, status: status})
		}
	}

	if len(candidates) == 0 {
		// No member is ready to defrag right now. Requeue when the earliest throttled
		// member becomes eligible (if any).
		if earliestRequeue > 0 {
			return ctrl.Result{RequeueAfter: earliestRequeue}, nil
		}
		return ctrl.Result{}, nil
	}

	// Sort: followers before the leader to minimise disruption. Ties are broken by member
	// name for determinism.
	sort.Slice(candidates, func(i, j int) bool {
		iIsLeader := candidates[i].status.ID == candidates[i].status.Leader
		jIsLeader := candidates[j].status.ID == candidates[j].status.Leader
		if iIsLeader != jIsLeader {
			// non-leader (false) sorts before leader (true)
			return !iIsLeader
		}
		return candidates[i].member.Name < candidates[j].member.Name
	})

	// Defragment one member per reconcile to avoid simultaneous unavailability.
	target := candidates[0]
	log.Info("Defragmenting etcd member", "member", target.member.Name,
		"dbSize", target.status.DbSize, "dbSizeInUse", target.status.DbSizeInUse)

	if err := workloadCluster.DefragEtcdMember(ctx, target.member.Name); err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to defragment etcd member %s", target.member.Name)
	}

	log.Info("Successfully defragmented etcd member", "member", target.member.Name)

	// Stamp the per-member defrag time after a successful defragmentation.
	now := metav1.Now()
	setMemberDefragTime(&controlPlane.KCP.Status.EtcdMemberDefragTimes, target.member.Name, now)

	// If more members still need defragmentation, requeue after a short settling delay
	// so the next reconcile can re-evaluate each member's updated status.
	if len(candidates) > 1 {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Even though no further candidates remain right now, a throttled member may become
	// eligible later. Schedule a requeue so it is not missed.
	if earliestRequeue > 0 {
		return ctrl.Result{RequeueAfter: earliestRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// memberDefragTime returns the LastDefragTime for the named member from the status list,
// or nil if no entry exists for that member yet.
func memberDefragTime(times []controlplanev1.EtcdMemberDefragStatus, name string) *metav1.Time {
	for i := range times {
		if times[i].Name == name {
			return &times[i].LastDefragTime
		}
	}
	return nil
}

// setMemberDefragTime upserts the defrag timestamp for the named member in the status list.
func setMemberDefragTime(times *[]controlplanev1.EtcdMemberDefragStatus, name string, t metav1.Time) {
	for i := range *times {
		if (*times)[i].Name == name {
			(*times)[i].LastDefragTime = t
			return
		}
	}
	*times = append(*times, controlplanev1.EtcdMemberDefragStatus{Name: name, LastDefragTime: t})
}

// defaultEtcdQuotaBytes is the etcd built-in default storage quota (2 GiB),
// used when the quota cannot be determined from the Status response or extraArgs.
const defaultEtcdQuotaBytes int64 = 2 * 1024 * 1024 * 1024

// resolveEtcdQuota returns the etcd storage quota in bytes for a given member,
// consulting the following sources in order:
//
//  1. DbSizeQuota from the etcd Status response (available since etcd v3.6).
//  2. quota-backend-bytes from spec.kubeadmConfigSpec.clusterConfiguration.etcd.local.extraArgs.
//  3. The etcd built-in default of 2 GiB.
//
// Note: step 2 is a compatibility shim for etcd v3.5, which does not expose quota in its
// Status response. It will be removed once etcd v3.5 is out of support.
func resolveEtcdQuota(dbSizeQuotaFromStatus int64, etcdLocal *bootstrapv1.LocalEtcd) int64 {
	// Step 1: DbSizeQuota is populated by etcd v3.6+ in the Status response.
	// On etcd v3.5 the field is absent and proto3 zero-decodes it as 0.
	if dbSizeQuotaFromStatus > 0 {
		return dbSizeQuotaFromStatus
	}

	// Step 2: fall back to parsing quota-backend-bytes from extraArgs.
	// TODO: Remove this block once etcd v3.5 is out of support.
	if etcdLocal != nil {
		for _, arg := range etcdLocal.ExtraArgs {
			if arg.Name == "quota-backend-bytes" && arg.Value != nil {
				quota, err := strconv.ParseInt(*arg.Value, 10, 64)
				if err == nil && quota > 0 {
					return quota
				}
			}
		}
	}

	// Step 3: fall back to etcd's built-in default.
	return defaultEtcdQuotaBytes
}
