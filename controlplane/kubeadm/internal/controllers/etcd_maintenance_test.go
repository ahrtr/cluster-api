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
	"slices"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	controlplanev1 "sigs.k8s.io/cluster-api/api/controlplane/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal/etcd"
)

// defragEntries is a test helper that builds a []EtcdMemberDefragStatus from a name→ago map.
func defragEntries(m map[string]time.Duration) []controlplanev1.EtcdMemberDefragStatus {
	if len(m) == 0 {
		return nil
	}
	out := make([]controlplanev1.EtcdMemberDefragStatus, 0, len(m))
	for name, ago := range m {
		out = append(out, controlplanev1.EtcdMemberDefragStatus{
			Name:           name,
			LastDefragTime: metav1.NewTime(time.Now().Add(-ago)),
		})
	}
	return out
}

func TestReconcileEtcdDefragmentation(t *testing.T) {
	ctx := context.Background()

	const (
		// defragRule triggers when the database is more than 80% of the quota.
		defragRule = "dbQuotaUsage > 0.8"

		dbQuota     int64 = 2_000_000_000 // 2 GiB
		dbSizeBig   int64 = 1_800_000_000 // quotaUsage = 0.90 → needs defrag
		dbSizeSmall int64 = 1_000_000_000 // quotaUsage = 0.50 → no defrag
	)

	// memberStatus builds a MemberStatus where leadership is expressed as ID == Leader.
	memberStatus := func(dbSize int64, isLeader bool) *etcd.MemberStatus {
		const leaderID uint64 = 99
		id := uint64(1)
		if isLeader {
			id = leaderID
		}
		return &etcd.MemberStatus{
			ID:          id,
			Leader:      leaderID,
			DbSize:      dbSize,
			DbSizeInUse: dbSize / 2,
			DbSizeQuota: dbQuota,
		}
	}

	tests := []struct {
		name string
		// setup returns the ControlPlane and the fakeWorkloadCluster so the test
		// can inspect DefraggedMembers after the reconcile call.
		setup func() (*internal.ControlPlane, *fakeWorkloadCluster)
		// wantDefragged is the ordered list of members expected to have been defragmented.
		wantDefragged []string
		wantRequeue   bool
		wantErr       bool
		// wantDefragTimesSet is true when EtcdMemberDefragTimes should be populated
		// for each member in wantDefragged.
		wantDefragTimesSet bool
	}{
		{
			name: "no-op when EtcdMaintenance is nil",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{}
				cp := &internal.ControlPlane{
					KCP:         &controlplanev1.KubeadmControlPlane{},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "no-op when DefragRule is empty",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: ""},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "no-op when etcd is external",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
							KubeadmConfigSpec: bootstrapv1.KubeadmConfigSpec{
								ClusterConfiguration: bootstrapv1.ClusterConfiguration{
									Etcd: bootstrapv1.Etcd{
										External: bootstrapv1.ExternalEtcd{
											Endpoints: []string{"https://etcd.example.com:2379"},
										},
									},
								},
							},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "no-op when EtcdMembers is empty",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster: &clusterv1.Cluster{},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "no-op when rule is not satisfied for any member",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeSmall, false),
						"node-b": memberStatus(dbSizeSmall, true),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}, {Name: "node-b"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "defrag single follower when rule is satisfied",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeBig, false),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
			wantDefragged:      []string{"node-a"},
			wantDefragTimesSet: true,
		},
		{
			name: "follower is defragged before leader when both need defrag",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeBig, true),  // leader
						"node-b": memberStatus(dbSizeBig, false), // follower
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}, {Name: "node-b"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
			wantDefragged:      []string{"node-b"},
			wantRequeue:        true,
			wantDefragTimesSet: true,
		},
		{
			name: "requeue when more than one member needs defrag",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeBig, false),
						"node-b": memberStatus(dbSizeBig, false),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}, {Name: "node-b"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
			wantDefragged:      []string{"node-a"}, // "node-a" < "node-b", both followers
			wantRequeue:        true,
			wantDefragTimesSet: true,
		},
		{
			name: "no requeue when exactly one member needs defrag",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeBig, false),
						"node-b": memberStatus(dbSizeSmall, false),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}, {Name: "node-b"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
			wantDefragged:      []string{"node-a"},
			wantDefragTimesSet: true,
		},
		{
			name: "member with empty name is skipped",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"": memberStatus(dbSizeBig, false),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: ""}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "learner member is skipped",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeBig, false),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a", IsLearner: true}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
		},
		{
			name: "EtcdMemberStatus error is logged and member is skipped",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatusErrors: map[string]error{
						"node-a": errors.New("connection refused"),
					},
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-b": memberStatus(dbSizeBig, false),
					},
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}, {Name: "node-b"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
			wantDefragged:      []string{"node-b"},
			wantDefragTimesSet: true,
		},
		{
			name: "DefragEtcdMember error is returned",
			setup: func() (*internal.ControlPlane, *fakeWorkloadCluster) {
				w := &fakeWorkloadCluster{
					EtcdMemberStatuses: map[string]*etcd.MemberStatus{
						"node-a": memberStatus(dbSizeBig, false),
					},
					DefragEtcdMemberErr: errors.New("defrag failed"),
				}
				cp := &internal.ControlPlane{
					KCP: &controlplanev1.KubeadmControlPlane{
						Spec: controlplanev1.KubeadmControlPlaneSpec{
							EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
						},
					},
					Cluster:     &clusterv1.Cluster{},
					EtcdMembers: []*etcd.Member{{Name: "node-a"}},
				}
				cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})
				return cp, w
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			cp, workload := tt.setup()
			r := &KubeadmControlPlaneReconciler{}
			result, err := r.reconcileEtcdDefragmentation(ctx, cp)

			if tt.wantErr {
				g.Expect(err).To(HaveOccurred())
				return
			}
			g.Expect(err).ToNot(HaveOccurred())

			if tt.wantRequeue {
				g.Expect(result.RequeueAfter).To(BeNumerically(">", time.Duration(0)))
			} else {
				g.Expect(result.RequeueAfter).To(BeZero())
			}

			g.Expect(workload.DefraggedMembers).To(Equal(tt.wantDefragged))

			if tt.wantDefragTimesSet {
				g.Expect(cp.KCP.Status.EtcdMemberDefragTimes).ToNot(BeEmpty(),
					"EtcdMemberDefragTimes should be populated when a defrag occurs")
				for _, name := range tt.wantDefragged {
					ts := memberDefragTime(cp.KCP.Status.EtcdMemberDefragTimes, name)
					g.Expect(ts).ToNot(BeNil(),
						"EtcdMemberDefragTimes should contain an entry for member %q", name)
					g.Expect(ts.IsZero()).To(BeFalse(),
						"defrag timestamp for member %q should not be zero", name)
				}
			} else {
				g.Expect(cp.KCP.Status.EtcdMemberDefragTimes).To(BeNil(),
					"EtcdMemberDefragTimes should remain nil when no defrag occurred")
			}
		})
	}
}

func TestReconcileEtcdDefragmentation_MinDefragInterval(t *testing.T) {
	ctx := context.Background()
	const defragRule = "dbSize >= 0.0" // always true; 0.0 keeps the literal a double to match dbSize's type

	baseStatus := func() *etcd.MemberStatus {
		return &etcd.MemberStatus{ID: 1, Leader: 99, DbSize: 1_000, DbSizeInUse: 800, DbSizeQuota: 2_000_000_000}
	}

	tests := []struct {
		name string
		// minDefragIntervalSeconds overrides the default when non-zero.
		minDefragIntervalSeconds int32
		// initialDefragTimes seeds EtcdMemberDefragTimes before the reconcile (name → how long ago).
		initialDefragAgo map[string]time.Duration
		// wantDefragged lists the members that should have been defragmented.
		wantDefragged []string
		// wantRequeueMin/Max bound the expected RequeueAfter (both zero means no requeue expected).
		wantRequeueMin time.Duration
		wantRequeueMax time.Duration
		// wantMemberDefragUpdated lists members whose entry should be freshly stamped.
		wantMemberDefragUpdated []string
	}{
		{
			name:                    "no EtcdMemberDefragTimes entry: defrag runs (first run)",
			wantDefragged:           []string{"node-a"},
			wantMemberDefragUpdated: []string{"node-a"},
		},
		{
			name:                    "last defrag for member exceeded default 1h interval: defrag runs",
			initialDefragAgo:        map[string]time.Duration{"node-a": 2 * time.Hour},
			wantDefragged:           []string{"node-a"},
			wantMemberDefragUpdated: []string{"node-a"},
		},
		{
			name:             "last defrag for member within default 1h interval: skipped, requeues for remainder",
			initialDefragAgo: map[string]time.Duration{"node-a": 30 * time.Minute},
			wantRequeueMin:   25 * time.Minute,
			wantRequeueMax:   30*time.Minute + 5*time.Second,
		},
		{
			name:                     "custom MinDefragIntervalSeconds not yet elapsed: skipped, requeues for remainder",
			minDefragIntervalSeconds: 600, // 10 minutes
			initialDefragAgo:         map[string]time.Duration{"node-a": 3 * time.Minute},
			wantRequeueMin:           6 * time.Minute,
			wantRequeueMax:           7*time.Minute + 5*time.Second,
		},
		{
			name:                     "custom MinDefragIntervalSeconds elapsed: defrag runs",
			minDefragIntervalSeconds: 600, // 10 minutes
			initialDefragAgo:         map[string]time.Duration{"node-a": 15 * time.Minute},
			wantDefragged:            []string{"node-a"},
			wantMemberDefragUpdated:  []string{"node-a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			initial := defragEntries(tt.initialDefragAgo)

			w := &fakeWorkloadCluster{
				EtcdMemberStatuses: map[string]*etcd.MemberStatus{"node-a": baseStatus()},
			}
			cp := &internal.ControlPlane{
				KCP: &controlplanev1.KubeadmControlPlane{
					Spec: controlplanev1.KubeadmControlPlaneSpec{
						EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{
							DefragRule:               defragRule,
							MinDefragIntervalSeconds: tt.minDefragIntervalSeconds,
						},
					},
					Status: controlplanev1.KubeadmControlPlaneStatus{
						EtcdMemberDefragTimes: initial,
					},
				},
				Cluster:     &clusterv1.Cluster{},
				EtcdMembers: []*etcd.Member{{Name: "node-a"}},
			}
			cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})

			r := &KubeadmControlPlaneReconciler{}
			result, err := r.reconcileEtcdDefragmentation(ctx, cp)

			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(w.DefraggedMembers).To(Equal(tt.wantDefragged))

			if tt.wantRequeueMin > 0 {
				g.Expect(result.RequeueAfter).To(BeNumerically(">", tt.wantRequeueMin))
				g.Expect(result.RequeueAfter).To(BeNumerically("<=", tt.wantRequeueMax))
			} else {
				g.Expect(result.RequeueAfter).To(BeZero())
			}

			for _, name := range tt.wantMemberDefragUpdated {
				ts := memberDefragTime(cp.KCP.Status.EtcdMemberDefragTimes, name)
				g.Expect(ts).ToNot(BeNil(), "EtcdMemberDefragTimes should have an entry for %q", name)
				g.Expect(ts.IsZero()).To(BeFalse(), "defrag timestamp for %q should not be zero", name)
				if prevAgo, had := tt.initialDefragAgo[name]; had {
					prevTime := time.Now().Add(-prevAgo)
					g.Expect(ts.Time).To(BeTemporally(">", prevTime),
						"defrag timestamp for %q should be updated past the initial value", name)
				}
			}

			// Members not in wantMemberDefragUpdated should be unchanged from initialDefragAgo.
			for name, ago := range tt.initialDefragAgo {
				if slices.Contains(tt.wantMemberDefragUpdated, name) {
					continue
				}
				ts := memberDefragTime(cp.KCP.Status.EtcdMemberDefragTimes, name)
				g.Expect(ts).ToNot(BeNil())
				// The stored time should still be approximately (now - ago), not refreshed.
				expectedTime := time.Now().Add(-ago)
				g.Expect(ts.Time).To(BeTemporally("~", expectedTime, 5*time.Second),
					"EtcdMemberDefragTimes[%q] should not have been refreshed", name)
			}
		})
	}
}

// TestReconcileEtcdDefragmentation_PerMemberIndependence verifies that per-member interval
// enforcement is truly independent: a member whose interval has not elapsed is skipped while
// a member whose interval has elapsed IS defragmented in the same reconcile pass.
func TestReconcileEtcdDefragmentation_PerMemberIndependence(t *testing.T) {
	ctx := context.Background()
	g := NewWithT(t)

	const defragRule = "dbSize >= 0.0" // always true

	baseStatus := func() *etcd.MemberStatus {
		return &etcd.MemberStatus{ID: 1, Leader: 99, DbSize: 1_000, DbSizeInUse: 800, DbSizeQuota: 2_000_000_000}
	}

	w := &fakeWorkloadCluster{
		EtcdMemberStatuses: map[string]*etcd.MemberStatus{
			"node-a": baseStatus(),
			"node-b": baseStatus(),
		},
	}

	// node-a was defragged 2 hours ago (interval elapsed); node-b was defragged 5 minutes ago (still throttled).
	cp := &internal.ControlPlane{
		KCP: &controlplanev1.KubeadmControlPlane{
			Spec: controlplanev1.KubeadmControlPlaneSpec{
				EtcdMaintenance: &controlplanev1.EtcdMaintenanceSpec{DefragRule: defragRule},
			},
			Status: controlplanev1.KubeadmControlPlaneStatus{
				EtcdMemberDefragTimes: []controlplanev1.EtcdMemberDefragStatus{
					{Name: "node-a", LastDefragTime: metav1.NewTime(time.Now().Add(-2 * time.Hour))},
					{Name: "node-b", LastDefragTime: metav1.NewTime(time.Now().Add(-5 * time.Minute))},
				},
			},
		},
		Cluster:     &clusterv1.Cluster{},
		EtcdMembers: []*etcd.Member{{Name: "node-a"}, {Name: "node-b"}},
	}
	cp.InjectTestManagementCluster(&fakeManagementCluster{Workload: w, Reader: fake.NewFakeClient()})

	r := &KubeadmControlPlaneReconciler{}
	result, err := r.reconcileEtcdDefragmentation(ctx, cp)

	g.Expect(err).ToNot(HaveOccurred())
	// Only node-a should be defragged; node-b is still within its interval.
	g.Expect(w.DefraggedMembers).To(Equal([]string{"node-a"}))
	// The controller should requeue when node-b becomes eligible (~55 minutes from now).
	g.Expect(result.RequeueAfter).To(BeNumerically(">", 50*time.Minute))
	g.Expect(result.RequeueAfter).To(BeNumerically("<=", 55*time.Minute+5*time.Second))
	// node-a's timestamp should be updated; node-b's should be unchanged (~5 minutes ago).
	tsA := memberDefragTime(cp.KCP.Status.EtcdMemberDefragTimes, "node-a")
	g.Expect(tsA.Time).To(BeTemporally(">", time.Now().Add(-10*time.Second)))
	tsB := memberDefragTime(cp.KCP.Status.EtcdMemberDefragTimes, "node-b")
	g.Expect(tsB.Time).To(BeTemporally("~", time.Now().Add(-5*time.Minute), 5*time.Second))
}
