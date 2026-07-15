// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package numa

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
)

// TestAdmitMatchesEvaluate checks that admit (solve without an allocation sink) and evaluate (solve
// with one) return the same admit decision across randomized topologies and pods — i.e. recording
// the placement never changes feasibility. Both go through the same solve; the worked examples pin
// the decision's correctness.
func TestAdmitMatchesEvaluate(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	resources := []string{gpu, "cpu", "memory"}
	policies := []node_info.TopologyManagerPolicy{
		node_info.TopologyPolicySingleNUMANode,
		node_info.TopologyPolicyRestricted,
	}
	scopes := []node_info.TopologyManagerScope{
		node_info.TopologyScopePod,
		node_info.TopologyScopeContainer,
	}

	for i := 0; i < 5000; i++ {
		policy := policies[rng.Intn(len(policies))]
		scope := scopes[rng.Intn(len(scopes))]
		nZones := 2 + rng.Intn(3) // 2..4 zones

		specs := make([]node_info.NumaZoneSpec, nZones)
		for z := 0; z < nZones; z++ {
			alloc := v1.ResourceList{}
			avail := v1.ResourceList{}
			for _, r := range resources {
				if rng.Intn(4) == 0 {
					continue // this resource not reported on this zone
				}
				capQty := rng.Intn(5) // 0..4
				freeQty := rng.Intn(capQty + 1)
				alloc[v1.ResourceName(r)] = quantity(r, capQty)
				avail[v1.ResourceName(r)] = quantity(r, freeQty)
			}
			specs[z] = node_info.NumaZoneSpec{ID: fmt.Sprintf("node-%d", z), Allocatable: alloc, Available: avail}
		}
		topo := numaTopology(policy, scope, specs...)

		task := randomPod(rng, i, resources)
		node := &node_info.NodeInfo{Name: "n", NumaTopology: topo}
		pp := &numaPlugin{ignoreList: sets.New[v1.ResourceName]()}

		_, wantAdmit := pp.evaluate(task, node)
		gotAdmit := pp.admit(task, node)

		assert.Equalf(t, wantAdmit, gotAdmit,
			"case %d: policy=%v scope=%v zones=%d pod=%s", i, policy, scope, nZones, describePod(task))
	}
}

// randomPod builds a Guaranteed pod with 1..3 containers of random per-resource requests.
func randomPod(rng *rand.Rand, seq int, resources []string) *pod_info.PodInfo {
	nContainers := 1 + rng.Intn(3)
	containers := make([]v1.Container, nContainers)
	for c := 0; c < nContainers; c++ {
		reqs := v1.ResourceList{}
		for _, r := range resources {
			if rng.Intn(2) == 0 {
				continue
			}
			reqs[v1.ResourceName(r)] = quantity(r, 1+rng.Intn(4)) // 1..4
		}
		containers[c] = v1.Container{Resources: v1.ResourceRequirements{Requests: reqs}}
	}
	return &pod_info.PodInfo{
		UID:  common_info.PodID(fmt.Sprintf("p-%d", seq)),
		Name: fmt.Sprintf("p-%d", seq),
		Pod: &v1.Pod{
			Status: v1.PodStatus{QOSClass: v1.PodQOSGuaranteed},
			Spec:   v1.PodSpec{Containers: containers},
		},
	}
}

// quantity builds a resource.Quantity, sizing memory in Gi so it exercises the byte-scale path.
func quantity(resourceName string, n int) resource.Quantity {
	if resourceName == "memory" {
		return resource.MustParse(fmt.Sprintf("%dGi", n))
	}
	return resource.MustParse(fmt.Sprintf("%d", n))
}

func describePod(task *pod_info.PodInfo) string {
	out := ""
	for i := range task.Pod.Spec.Containers {
		out += fmt.Sprintf("[c%d %v]", i, task.Pod.Spec.Containers[i].Resources.Requests)
	}
	return out
}
