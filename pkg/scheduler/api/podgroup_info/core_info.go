// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package podgroup_info

import (
	"sort"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
)

// GetCoreTasks returns the set of allocated tasks that make up the job's minimal satisfying shape
// (its "core"): at each SubGroupSet the GetMinMembersToSatisfy() highest-priority satisfied members
// (sorted by subGroupOrderFn), recursively; at each leaf PodSet the minAvailable highest-priority
// allocated pods (sorted by taskOrderFn). The remaining allocated tasks are elastic surplus.
//
// This mirrors the eviction protection logic (eviction_info.go) so quota accounting and eviction agree
// by construction on what "core" means. Flat jobs (no minSubGroup) reduce to the per-leaf-minMember
// result and are backward compatible.
func GetCoreTasks(
	job *PodGroupInfo, subGroupOrderFn, taskOrderFn common_info.LessFn,
) map[common_info.PodID]*pod_info.PodInfo {
	core := map[common_info.PodID]*pod_info.PodInfo{}

	root := job.RootSubGroupSet
	if root == nil {
		root = subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName, nil)
		for _, ps := range job.PodSets {
			root.AddPodSet(ps)
		}
	}

	collectCoreFromSubGroupSet(root, subGroupOrderFn, taskOrderFn, core)
	return core
}

// collectCoreFromSubGroupSet adds the core tasks of a SubGroupSet to the accumulator.
func collectCoreFromSubGroupSet(
	sgs *subgroup_info.SubGroupSet, subGroupOrderFn, taskOrderFn common_info.LessFn,
	core map[common_info.PodID]*pod_info.PodInfo,
) {
	k := sgs.GetMinMembersToSatisfy()
	members := sgs.GetMembers()
	sort.Slice(members, func(i, j int) bool {
		return subGroupOrderFn(members[i], members[j])
	})

	// Take the k highest-priority members whose own minimum is satisfied; those form the core, recurse into each.
	taken := 0
	for _, member := range members {
		if taken >= k {
			break
		}
		if !isMemberMinSatisfied(member) {
			continue
		}
		collectCoreFromMember(member, subGroupOrderFn, taskOrderFn, core)
		taken++
	}
}

func collectCoreFromMember(
	member subgroup_info.SubGroupMember, subGroupOrderFn, taskOrderFn common_info.LessFn,
	core map[common_info.PodID]*pod_info.PodInfo,
) {
	switch m := member.(type) {
	case *subgroup_info.SubGroupSet:
		collectCoreFromSubGroupSet(m, subGroupOrderFn, taskOrderFn, core)
	case *subgroup_info.PodSet:
		collectCoreFromPodSet(m, taskOrderFn, core)
	}
}

// collectCoreFromPodSet adds the minAvailable highest-priority allocated pods of a leaf PodSet to core.
func collectCoreFromPodSet(
	ps *subgroup_info.PodSet, taskOrderFn common_info.LessFn,
	core map[common_info.PodID]*pod_info.PodInfo,
) {
	allocated := make([]*pod_info.PodInfo, 0, len(ps.GetPodInfos()))
	for _, task := range ps.GetPodInfos() {
		if pod_status.IsActiveAllocatedStatus(task.Status) {
			allocated = append(allocated, task)
		}
	}
	sort.Slice(allocated, func(i, j int) bool {
		return taskOrderFn(allocated[i], allocated[j])
	})

	minMembers := ps.GetMinMembersToSatisfy()
	for i := 0; i < minMembers && i < len(allocated); i++ {
		core[allocated[i].UID] = allocated[i]
	}
}

func isMemberMinSatisfied(member subgroup_info.SubGroupMember) bool {
	switch m := member.(type) {
	case *subgroup_info.SubGroupSet:
		return m.IsMinRequirementSatisfied()
	case *subgroup_info.PodSet:
		return m.GetNumActiveAllocatedTasks() >= int(m.GetMinAvailable())
	}
	return false
}

// IsMinRequirementSatisfied reports whether the job's root SubGroupSet has met its minimal shape,
// i.e. the whole core is allocated and any further allocation is elastic burst.
func IsMinRequirementSatisfied(job *PodGroupInfo) bool {
	root := job.RootSubGroupSet
	if root == nil {
		root = subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName, nil)
		for _, ps := range job.PodSets {
			root.AddPodSet(ps)
		}
	}
	return root.IsMinRequirementSatisfied()
}
