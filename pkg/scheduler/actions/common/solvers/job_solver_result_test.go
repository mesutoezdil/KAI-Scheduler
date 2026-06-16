// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package solvers

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/common/scenariosearch"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common/solvers/scenario"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/queue_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
)

func TestNewJobsSolverDefaultsNilBudgetToUnlimited(t *testing.T) {
	solver := NewJobsSolver(nil, nil, nil, framework.Reclaim, nil)

	require.NotNil(t, solver.actionBudget)
	require.False(t, solver.actionBudget.Exhausted())
	require.Greater(t, solver.actionBudget.BeginJob().Remaining(), time.Hour)
}

func TestNewJobsSolverDefaultsOmittedBudgetToUnlimited(t *testing.T) {
	solver := NewJobsSolver(nil, nil, nil, framework.Reclaim)

	require.NotNil(t, solver.actionBudget)
	require.False(t, solver.actionBudget.Exhausted())
	require.Greater(t, solver.actionBudget.BeginJob().Remaining(), time.Hour)
}

func TestSolveWithResultReturnsTerminalResultWhenNoTasksToAllocate(t *testing.T) {
	solver := NewJobsSolver(nil, nil, nil, framework.Reclaim, nil)
	pendingJob := podgroup_info.NewPodGroupInfo("pending-job")

	solved, statement, victims, result := solver.SolveWithResult(&framework.Session{}, pendingJob)

	require.False(t, solved)
	require.Nil(t, statement)
	require.Empty(t, victims)
	require.Equal(t, SearchResultGeneratorsExhausted, result.Reason())
	require.False(t, result.ReducedBudget())
	require.False(t, result.EnteredSearch())
}

func TestSolveWithResultRecordsNoSearchMetricAsNotAttempted(t *testing.T) {
	labels := map[string]string{
		"action":         "reclaim",
		"result":         string(SearchResultNotAttempted),
		"reduced_budget": "false",
	}
	before := scenarioSearchCounterValue(t, "scenario_search_jobs_total", labels)
	solver := NewJobsSolver(nil, nil, nil, framework.Reclaim, nil)
	pendingJob := podgroup_info.NewPodGroupInfo("pending-job")

	_, _, _, result := solver.SolveWithResult(&framework.Session{}, pendingJob)

	require.Equal(t, SearchResultGeneratorsExhausted, result.Reason())
	require.False(t, result.EnteredSearch())
	require.Equal(t, before+1, scenarioSearchCounterValue(t, "scenario_search_jobs_total", labels))
}

func TestSolveWithResultReturnsNoGeneratorWhenGeneratorFuncIsNil(t *testing.T) {
	ssn, pendingJob := newJobSolverResultTestSession(t, 1)
	solver := NewJobsSolver(nil, nil, nil, framework.Reclaim, nil)

	solved, statement, victims, result := solver.SolveWithResult(ssn, pendingJob)

	require.False(t, solved)
	require.Nil(t, statement)
	require.Empty(t, victims)
	require.Equal(t, SearchResultNoGenerator, result.Reason())
	require.False(t, result.ReducedBudget())
	require.False(t, result.EnteredSearch())
}

func TestSolveWithResultReturnsNoGeneratorWhenGeneratorReturnsNil(t *testing.T) {
	ssn, pendingJob := newJobSolverResultTestSession(t, 1)
	solver := NewJobsSolver(
		nil,
		nil,
		func() *utils.JobsOrderByQueues {
			return nil
		},
		framework.Reclaim,
		nil,
	)

	solved, statement, victims, result := solver.SolveWithResult(ssn, pendingJob)

	require.False(t, solved)
	require.Nil(t, statement)
	require.Empty(t, victims)
	require.Equal(t, SearchResultNoGenerator, result.Reason())
	require.False(t, result.ReducedBudget())
	require.False(t, result.EnteredSearch())
}

func TestSolveWithResultRecordsGeneratorExhaustedMetricAfterGeneratorAttempt(t *testing.T) {
	labels := map[string]string{
		"action":         "reclaim",
		"result":         string(SearchResultGeneratorsExhausted),
		"reduced_budget": "false",
	}
	before := scenarioSearchCounterValue(t, "scenario_search_jobs_total", labels)
	ssn, pendingJob := newJobSolverResultTestSession(t, 1)
	ssn.AddScenarioGenerator("empty", portfolioTestFactory(&portfolioTestGenerator{name: "empty"}), framework.Reclaim)
	solver := NewJobsSolver(
		nil,
		nil,
		func() *utils.JobsOrderByQueues {
			return utils.GetVictimsQueue(ssn, nil)
		},
		framework.Reclaim,
		nil,
	)

	_, _, _, result := solver.SolveWithResult(ssn, pendingJob)

	require.Equal(t, SearchResultGeneratorsExhausted, result.Reason())
	require.False(t, result.EnteredSearch())
	require.Equal(t, before+1, scenarioSearchCounterValue(t, "scenario_search_jobs_total", labels))
}

func TestSolveWithResultRecordsUnsolvedScenarioDurationAfterSimulation(t *testing.T) {
	generatorName := "test-unsolved-duration"
	labels := map[string]string{
		"action":    "reclaim",
		"generator": generatorName,
		"result":    scenarioSearchResultUnsolved,
	}
	before := scenarioSearchHistogramCount(t, "scenario_search_duration_seconds", labels)
	ssn, pendingJob := newJobSolverResultTestSession(t, 1)
	ssn.ClusterInfo.Nodes = map[string]*node_info.NodeInfo{"node-1": {}}
	scenarioToSolve := scenario.NewByNodeScenario(
		ssn, pendingJob,
		podgroup_info.GetTasksToAllocate(pendingJob, ssn.SubGroupOrderFn, ssn.TaskOrderFn, false),
		nil, nil,
	)
	ssn.AddScenarioGenerator(generatorName, portfolioTestFactory(&portfolioTestGenerator{
		name:      generatorName,
		scenarios: []api.ScenarioInfo{scenarioToSolve},
	}), framework.Reclaim)
	solver := NewJobsSolver(
		nil,
		nil,
		func() *utils.JobsOrderByQueues {
			return utils.GetVictimsQueue(ssn, nil)
		},
		framework.Reclaim,
		nil,
	)

	solver.SolveWithResult(ssn, pendingJob)

	require.Equal(t, before+1, scenarioSearchHistogramCount(t, "scenario_search_duration_seconds", labels))
}

func TestSearchMaxSolvableKSkipsSingleTaskFullProbe(t *testing.T) {
	ssn, pendingJob := newJobSolverResultTestSession(t, 1)
	actionBudget := newUnlimitedActionSearchBudget(framework.Reclaim)
	jobBudget := actionBudget.BeginJob()
	solver := NewJobsSolver(
		nil,
		nil,
		func() *utils.JobsOrderByQueues {
			t.Fatal("single-task search must leave the full probe to SolveWithResult")
			return nil
		},
		framework.Reclaim,
		actionBudget,
	)
	tasksToAllocate := podgroup_info.GetTasksToAllocate(pendingJob, ssn.SubGroupOrderFn, ssn.TaskOrderFn, false)

	maxSolvedK, result := solver.searchMaxSolvableK(ssn, &solvingState{}, pendingJob, tasksToAllocate, jobBudget)

	require.Equal(t, 0, maxSolvedK)
	require.Nil(t, result)
}

func TestSolveWithResultReportsDeadlineBeforeScenarioSimulation(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	actionBudget, err := newActionSearchBudgetWithClock(
		sessionWithScenarioSearchBudgets(&conf.ScenarioSearchBudgets{
			MaxActionSearchDuration: map[string]string{
				scenariosearch.ActionReclaim: "10ms",
			},
			MaxJobSearchDuration: "1ms",
		}),
		framework.Reclaim,
		clock.Now,
	)
	require.NoError(t, err)
	ssn, pendingJob := newJobSolverResultTestSession(t, 1)
	ssn.AddScenarioGenerator("deadline-test", NewMultiNodeGangGenerator, framework.Reclaim)
	solver := NewJobsSolver(
		nil,
		nil,
		func() *utils.JobsOrderByQueues {
			clock.Advance(time.Millisecond)
			return utils.GetVictimsQueue(ssn, nil)
		},
		framework.Reclaim,
		actionBudget,
	)

	solved, statement, victims, result := solver.SolveWithResult(ssn, pendingJob)

	require.False(t, solved)
	require.Nil(t, statement)
	require.Empty(t, victims)
	require.Equal(t, SearchResultDeadlineExhausted, result.Reason())
	require.False(t, result.EnteredSearch())
}

func TestSearchMaxSolvableKPreservesEnteredSearchAfterTerminalPartialProbe(t *testing.T) {
	probes := map[int]*SearchResult{
		1: solvedSearchResult(&solutionResult{solved: true}, false),
		2: terminalSearchResult(SearchResultDeadlineExhausted, false, false),
	}

	maxSolvedK, result := searchMaxSolvableK(3, func(k int) *SearchResult {
		return probes[k]
	})

	require.Equal(t, 0, maxSolvedK)
	require.Equal(t, SearchResultDeadlineExhausted, result.Reason())
	require.True(t, result.EnteredSearch())
}

func TestPreserveEnteredSearchMarksTerminalResult(t *testing.T) {
	result := terminalSearchResult(SearchResultDeadlineExhausted, false, false)

	preserveEnteredSearch(result, true)

	require.True(t, result.EnteredSearch())
}

func newJobSolverResultTestSession(t *testing.T, tasksCount int) (*framework.Session, *podgroup_info.PodGroupInfo) {
	t.Helper()

	pendingJob, _ := createJobWithTasks(tasksCount, 1, "team-a", v1.PodPending, nil)
	defaultQueue := createQueue("default")
	defaultQueue.ParentQueue = ""
	submitQueue := createQueue("team-a")

	return &framework.Session{
		ClusterInfo: &api.ClusterInfo{
			PodGroupInfos: map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{
				pendingJob.UID: pendingJob,
			},
			Queues: map[common_info.QueueID]*queue_info.QueueInfo{
				defaultQueue.UID: defaultQueue,
				submitQueue.UID:  submitQueue,
			},
			Nodes: map[string]*node_info.NodeInfo{},
		},
	}, pendingJob
}

func scenarioSearchCounterValue(t *testing.T, metricName string, labels map[string]string) float64 {
	t.Helper()

	metric := scenarioSearchMetric(t, metricName, labels)
	if metric == nil || metric.GetCounter() == nil {
		return 0
	}
	return metric.GetCounter().GetValue()
}

func scenarioSearchHistogramCount(t *testing.T, metricName string, labels map[string]string) uint64 {
	t.Helper()

	metric := scenarioSearchMetric(t, metricName, labels)
	if metric == nil || metric.GetHistogram() == nil {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func scenarioSearchMetric(t *testing.T, metricName string, labels map[string]string) *dto.Metric {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			if scenarioSearchMetricHasLabels(metric, labels) {
				return metric
			}
		}
	}
	return nil
}

func scenarioSearchMetricHasLabels(metric *dto.Metric, labels map[string]string) bool {
	if len(metric.GetLabel()) != len(labels) {
		return false
	}
	for _, label := range metric.GetLabel() {
		expectedValue, found := labels[label.GetName()]
		if !found || expectedValue != label.GetValue() {
			return false
		}
	}
	return true
}
