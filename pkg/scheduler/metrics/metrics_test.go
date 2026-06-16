// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	schedmetrics "github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/metrics"
)

func TestScenarioSearchMetricWrappersUseExpectedLabels(t *testing.T) {
	jobsLabels := map[string]string{
		"action":         "test-action-jobs",
		"result":         "solved",
		"reduced_budget": "true",
	}
	actionExhaustedLabels := map[string]string{
		"action": "test-action-exhausted",
	}
	scenariosLabels := map[string]string{
		"action":    "test-action-scenarios",
		"generator": "test-generator-labels",
		"state":     "emitted",
	}
	jobsBefore := counterValueOrZero(t, "scenario_search_jobs_total", jobsLabels)
	actionExhaustedBefore := counterValueOrZero(
		t, "scenario_search_action_budget_exhausted_total", actionExhaustedLabels,
	)
	scenariosBefore := counterValueOrZero(t, "scenario_search_scenarios_total", scenariosLabels)

	schedmetrics.IncScenarioSearchJobs("test-action-jobs", "solved", true)
	schedmetrics.IncScenarioSearchActionBudgetExhausted("test-action-exhausted")
	schedmetrics.IncScenarioSearchScenario("test-action-scenarios", "test-generator-labels", "emitted")

	require.Equal(t, jobsBefore+1, counterValue(t, "scenario_search_jobs_total", jobsLabels))
	require.Equal(t, actionExhaustedBefore+1, counterValue(
		t, "scenario_search_action_budget_exhausted_total", actionExhaustedLabels,
	))
	require.Equal(t, scenariosBefore+1, counterValue(t, "scenario_search_scenarios_total", scenariosLabels))
}

func TestScenarioSearchDurationMetricObservesSeconds(t *testing.T) {
	labels := map[string]string{
		"action":    "test-action-duration",
		"generator": "test-generator-duration",
		"result":    "generators_exhausted",
	}
	countBefore, sumBefore := histogramSnapshot(t, "scenario_search_duration_seconds", labels)

	schedmetrics.ObserveScenarioSearchDuration(
		"test-action-duration", "test-generator-duration", "generators_exhausted", 2500*time.Millisecond,
	)

	countAfter, sumAfter := histogramSnapshot(t, "scenario_search_duration_seconds", labels)

	require.Equal(t, countBefore+1, countAfter)
	require.InEpsilon(t, sumBefore+2.5, sumAfter, 0.000001)
}

func TestScenarioSearchConfiguredBudgetMetricsAcceptUnlimitedZero(t *testing.T) {
	schedmetrics.SetScenarioSearchActionBudget("test-action-zero-budget", 0)
	schedmetrics.SetScenarioSearchJobBudget(0)
	schedmetrics.SetScenarioSearchGeneratorBudget("test-generator-zero-budget", 0)

	require.Equal(t, 0.0, gaugeValue(t, "scenario_search_action_budget_configured_seconds", map[string]string{
		"action": "test-action-zero-budget",
	}))
	require.Equal(t, 0.0, gaugeValue(t, "scenario_search_job_budget_configured_seconds", nil))
	require.Equal(t, 0.0, gaugeValue(t, "scenario_search_generator_budget_configured_seconds", map[string]string{
		"generator": "test-generator-zero-budget",
	}))
}

func counterValue(t *testing.T, metricName string, labels map[string]string) float64 {
	t.Helper()

	metric := findMetric(t, metricName, labels)
	require.NotNil(t, metric.GetCounter())
	return metric.GetCounter().GetValue()
}

func counterValueOrZero(t *testing.T, metricName string, labels map[string]string) float64 {
	t.Helper()

	metric := findMetricOrNil(t, metricName, labels)
	if metric == nil || metric.GetCounter() == nil {
		return 0
	}
	return metric.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, metricName string, labels map[string]string) float64 {
	t.Helper()

	metric := findMetric(t, metricName, labels)
	require.NotNil(t, metric.GetGauge())
	return metric.GetGauge().GetValue()
}

func histogramSnapshot(t *testing.T, metricName string, labels map[string]string) (uint64, float64) {
	t.Helper()

	metric := findMetricOrNil(t, metricName, labels)
	if metric == nil || metric.GetHistogram() == nil {
		return 0, 0
	}
	histogram := metric.GetHistogram()
	return histogram.GetSampleCount(), histogram.GetSampleSum()
}

func findMetric(t *testing.T, metricName string, labels map[string]string) *dto.Metric {
	t.Helper()

	family := findMetricFamily(t, metricName)
	for _, metric := range family.GetMetric() {
		if metricHasLabels(metric, labels) {
			return metric
		}
	}
	t.Fatalf("metric %q with labels %v not found", metricName, labels)
	return nil
}

func findMetricOrNil(t *testing.T, metricName string, labels map[string]string) *dto.Metric {
	t.Helper()

	family := findMetricFamilyOrNil(t, metricName)
	if family == nil {
		return nil
	}
	for _, metric := range family.GetMetric() {
		if metricHasLabels(metric, labels) {
			return metric
		}
	}
	return nil
}

func findMetricFamily(t *testing.T, metricName string) *dto.MetricFamily {
	t.Helper()

	if family := findMetricFamilyOrNil(t, metricName); family != nil {
		return family
	}
	t.Fatalf("metric family %q not found", metricName)
	return nil
}

func findMetricFamilyOrNil(t *testing.T, metricName string) *dto.MetricFamily {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == metricName {
			return family
		}
	}
	return nil
}

func metricHasLabels(metric *dto.Metric, labels map[string]string) bool {
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
