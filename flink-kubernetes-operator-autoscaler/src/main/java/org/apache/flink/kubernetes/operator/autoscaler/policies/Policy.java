package org.apache.flink.kubernetes.operator.autoscaler.policies;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingOverrides;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingSummary;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.ResourceProfile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public interface Policy {

    Map<JobID, Map<JobVertexID, Integer>> previousMemoryMap = new HashMap<>();

    static Policy getInstance(Configuration conf) {
        String policy = conf.getString(AutoScalerOptions.SCALING_POLICY);
        switch (policy) {
            case "static":
                return StaticPolicy.getInstance();
            case "basic":
                return BasicPolicy.getInstance();
            case "basic-hybrid":
                return BasicHybridPolicy.getInstance();
            case "basic-scaleup":
                return new BasicScaleUpPolicy();
            case "working-set":
                return new WorkingSetPolicy();
            case "with-latency":
                return new WithLatencyPolicy();
            default:
                return new DefaultPolicy();

        }
    }

    ScalingOverrides scaleDecision(
            JobID jobID,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries,
            Configuration conf
    );

    default Integer getPreviousMemoryOrDefault(JobID jobID, JobVertexID jobVertexID, Integer defaultMemory) {
        if (previousMemoryMap.containsKey(jobID)) {
            return previousMemoryMap.get(jobID).getOrDefault(jobVertexID, defaultMemory);
        } else {
            return defaultMemory;
        }
    }

    default void setPreviousMemory(JobID jobID, JobVertexID jobVertexID, Integer memory) {
        if (!previousMemoryMap.containsKey(jobID)) {
            previousMemoryMap.put(jobID, new HashMap<JobVertexID, Integer>());
        }
        previousMemoryMap.get(jobID).put(jobVertexID, memory);
    }

    default ArrayList<JobVertexID> getStatefulOperatorsID(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        var stateful = new ArrayList<JobVertexID>();
        evaluatedMetrics.forEach((jobVertexID, scalingMetricEvaluatedScalingMetricMap) -> {
            if (scalingMetricEvaluatedScalingMetricMap.containsKey(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
                stateful.add(jobVertexID);
            }
        });
        return stateful;
    }

    default Map<JobVertexID, ScaleUpDecision> scaleDecisions(Map<JobVertexID, ScalingSummary> summaries) {
        var scaleUpDecision = new HashMap<JobVertexID, ScaleUpDecision>();
        summaries.forEach((jobVertexID, scalingSummary) -> {
            ScaleUpDecision decision;
            if (scalingSummary.isScaledUp()) {
                decision = ScaleUpDecision.INCREASE;
            } else if (scalingSummary.isScaledDown()) {
                decision = ScaleUpDecision.DECREASE;
            } else {
                decision = ScaleUpDecision.SAME;
            }
            scaleUpDecision.put(jobVertexID, decision);
        });
        return scaleUpDecision;
    }

    enum ScaleUpDecision {
        INCREASE,
        DECREASE,
        SAME
    }

    final class ScaleUpSummary {
        ResourceProfile resourceProfile;
        int parallelism;

        public ScaleUpSummary(ResourceProfile resourceProfile, int parallelism) {
            this.resourceProfile = resourceProfile;
            this.parallelism = parallelism;
        }

        @Override
        public String toString() {
            return "ScaleUpSummary{" +
                    "resourceProfile=" + resourceProfile +
                    ", parallelism=" + parallelism +
                    '}';
        }
    }

}
