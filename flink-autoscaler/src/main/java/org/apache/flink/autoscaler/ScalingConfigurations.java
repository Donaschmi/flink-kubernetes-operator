package org.apache.flink.autoscaler;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.LIST_STATE_GET_MEAN_LATENCY;

public class ScalingConfigurations {

    public static final int MAX_MEMORY_LEVEL = 2;

    private final HashMap<JobID, HashMap<Integer, ScalingConfiguration>> scalingConfiguration;

    private final HashMap <JobID, Integer> currentPeriod;

    public ScalingConfigurations() {
        this.scalingConfiguration = new HashMap<>();
        this.currentPeriod =  new HashMap<>();
    }

    @VisibleForTesting
    public int getPeriod(JobID jobID) {
        return currentPeriod.getOrDefault(jobID, 0);
    }

    public ScalingConfiguration setCurrentConfiguration(
            JobID jobID,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<JobVertexID, ScalingSummary> summaries,
            int period) {
        if (!this.scalingConfiguration.containsKey(jobID)){
            this.scalingConfiguration.put(jobID, new HashMap<>());
        }
        var configuration = new ScalingConfiguration(vertexMetrics, summaries);
        this.scalingConfiguration.get(jobID).put(period, configuration);
        return configuration;
    }

    public ScalingInformation getPreviousScalingInformation(JobID jobID, JobVertexID jobVertexId, int period) {
        if (scalingConfiguration.get(jobID).containsKey(period-1)) {
            return scalingConfiguration.get(jobID).get(period-1).getFromJobVertexId(jobVertexId);
        }
        return null;
    }

    public ScalingConfiguration getCurrentConfiguration(JobID jobID, int period) {
        return scalingConfiguration.get(jobID).get(period);
    }

    @Override
    public String toString() {
        return "ScalingConfigurations{" +
                "currentPeriod=" + currentPeriod +
                ", scalingConfiguration=" + scalingConfiguration +
                '}';
    }

    public static class ScalingInformation {
        @Getter @Setter
        private int parallelism;
        @Getter @Setter
        private int memoryLevel = 0;
        @Getter @Setter
        private boolean verticalScaling = false;
        @Getter @Setter
        private boolean horizontalScaling = false;
        @Getter
        private final double avgCacheHitRate;
        @Getter
        private final double avgStateLatency;

        public ScalingInformation(int parallelism, double avgCacheHitRate, double avgStateLatency) {
            this.parallelism = parallelism;
            this.avgCacheHitRate = avgCacheHitRate;
            this.avgStateLatency = avgStateLatency;
        }

        @Override
        public String toString() {
            return "ScalingInformation{" +
                    "parallelism=" + parallelism +
                    ", memoryLevel=" + memoryLevel +
                    ", verticalScaling=" + verticalScaling +
                    ", horizontalScaling=" + horizontalScaling +
                    ", avgCacheHitRate=" + avgCacheHitRate +
                    ", avgStateLatency=" + avgStateLatency +
                    '}';
        }
    }

    public static class ScalingConfiguration {

        @Getter
        private final HashMap<JobVertexID, ScalingInformation> scaling;

        public ScalingConfiguration(Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
           this.scaling = new HashMap<>();
           evaluatedMetrics.forEach(
                   (id, metrics) -> {
                       var parallelism = 1;
                       var avgCacheHitRate = 0.0;
                       var avgStateLatency = 0.0;
                       if (summaries.containsKey(id)) {
                           parallelism = summaries.get(id).getNewParallelism();
                       } else {
                           parallelism = (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();
                       }
                       if (evaluatedMetrics.get(id).containsKey(ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
                           avgCacheHitRate = evaluatedMetrics.get(id).get(ROCKS_DB_BLOCK_CACHE_HIT_RATE).getAverage();
                       }
                       if (evaluatedMetrics.get(id).containsKey(LIST_STATE_GET_MEAN_LATENCY)) {
                           avgCacheHitRate = evaluatedMetrics.get(id).get(LIST_STATE_GET_MEAN_LATENCY).getAverage();
                       }
                       scaling.put(id, new ScalingInformation(parallelism, avgCacheHitRate, avgStateLatency));
                   });
       }

       public ScalingInformation getFromJobVertexId(JobVertexID jobVertexID) {
           return scaling.get(jobVertexID);
       }

        @Override
        public String toString() {
            return "ScalingConfiguration{" +
                    "scaling=" + scaling +
                    '}';
        }
    }
}
