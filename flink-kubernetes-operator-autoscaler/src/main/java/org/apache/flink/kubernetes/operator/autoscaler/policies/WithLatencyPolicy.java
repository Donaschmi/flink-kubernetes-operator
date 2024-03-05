package org.apache.flink.kubernetes.operator.autoscaler.policies;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingOverrides;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingSummary;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.ResourceProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.ROCKSDB_CACHE_HIT_RATE_LOWER_THRESHOLD;

public class WithLatencyPolicy implements Policy {

    private final static Logger LOG = LoggerFactory.getLogger(WithLatencyPolicy.class);
    private static final int DEFAULT_MEMORY = 343;
    private static final int MAX_MEMORY = DEFAULT_MEMORY * 4;
    private static WithLatencyPolicy instance;
    private final Map<String, Integer> previousMemoryMap;

    public WithLatencyPolicy() {
        this.previousMemoryMap = new HashMap<>();
    }

    public static WithLatencyPolicy getInstance() {
        if (instance == null) {
            instance = new WithLatencyPolicy();
        }
        return instance;
    }

    @Override
    public ScalingOverrides scaleDecision(
            JobID jobID,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries,
            Configuration conf
    ) {
        var overrides = new ScalingOverrides();
        summaries.forEach(
                ((jobVertexID, scalingSummary) -> {
                    if (evaluatedMetrics.get(jobVertexID).containsKey(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
                        if (evaluatedMetrics.get(jobVertexID).get(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE).getAverage() < conf.get(ROCKSDB_CACHE_HIT_RATE_LOWER_THRESHOLD)) {
                            ScaleUpSummary scaleUpSummary = scaleUp(jobVertexID, scalingSummary.getCurrentParallelism());
                            LOG.info(scaleUpSummary.toString());
                            overrides.putResourceProfileOverride(jobVertexID.toString(), String.valueOf(scaleUpSummary.resourceProfile));
                            overrides.putParallelismOverride(jobVertexID.toString(), String.valueOf(scaleUpSummary.parallelism));
                        } else {
                            overrides.putResourceProfileOverride(
                                    jobVertexID.toString(),
                                    String.valueOf(
                                            ResourceProfile.newBuilder()
                                                    .setCpuCores(1.0)
                                                    .setTaskHeapMemoryMB(363)
                                                    .setTaskOffHeapMemoryMB(0)
                                                    .setManagedMemoryMB(previousMemoryMap.getOrDefault(jobVertexID.toString(), DEFAULT_MEMORY))
                                                    .setNetworkMemoryMB(84)
                                                    .build()
                                    ));
                            overrides.putParallelismOverride(
                                    jobVertexID.toString(),
                                    String.valueOf(scalingSummary.getNewParallelism())
                            );
                        }
                    } else {
                        var defaultResourceProfile = ResourceProfile.newBuilder()
                                .setCpuCores(1.0)
                                .setTaskHeapMemoryMB(363)
                                .setTaskOffHeapMemoryMB(0)
                                .setManagedMemoryMB(0)
                                .setNetworkMemoryMB(84)
                                .build();
                        overrides.putResourceProfileOverride(
                                jobVertexID.toString(),
                                String.valueOf(defaultResourceProfile));
                        overrides.putParallelismOverride(
                                jobVertexID.toString(),
                                String.valueOf(scalingSummary.getNewParallelism()));
                    }
                })
        );
        return overrides;
    }

    private ScaleUpSummary scaleUp(JobVertexID jobVertexId, int currentParallelism) {
        var rpb = ResourceProfile.newBuilder()
                .setCpuCores(1.0)
                .setTaskHeapMemoryMB(363)
                .setTaskOffHeapMemoryMB(0)
                .setManagedMemoryMB(0)
                .setNetworkMemoryMB(84);
        int memory = previousMemoryMap.getOrDefault(jobVertexId.toString(), DEFAULT_MEMORY);
        LOG.debug("Found previous memory for {} -> {}", jobVertexId, memory);
        if (memory * 2 > MAX_MEMORY) {
            return new ScaleUpSummary(rpb.setManagedMemoryMB(memory).build(), currentParallelism + 1);
        } else {
            previousMemoryMap.put(jobVertexId.toString(), memory * 2);
            return new ScaleUpSummary(rpb.setManagedMemoryMB(memory * 2).build(), currentParallelism);
        }
    }

    private static final class ScaleUpSummary {
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
