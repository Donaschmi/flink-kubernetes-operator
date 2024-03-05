package org.apache.flink.kubernetes.operator.autoscaler.policies;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingOverrides;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingSummary;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.utils.ResourceProfileUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.ResourceProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.ROCKSDB_CACHE_HIT_RATE_LOWER_THRESHOLD;

public class BasicHybridPolicy implements Policy {

    private final static Logger LOG = LoggerFactory.getLogger(BasicHybridPolicy.class);

    private static BasicHybridPolicy instance;

    public static BasicHybridPolicy getInstance() {
        if (instance == null) {
            instance = new BasicHybridPolicy();
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
                    var defaultResourceProfile = ResourceProfileUtils.getDefaultResourceProfile(conf);
                    if (evaluatedMetrics.get(jobVertexID).containsKey(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
                        if (evaluatedMetrics.get(jobVertexID).get(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE).getAverage() < conf.get(ROCKSDB_CACHE_HIT_RATE_LOWER_THRESHOLD)) {
                            ScaleUpSummary scaleUpSummary = scaleUp(jobID, jobVertexID, scalingSummary.getCurrentParallelism(), defaultResourceProfile);
                            LOG.info(scaleUpSummary.toString());
                            overrides.putResourceProfileOverride(jobVertexID.toString(), String.valueOf(scaleUpSummary.resourceProfile));
                            overrides.putParallelismOverride(jobVertexID.toString(), String.valueOf(scaleUpSummary.parallelism));
                        } else {
                            var resourceProfile = defaultResourceProfile.f0
                                    .setManagedMemoryMB(getPreviousMemoryOrDefault(jobID, jobVertexID, defaultResourceProfile.f1))
                                    .build();
                            overrides.putResourceProfileOverride(
                                    jobVertexID.toString(),
                                    String.valueOf(resourceProfile)
                            );
                            overrides.putParallelismOverride(
                                    jobVertexID.toString(),
                                    String.valueOf(scalingSummary.getNewParallelism())
                            );
                        }
                    } else {
                        var resourceProfile = defaultResourceProfile.f0.build();
                        overrides.putResourceProfileOverride(
                                jobVertexID.toString(),
                                String.valueOf(resourceProfile));
                        overrides.putParallelismOverride(
                                jobVertexID.toString(),
                                String.valueOf(scalingSummary.getNewParallelism()));
                    }
                })
        );
        return overrides;
    }

    private ScaleUpSummary scaleUp(JobID jobID, JobVertexID jobVertexID, int currentParallelism, Tuple2<ResourceProfile.Builder, Integer> defaultResourceProfile) {
        var rpb = defaultResourceProfile.f0;
        int memory = getPreviousMemoryOrDefault(jobID, jobVertexID, defaultResourceProfile.f1);
        LOG.debug("Found previous memory for {} -> {}", jobVertexID.toString(), memory);
        if ((memory * 2) > (defaultResourceProfile.f1 * 4)) {
            return new ScaleUpSummary(rpb.setManagedMemoryMB(memory).build(), currentParallelism + 1);
        } else {
            var newMemory = memory * 2;
            setPreviousMemory(jobID, jobVertexID, newMemory);
            return new ScaleUpSummary(rpb.setManagedMemoryMB(newMemory).build(), currentParallelism);
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
