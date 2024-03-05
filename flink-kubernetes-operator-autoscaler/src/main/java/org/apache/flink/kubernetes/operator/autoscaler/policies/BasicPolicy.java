package org.apache.flink.kubernetes.operator.autoscaler.policies;

import org.apache.flink.api.common.JobID;
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
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.ROCKSDB_CACHE_HIT_RATE_UPPER_THRESHOLD;

public class BasicPolicy implements Policy {

    private final static Logger LOG = LoggerFactory.getLogger(BasicPolicy.class);

    private static BasicPolicy instance;

    public static BasicPolicy getInstance() {
        if (instance == null) {
            instance = new BasicPolicy();
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
        var statefulOperatorsID = getStatefulOperatorsID(evaluatedMetrics);
        var scaleDecisions = scaleDecisions(summaries);
        summaries.forEach(
                ((jobVertexID, scalingSummary) -> {
                    var defaultResourceProfile = ResourceProfileUtils.getDefaultResourceProfile(conf);
                    ResourceProfile resourceProfile;
                    int parallelism;
                    var prevMem = getPreviousMemoryOrDefault(jobID, jobVertexID, defaultResourceProfile.f1);
                    if (statefulOperatorsID.contains(jobVertexID)) { // Operator is Stateful
                        if (scaleDecisions.get(jobVertexID) == ScaleUpDecision.SAME) {
                            resourceProfile = defaultResourceProfile.f0
                                    .setManagedMemoryMB(prevMem)
                                    .build();
                            parallelism = scalingSummary.getNewParallelism();
                        } else if (scaleDecisions.get(jobVertexID) == ScaleUpDecision.INCREASE) {
                            LOG.info(jobID + " -> INCREASE");
                            var cacheHitRate = evaluatedMetrics.get(jobVertexID).get(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE).getAverage();
                            if (cacheHitRate > conf.get(ROCKSDB_CACHE_HIT_RATE_UPPER_THRESHOLD)) {
                                resourceProfile = defaultResourceProfile.f0
                                        .setManagedMemoryMB(prevMem)
                                        .build();
                                parallelism = scalingSummary.getNewParallelism();
                            } else if (cacheHitRate > conf.get(ROCKSDB_CACHE_HIT_RATE_LOWER_THRESHOLD)) {
                                if (canDoubleMemory(jobID, jobVertexID, defaultResourceProfile.f1)) {
                                    resourceProfile = defaultResourceProfile.f0
                                            .setManagedMemoryMB(prevMem * 2)
                                            .build();
                                    parallelism = (int) Math.ceil(scalingSummary.getCurrentParallelism() + (scalingSummary.getNewParallelism() - scalingSummary.getCurrentParallelism()) / 2.0);
                                } else {
                                    resourceProfile = defaultResourceProfile.f0
                                            .setManagedMemoryMB(prevMem)
                                            .build();
                                    parallelism = scalingSummary.getNewParallelism();
                                }
                            } else {
                                if (canDoubleMemory(jobID, jobVertexID, defaultResourceProfile.f1)) {
                                    prevMem = prevMem * 2;
                                }
                                resourceProfile = defaultResourceProfile.f0
                                        .setManagedMemoryMB(prevMem)
                                        .build();
                                parallelism = scalingSummary.getNewParallelism();
                            }
                        } else { // Decrease
                            LOG.info(jobID + " -> DECREASE");
                            resourceProfile = defaultResourceProfile.f0
                                    .setManagedMemoryMB(prevMem)
                                    .build();
                            parallelism = scalingSummary.getNewParallelism();
                        }
                    } else { // Operator is Stateless
                        resourceProfile = defaultResourceProfile.f0.build();
                        parallelism = scalingSummary.getNewParallelism();

                    }
                    overrides.putResourceProfileOverride(
                            jobVertexID.toString(),
                            String.valueOf(resourceProfile)
                    );
                    overrides.putParallelismOverride(
                            jobVertexID.toString(),
                            String.valueOf(parallelism)
                    );
                    this.setPreviousMemory(jobID, jobVertexID, resourceProfile.getManagedMemory().getMebiBytes());
                })
        );
        return overrides;
    }

    private boolean canDoubleMemory(JobID jobID, JobVertexID jobVertexID, Integer defaultMemory) {
        return (getPreviousMemoryOrDefault(jobID, jobVertexID, defaultMemory) * 2) <= (defaultMemory * 4);
    }

}
