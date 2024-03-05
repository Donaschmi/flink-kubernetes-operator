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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StaticPolicy implements Policy {

    private final static Logger LOG = LoggerFactory.getLogger(StaticPolicy.class);

    private static StaticPolicy instance;
    private static int scaleCount = 0;
    private final Map<String, Integer> previousMemoryMap;

    public StaticPolicy() {
        this.previousMemoryMap = new HashMap<>();
    }

    public static StaticPolicy getInstance() {
        if (instance == null) {
            instance = new StaticPolicy();
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
                    if (statefulOperatorsID.contains(jobVertexID)) { // Operator is Stateful
                        int mem;
                        if (scaleCount == 0) {
                            mem = defaultResourceProfile.f1 * 2;
                            parallelism = 2;
                        } else if (scaleCount == 1) {
                            mem = defaultResourceProfile.f1 * 2;
                            parallelism = 4;
                        } else if (scaleCount == 2) {
                            mem = defaultResourceProfile.f1 * 2;
                            parallelism = 6;
                        } else {
                            mem = defaultResourceProfile.f1 * 2;
                            parallelism = 8;
                        }
                        resourceProfile = defaultResourceProfile.f0
                                .setManagedMemoryMB(mem)
                                .build();
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

                })
        );
        scaleCount++;
        return overrides;
    }

    private int numberOfReplicas(Map<JobVertexID, ScalingSummary> summaries) {
        AtomicInteger cpus = new AtomicInteger();
        summaries.forEach((jobVertexID, scalingSummary) -> {
            cpus.addAndGet(scalingSummary.getNewParallelism());
        });
        return cpus.get() / 4;
    }

    private ScaleUpSummary scaleUp(JobVertexID jobVertexId, int currentParallelism, Tuple2<ResourceProfile.Builder, Integer> defaultResourceProfile) {
        var rpb = defaultResourceProfile.f0;
        int memory = previousMemoryMap.getOrDefault(jobVertexId.toString(), defaultResourceProfile.f1);
        LOG.debug("Found previous memory for {} -> {}", jobVertexId, memory);
        if ((memory * 2) > (defaultResourceProfile.f1 * 4)) {
            return new ScaleUpSummary(rpb.setManagedMemoryMB(memory).build(), currentParallelism + 1);
        } else {
            previousMemoryMap.put(jobVertexId.toString(), memory * 2);
            return new ScaleUpSummary(rpb.setManagedMemoryMB(memory * 2).build(), currentParallelism);
        }
    }

}
