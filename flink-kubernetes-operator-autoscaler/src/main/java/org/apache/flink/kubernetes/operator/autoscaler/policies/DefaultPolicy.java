package org.apache.flink.kubernetes.operator.autoscaler.policies;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingOverrides;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingSummary;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.utils.ResourceProfileUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;

public class DefaultPolicy implements Policy {

    @Override
    public ScalingOverrides scaleDecision(
            JobID jobID, Map<JobVertexID,
            Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries,
            Configuration conf) {

        var overrides = new ScalingOverrides();
        var defaultResourceProfile = ResourceProfileUtils.getDefaultResourceProfile(conf);
        var resourceProfile = defaultResourceProfile.f0.setManagedMemoryMB(defaultResourceProfile.f1
        ).build();
        summaries.forEach(
                ((jobVertexID, scalingSummary) -> {
                    overrides.putResourceProfileOverride(
                            jobVertexID.toString(),
                            String.valueOf(resourceProfile)
                    );
                    overrides.putParallelismOverride(
                            jobVertexID.toString(),
                            String.valueOf(scalingSummary.getNewParallelism())
                    );
                })
        );
        return overrides;
    }
}
