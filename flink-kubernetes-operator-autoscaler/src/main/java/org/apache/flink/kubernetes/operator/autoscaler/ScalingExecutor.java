/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.policies.Policy;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.ResourceProfile;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.*;

/** Class responsible for executing scaling decisions. */
public class ScalingExecutor {
    public static final String SCALING_SUMMARY_ENTRY =
            " Vertex ID %s | Parallelism %d -> %d | Processing capacity %.2f -> %.2f | Target data rate %.2f";
    public static final String SCALING_SUMMARY_HEADER_SCALING_DISABLED =
            "Recommended parallelism change:";
    public static final String SCALING_SUMMARY_HEADER_SCALING_ENABLED = "Scaling vertices:";
    private static final Logger LOG = LoggerFactory.getLogger(ScalingExecutor.class);

    private final JobVertexScaler jobVertexScaler;
    private final EventRecorder eventRecorder;
    private Clock clock = Clock.system(ZoneId.systemDefault());

    public ScalingExecutor(EventRecorder eventRecorder) {
        this(new JobVertexScaler(eventRecorder), eventRecorder);
    }

    public ScalingExecutor(JobVertexScaler jobVertexScaler, EventRecorder eventRecorder) {
        this.jobVertexScaler = jobVertexScaler;
        this.eventRecorder = eventRecorder;
    }

    private static String scalingReport(
            Map<JobVertexID, ScalingSummary> scalingSummaries, boolean scalingEnabled) {
        StringBuilder sb =
                new StringBuilder(
                        scalingEnabled
                                ? SCALING_SUMMARY_HEADER_SCALING_ENABLED
                                : SCALING_SUMMARY_HEADER_SCALING_DISABLED);
        scalingSummaries.forEach(
                (v, s) ->
                        sb.append(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        v,
                                        s.getCurrentParallelism(),
                                        s.getNewParallelism(),
                                        s.getMetrics().get(TRUE_PROCESSING_RATE).getAverage(),
                                        s.getMetrics().get(EXPECTED_PROCESSING_RATE) != null ? s.getMetrics().get(EXPECTED_PROCESSING_RATE).getCurrent() : 0, // Justin
                                        s.getMetrics().get(TARGET_DATA_RATE).getAverage())));
        return sb.toString();
    }

    private void updateRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {
        scalingSummaries.forEach(
                (jobVertexID, scalingSummary) -> {
                    evaluatedMetrics
                            .get(jobVertexID)
                            .put(
                                    ScalingMetric.RECOMMENDED_PARALLELISM,
                                    EvaluatedScalingMetric.of(scalingSummary.getNewParallelism()));
                });
    }

    @VisibleForTesting
    public static Map<String, String> getVertexResourceProfileOverrides(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var defaultResourceProfile = ResourceProfile.newBuilder()
                .setCpuCores(1.0)
                .setTaskHeapMemoryMB(363)
                .setTaskOffHeapMemoryMB(0)
                .setManagedMemoryMB(0)
                .setNetworkMemoryMB(84)
                .build();
        var statefulResourceProfile = getStatefulResourceProfile(evaluatedMetrics, summaries);
        LOG.info("Stateful resource profile -> " + statefulResourceProfile);
        summaries.forEach(
                (jobVertexID, scalingSummary) -> {
                    LOG.debug("Summary: {} -> {} ", jobVertexID, scalingSummary);
                    if (evaluatedMetrics.get(jobVertexID).containsKey(ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
                        scalingSummary.setNewResourceProfile(statefulResourceProfile);
                    } else {
                        scalingSummary.setNewResourceProfile(defaultResourceProfile);
                    }
                });
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    LOG.debug("Evaluated: {} -> {} ", id, metrics);
                    overrides.put(
                            id.toString(),
                            String.valueOf(summaries.get(id).getNewResourceProfile()));
                });
        return overrides;
    }

    protected static boolean allVerticesWithinUtilizationTarget(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {

        for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingSummaries.entrySet()) {
            var vertex = entry.getKey();
            var scalingSummary = entry.getValue();
            var metrics = evaluatedMetrics.get(vertex);

            double processingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
            double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
            double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

            if (processingRate < scaleUpRateThreshold || processingRate > scaleDownRateThreshold) {
                LOG.debug(
                        "Vertex {} processing rate {} is outside ({}, {})",
                        vertex,
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
                return false;
            } else {
                LOG.debug(
                        "Vertex {} processing rate {} is within target ({}, {})",
                        vertex,
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
            }
        }
        LOG.info("All vertex processing rates are within target.");
        return true;
    }


    private static ResourceProfile getStatefulResourceProfile(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var totalTasks = 0;
        var statefulTasks = 0;
        for (Map.Entry<JobVertexID, ScalingSummary> summary :
                summaries.entrySet()) {
            if (evaluatedMetrics.get(summary.getKey()).containsKey(ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
                statefulTasks += summary.getValue().getNewParallelism();
            }
            totalTasks += summary.getValue().getNewParallelism();
        }
        var tasksPerTM = 4;
        var rp = ResourceProfile.newBuilder()
                .setCpuCores(1.0)
                .setTaskHeapMemoryMB(364)
                .setTaskOffHeapMemoryMB(0)
                .setNetworkMemoryMB(85);
        var TMs = ((totalTasks - 1) / tasksPerTM) + 1;
        LOG.info("TMs -> " + TMs);
        while (statefulTasks % TMs != 0) {
            statefulTasks += 1;
        }

        return rp.setManagedMemoryMB((TMs * 343 * 4 / statefulTasks) - 1).build();
    }

    private static Map<String, String> getVertexParallelismOverrides(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    if (summaries.containsKey(id)) {
                        overrides.put(
                                id.toString(),
                                String.valueOf(summaries.get(id).getNewParallelism()));
                    } else {
                        overrides.put(
                                id.toString(),
                                String.valueOf(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent()));
                    }
                });
        return overrides;
    }

    public boolean scaleResource(
            AbstractFlinkResource<?, ?> resource,
            AutoScalerInfo scalingInformation,
            Configuration conf,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        var now = Instant.now();
        var scalingHistory = scalingInformation.getScalingHistory(now, conf);
        var scalingSummaries =
                computeScalingSummary(resource, conf, evaluatedMetrics, scalingHistory);

        var jobID = JobID.fromHexString(resource.getStatus().getJobStatus().getJobId());
        //var justinSummaries =
        //        computeJustinSummary(resource, conf, evaluatedMetrics, null);

        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return false;
        }

        if (allVerticesWithinUtilizationTarget(evaluatedMetrics, scalingSummaries)) {
            return false;
        }

        updateRecommendedParallelism(evaluatedMetrics, scalingSummaries);
        //updateRecommendedJustin(evaluatedMetrics, justinSummaries);

        var scalingEnabled = conf.get(SCALING_ENABLED);

        var scalingReport = scalingReport(scalingSummaries, scalingEnabled);
        eventRecorder.triggerEvent(
                resource,
                EventRecorder.Type.Normal,
                EventRecorder.Reason.ScalingReport,
                EventRecorder.Component.Operator,
                scalingReport,
                "ScalingExecutor");

        if (!scalingEnabled) {
            return false;
        }

        scalingInformation.addToScalingHistory(clock.instant(), scalingSummaries, conf);

        Policy scalingPolicy = Policy.getInstance(conf);
        ScalingOverrides scalingOverrides = scalingPolicy.scaleDecision(jobID, evaluatedMetrics, scalingSummaries, conf);
        scalingInformation.setCurrentOverrides(scalingOverrides.getParallelismOverrides());
        scalingInformation.setCurrentJustinOverrides(scalingOverrides.getResourceProfileOverrides());

        return true;
    }


    private Map<JobVertexID, ScalingSummary> computeScalingSummary(
            AbstractFlinkResource<?, ?> resource,
            Configuration conf,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {

        var out = new HashMap<JobVertexID, ScalingSummary>();
        var excludeVertexIdList = conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);
        evaluatedMetrics.forEach(
                (v, metrics) -> {
                    if (excludeVertexIdList.contains(v.toHexString())) {
                        LOG.debug(
                                "Vertex {} is part of `vertex.exclude.ids` config, Ignoring it for scaling",
                                v);
                        out.put(v,
                                new ScalingSummary(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent(),
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent(),
                                        metrics));
                    } else {
                        var currentParallelism =
                                (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();
                        var newParallelism =
                                jobVertexScaler.computeScaleTargetParallelism(
                                        resource,
                                        conf,
                                        v,
                                        metrics,
                                        scalingHistory.getOrDefault(
                                                v, Collections.emptySortedMap()));
                        // Donatien Schmitz: Remove the filter to give a scaling summary to each and every vertex
                            out.put(
                                    v,
                                    new ScalingSummary(
                                            currentParallelism, newParallelism, metrics));
                    }
                });

        return out;
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
        jobVertexScaler.setClock(clock);
    }
}
