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

package org.apache.flink.kubernetes.operator.autoscaler.metrics;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Utilities for computing scaling metrics based on Flink metrics. */
public class ScalingMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetrics.class);

    public static void computeLoadMetrics(
            JobVertexID jobVertexID,
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            Configuration conf) {

        double busyTimeMsPerSecond = getBusyTimeMsPerSecond(flinkMetrics, conf, jobVertexID);
        scalingMetrics.put(ScalingMetric.LOAD, busyTimeMsPerSecond / 1000);
    }

    public static void computeDataRateMetrics(
            JobVertexID jobVertexID,
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            JobTopology topology,
            double lagGrowthRate,
            Configuration conf) {

        var isSource = topology.isSource(jobVertexID);

        double busyTimeMsPerSecond = getBusyTimeMsPerSecond(flinkMetrics, conf, jobVertexID);
        double numRecordsInPerSecond =
                getNumRecordsInPerSecond(flinkMetrics, jobVertexID, isSource);

        if (isSource) {
            double sourceDataRate = Math.max(0, numRecordsInPerSecond + lagGrowthRate);
            LOG.debug("Using computed source data rate {} for {}", sourceDataRate, jobVertexID);
            scalingMetrics.put(ScalingMetric.SOURCE_DATA_RATE, sourceDataRate);
            scalingMetrics.put(ScalingMetric.CURRENT_PROCESSING_RATE, numRecordsInPerSecond);
        }

        if (!Double.isNaN(numRecordsInPerSecond)) {
            double trueProcessingRate = computeTrueRate(numRecordsInPerSecond, busyTimeMsPerSecond);
            scalingMetrics.put(ScalingMetric.TRUE_PROCESSING_RATE, trueProcessingRate);
            scalingMetrics.put(ScalingMetric.CURRENT_PROCESSING_RATE, numRecordsInPerSecond);
        } else {
            LOG.warn("Cannot compute true processing rate without numRecordsInPerSecond");
            scalingMetrics.put(ScalingMetric.TRUE_PROCESSING_RATE, Double.NaN);
            scalingMetrics.put(ScalingMetric.CURRENT_PROCESSING_RATE, Double.NaN);
        }
    }

    public static Map<Edge, Double> computeOutputRatios(
            Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> flinkMetrics,
            JobTopology topology) {

        var out = new HashMap<Edge, Double>();
        for (JobVertexID from : flinkMetrics.keySet()) {
            var outputs = topology.getOutputs().get(from);
            if (outputs.isEmpty()) {
                continue;
            }

            double numRecordsInPerSecond =
                    getNumRecordsInPerSecond(flinkMetrics.get(from), from, topology.isSource(from));

            for (JobVertexID to : outputs) {
                double outputRatio = 0;
                // If the input rate is zero, we also need to flatten the output rate.
                // Otherwise, the OUTPUT_RATIO would be outrageously large, leading to
                // a rapid scale up.
                if (numRecordsInPerSecond > 0) {
                    double edgeOutPerSecond =
                            computeEdgeOutPerSecond(topology, flinkMetrics, from, to);
                    if (edgeOutPerSecond > 0) {
                        outputRatio = edgeOutPerSecond / numRecordsInPerSecond;
                    }
                }
                out.put(new Edge(from, to), outputRatio);
            }
        }

        return out;
    }

    public static void computeLagMetrics(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {
        var pendingRecords = flinkMetrics.get(FlinkMetric.PENDING_RECORDS);
        if (pendingRecords != null) {
            scalingMetrics.put(ScalingMetric.LAG, pendingRecords.getSum());
        } else {
            scalingMetrics.put(ScalingMetric.LAG, 0.);
        }
    }

    public static void computeCacheRateMetrics(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        double cacheHits = flinkMetrics.get(FlinkMetric.ROCKS_DB_BLOCK_CACHE_HIT).getSum();
        double cacheMisses = flinkMetrics.get(FlinkMetric.ROCKS_DB_BLOCK_CACHE_MISS).getSum();

        double rate = 0.0;
        if (cacheHits != 0 && cacheMisses != 0) {
            rate = cacheHits / (cacheHits + cacheMisses);
        }
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT, cacheHits);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_MISS, cacheMisses);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE, rate);
    }

    public static void computeStateLatencyMetrics(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            Configuration conf) {
        var busyTimeAggregator = conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR);
        var busyTimeMsPerSecond =
                busyTimeAggregator.get(flinkMetrics.get(FlinkMetric.BUSY_TIME_PER_SEC));
        var numRecordsInPerSecond = scalingMetrics.get(ScalingMetric.CURRENT_PROCESSING_RATE);

        if (flinkMetrics.keySet().stream().anyMatch(flinkMetric -> flinkMetric.toString().contains("VALUE_STATE"))) {
            var meanGetLatency = getAverage(flinkMetrics, FlinkMetric.VALUE_STATE_GET_MEAN_LATENCY);
            var countGetLatency = getAverage(flinkMetrics, FlinkMetric.VALUE_STATE_GET_COUNT);
            var meanPutLatency = getAverage(flinkMetrics, FlinkMetric.VALUE_STATE_UPDATE_MEAN_LATENCY);
            var countPutLatency = getAverage(flinkMetrics, FlinkMetric.VALUE_STATE_UPDATE_COUNT);
            if (!Double.isNaN(countGetLatency) || !Double.isNaN(countPutLatency)) {
                scalingMetrics.put(ScalingMetric.VALUE_STATE_GET_MEAN_LATENCY, meanGetLatency);
                scalingMetrics.put(ScalingMetric.VALUE_STATE_GET_COUNT, countGetLatency);
                scalingMetrics.put(ScalingMetric.VALUE_STATE_UPDATE_MEAN_LATENCY, meanPutLatency);
                scalingMetrics.put(ScalingMetric.VALUE_STATE_UPDATE_COUNT, countPutLatency);
                var absoluteBusyTime = getAbsoluteBusyTime(meanGetLatency, countGetLatency, meanPutLatency, countPutLatency, numRecordsInPerSecond);
                scalingMetrics.put(ScalingMetric.ABSOLUTE_ROCKSDB_BUSY_TIME, absoluteBusyTime);
                scalingMetrics.put(ScalingMetric.RELATIVE_ROCKSDB_BUSY_TIME, getRelativeBusyTime(absoluteBusyTime, busyTimeMsPerSecond));
                return;
            }
        }
        if (flinkMetrics.keySet().stream().anyMatch(flinkMetric -> flinkMetric.toString().contains("LIST_STATE"))) {
            var meanGetLatency = getAverage(flinkMetrics, FlinkMetric.LIST_STATE_GET_MEAN_LATENCY);
            var countGetLatency = getAverage(flinkMetrics, FlinkMetric.LIST_STATE_GET_COUNT);
            var meanPutLatency = getAverage(flinkMetrics, FlinkMetric.LIST_STATE_ADD_MEAN_LATENCY);
            var countPutLatency = getAverage(flinkMetrics, FlinkMetric.LIST_STATE_ADD_COUNT);
            if (!Double.isNaN(countGetLatency) || !Double.isNaN(countPutLatency)) {
                scalingMetrics.put(ScalingMetric.LIST_STATE_GET_MEAN_LATENCY, meanGetLatency);
                scalingMetrics.put(ScalingMetric.LIST_STATE_GET_COUNT, countGetLatency);
                scalingMetrics.put(ScalingMetric.LIST_STATE_ADD_MEAN_LATENCY, meanPutLatency);
                scalingMetrics.put(ScalingMetric.LIST_STATE_ADD_COUNT, countPutLatency);
                var absoluteBusyTime = getAbsoluteBusyTime(meanGetLatency, countGetLatency, meanPutLatency, countPutLatency, numRecordsInPerSecond);
                scalingMetrics.put(ScalingMetric.ABSOLUTE_ROCKSDB_BUSY_TIME, absoluteBusyTime);
                scalingMetrics.put(ScalingMetric.RELATIVE_ROCKSDB_BUSY_TIME, getRelativeBusyTime(absoluteBusyTime, busyTimeMsPerSecond));
                return;
            }
        }
        if (flinkMetrics.keySet().stream().anyMatch(flinkMetric -> flinkMetric.toString().contains("MAP_STATE"))) {
            var meanGetLatency = getAverage(flinkMetrics, FlinkMetric.MAP_STATE_GET_MEAN_LATENCY);
            var countGetLatency = getAverage(flinkMetrics, FlinkMetric.MAP_STATE_GET_COUNT);
            var meanPutLatency = getAverage(flinkMetrics, FlinkMetric.MAP_STATE_PUT_MEAN_LATENCY);
            var countPutLatency = getAverage(flinkMetrics, FlinkMetric.MAP_STATE_PUT_COUNT);
            if (!Double.isNaN(countGetLatency) || !Double.isNaN(countPutLatency)) {
                scalingMetrics.put(ScalingMetric.MAP_STATE_GET_MEAN_LATENCY, meanGetLatency);
                scalingMetrics.put(ScalingMetric.MAP_STATE_GET_COUNT, countGetLatency);
                scalingMetrics.put(ScalingMetric.MAP_STATE_PUT_MEAN_LATENCY, meanPutLatency);
                scalingMetrics.put(ScalingMetric.MAP_STATE_PUT_COUNT, countPutLatency);
                var absoluteBusyTime = getAbsoluteBusyTime(meanGetLatency, countGetLatency, meanPutLatency, countPutLatency, numRecordsInPerSecond);
                scalingMetrics.put(ScalingMetric.ABSOLUTE_ROCKSDB_BUSY_TIME, absoluteBusyTime);
                scalingMetrics.put(ScalingMetric.RELATIVE_ROCKSDB_BUSY_TIME, getRelativeBusyTime(absoluteBusyTime, busyTimeMsPerSecond));
                return;
            }
        }
        if (flinkMetrics.keySet().stream().anyMatch(flinkMetric -> flinkMetric.toString().contains("AGGREGATE_STATE"))) {
            var meanGetLatency = getAverage(flinkMetrics, FlinkMetric.AGGREGATE_STATE_GET_MEAN_LATENCY);
            var countGetLatency = getAverage(flinkMetrics, FlinkMetric.AGGREGATE_STATE_GET_COUNT);
            var meanPutLatency = getAverage(flinkMetrics, FlinkMetric.AGGREGATE_STATE_ADD_MEAN_LATENCY);
            var countPutLatency = getAverage(flinkMetrics, FlinkMetric.AGGREGATE_STATE_ADD_COUNT);
            if (!Double.isNaN(countGetLatency) || !Double.isNaN(countPutLatency)) {
                scalingMetrics.put(ScalingMetric.AGGREGATE_STATE_GET_MEAN_LATENCY, meanGetLatency);
                scalingMetrics.put(ScalingMetric.AGGREGATE_STATE_GET_COUNT, countGetLatency);
                scalingMetrics.put(ScalingMetric.AGGREGATE_STATE_ADD_MEAN_LATENCY, meanPutLatency);
                scalingMetrics.put(ScalingMetric.AGGREGATE_STATE_ADD_COUNT, countPutLatency);
                var absoluteBusyTime = getAbsoluteBusyTime(meanGetLatency, countGetLatency, meanPutLatency, countPutLatency, numRecordsInPerSecond);
                scalingMetrics.put(ScalingMetric.ABSOLUTE_ROCKSDB_BUSY_TIME, absoluteBusyTime);
                scalingMetrics.put(ScalingMetric.RELATIVE_ROCKSDB_BUSY_TIME, getRelativeBusyTime(absoluteBusyTime, busyTimeMsPerSecond));
                return;
            }
        }
        if (flinkMetrics.keySet().stream().anyMatch(flinkMetric -> flinkMetric.toString().contains("REDUCING_STATE"))) {
            var meanGetLatency = getAverage(flinkMetrics, FlinkMetric.REDUCING_STATE_GET_MEAN_LATENCY);
            var countGetLatency = getAverage(flinkMetrics, FlinkMetric.REDUCING_STATE_GET_COUNT);
            var meanPutLatency = getAverage(flinkMetrics, FlinkMetric.REDUCING_STATE_ADD_MEAN_LATENCY);
            var countPutLatency = getAverage(flinkMetrics, FlinkMetric.REDUCING_STATE_ADD_COUNT);
            if (!Double.isNaN(countGetLatency) || !Double.isNaN(countPutLatency)) {
                scalingMetrics.put(ScalingMetric.REDUCING_STATE_GET_MEAN_LATENCY, meanGetLatency);
                scalingMetrics.put(ScalingMetric.AGGREGATE_STATE_GET_COUNT, countGetLatency);
                scalingMetrics.put(ScalingMetric.REDUCING_STATE_ADD_MEAN_LATENCY, meanPutLatency);
                scalingMetrics.put(ScalingMetric.REDUCING_STATE_ADD_COUNT, countPutLatency);
                var absoluteBusyTime = getAbsoluteBusyTime(meanGetLatency, countGetLatency, meanPutLatency, countPutLatency, numRecordsInPerSecond);
                scalingMetrics.put(ScalingMetric.ABSOLUTE_ROCKSDB_BUSY_TIME, absoluteBusyTime);
                scalingMetrics.put(ScalingMetric.RELATIVE_ROCKSDB_BUSY_TIME, getRelativeBusyTime(absoluteBusyTime, busyTimeMsPerSecond));
                return;
            }
        }
    }

    private static double getAbsoluteBusyTime(
            double meanGetLatency,
            double countGetLatency,
            double meanPutLatency,
            double countPutLatency,
            double numRecordsInPerSecond) {
        double totalGet = 0.0;
        double totalPut = 0.0;
        if (!Double.isNaN(countGetLatency)) {
            totalGet = meanGetLatency / 1000000 * numRecordsInPerSecond * (numRecordsInPerSecond / (countGetLatency * 100));
        }
        if (!Double.isNaN(countPutLatency)) {
            totalPut = meanPutLatency / 1000000 * numRecordsInPerSecond * (numRecordsInPerSecond / (countPutLatency * 100));
        }
        return totalGet + totalPut;
    }

    private static double getRelativeBusyTime(double absoluteBusyTime, double busyTimeMsPerSecond) {
        return absoluteBusyTime / busyTimeMsPerSecond;
    }

    private static Double getAverage(Map<FlinkMetric, AggregatedMetric> flinkMetrics, FlinkMetric flinkMetric) {
        var ret = flinkMetrics.get(flinkMetric);
        if (ret != null) {
            return ret.getAvg();
        }
        return Double.NaN;
    }

    private static double getBusyTimeMsPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Configuration conf,
            JobVertexID jobVertexId) {
        var busyTimeAggregator = conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR);
        var busyTimeMsPerSecond =
                busyTimeAggregator.get(flinkMetrics.get(FlinkMetric.BUSY_TIME_PER_SEC));
        if (!Double.isFinite(busyTimeMsPerSecond)) {
            if (AutoScalerUtils.excludeVertexFromScaling(conf, jobVertexId)) {
                // We only want to log this once
                LOG.warn(
                        "No busyTimeMsPerSecond metric available for {}. No scaling will be performed for this vertex.",
                        jobVertexId);
            }
            // Pretend that the load is balanced because we don't know any better
            busyTimeMsPerSecond = conf.get(AutoScalerOptions.TARGET_UTILIZATION) * 1000;
        }
        return busyTimeMsPerSecond;
    }

    private static double getNumRecordsInPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            JobVertexID jobVertexID,
            boolean isSource) {
        var numRecordsInPerSecond = flinkMetrics.get(FlinkMetric.NUM_RECORDS_IN_PER_SEC);
        if (isSource && (numRecordsInPerSecond == null || numRecordsInPerSecond.getSum() == 0)) {
            numRecordsInPerSecond =
                    flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC);
        }
        if (isSource && (numRecordsInPerSecond == null || numRecordsInPerSecond.getSum() == 0)) {
            numRecordsInPerSecond =
                    flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC);
        }
        if (numRecordsInPerSecond == null) {
            LOG.warn("Received null input rate for {}. Returning NaN.", jobVertexID);
            return Double.NaN;
        }
        return numRecordsInPerSecond.getSum();
    }

    private static double getNumRecordsOutPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics, JobVertexID jobVertexID) {

        AggregatedMetric numRecordsOutPerSecond =
                flinkMetrics.get(FlinkMetric.NUM_RECORDS_OUT_PER_SEC);

        if (numRecordsOutPerSecond == null) {
            LOG.warn("Received null output rate for {}. Returning NaN.", jobVertexID);
            return Double.NaN;
        }
        return numRecordsOutPerSecond.getSum();
    }

    private static double computeEdgeOutPerSecond(
            JobTopology topology,
            Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> flinkMetrics,
            JobVertexID from,
            JobVertexID to) {
        var toMetrics = flinkMetrics.get(to);

        var toVertexInputs = topology.getInputs().get(to);
        // Case 1: Downstream vertex has a single input (from) so we can use the most reliable num
        // records in
        if (toVertexInputs.size() == 1) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate for single input downstream task", from, to);
            return getNumRecordsInPerSecond(toMetrics, to, false);
        }

        // Case 2: Downstream vertex has only inputs from upstream vertices which don't have other
        // outputs
        double numRecordsOutFromUpstreamInputs = 0;
        for (JobVertexID input : toVertexInputs) {
            if (input.equals(from)) {
                // Exclude source edge because we only want to consider other input edges
                continue;
            }
            if (topology.getOutputs().get(input).size() == 1) {
                numRecordsOutFromUpstreamInputs +=
                        getNumRecordsOutPerSecond(flinkMetrics.get(input), input);
            } else {
                // Output vertex has multiple outputs, cannot use this information...
                numRecordsOutFromUpstreamInputs = Double.NaN;
                break;
            }
        }
        if (!Double.isNaN(numRecordsOutFromUpstreamInputs)) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate by subtracting upstream input rates",
                    from,
                    to);
            return getNumRecordsInPerSecond(toMetrics, to, false) - numRecordsOutFromUpstreamInputs;
        }
        var fromMetrics = flinkMetrics.get(from);

        // Case 3: We fall back simply to num records out, this is the least reliable
        LOG.debug(
                "Computing edge ({}, {}) data rate by falling back to from num records out",
                from,
                to);
        return getNumRecordsOutPerSecond(fromMetrics, from);
    }

    private static double computeTrueRate(double rate, double busyTimeMsPerSecond) {
        if (rate <= 0 || busyTimeMsPerSecond <= 0) {
            // Nothing is coming in, we assume infinite processing power
            // until we can sample the true processing rate (i.e. data flows).
            return Double.POSITIVE_INFINITY;
        }
        return rate / (busyTimeMsPerSecond / 1000);
    }

    public static double roundMetric(double value) {
        double rounded = Precision.round(value, 3);
        // Never round down to 0, return original value instead
        return rounded == 0 ? value : rounded;
    }
}
