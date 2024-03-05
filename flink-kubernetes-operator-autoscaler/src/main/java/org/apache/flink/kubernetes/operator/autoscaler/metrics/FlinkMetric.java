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

import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Enum representing the collected Flink metrics for autoscaling. The actual metric names depend on
 * the JobGraph.
 */
public enum FlinkMetric {
    BUSY_TIME_PER_SEC(s -> s.equals("busyTimeMsPerSecond")),
    BACKPRESSURE_TIME_PER_SEC(s -> s.equals("busyTimeMsPerSecond")),
    IDLE_TIME_PER_SEC(s -> s.equals("busyTimeMsPerSecond")),
    NUM_RECORDS_IN_PER_SEC(s -> s.equals("numRecordsInPerSecond")),
    NUM_RECORDS_OUT_PER_SEC(s -> s.equals("numRecordsOutPerSecond")),
    SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC(
            s -> s.startsWith("Source__") && s.endsWith(".numRecordsOutPerSecond")),
    SOURCE_TASK_NUM_RECORDS_IN_PER_SEC(
            s -> s.startsWith("Source__") && s.endsWith(".numRecordsInPerSecond")),
    PENDING_RECORDS(s -> s.endsWith(".pendingRecords")),
    ROCKS_DB_BLOCK_CACHE_HIT(s -> s.endsWith(".rocksdb_block_cache_hit")),
    ROCKS_DB_BLOCK_CACHE_MISS(s -> s.endsWith(".rocksdb_block_cache_miss")),
    ROCKS_DB_BLOCK_CACHE_USAGE(s -> s.endsWith(".rocksdb_block-cache-usage")),
    ROCKS_DB_ESTIMATE_NUM_KEYS(s -> s.endsWith(".rocksdb_estimate-num-keys")),
    ROCKS_DB_LIVE_SST_FILES_SIZE(s -> s.endsWith(".rocksdb_live-sst-files-size")),
    // STATE GET LATENCY
    LIST_STATE_GET_MEAN_LATENCY(s -> s.endsWith(".listStateGetLatency_mean")),
    MAP_STATE_GET_MEAN_LATENCY(s -> s.endsWith(".mapStateGetLatency_mean")),
    VALUE_STATE_GET_MEAN_LATENCY(s -> s.endsWith(".valueStateGetLatency_mean")),
    AGGREGATE_STATE_GET_MEAN_LATENCY(s -> s.endsWith(".aggregateStateGetLatency_mean")),
    REDUCING_STATE_GET_MEAN_LATENCY(s -> s.endsWith(".reducingStateGetLatency_mean")),
    LIST_STATE_GET_COUNT(s -> false),
    MAP_STATE_GET_COUNT(s -> false),
    VALUE_STATE_GET_COUNT(s -> false),
    AGGREGATE_STATE_GET_COUNT(s -> false),
    REDUCING_STATE_GET_COUNT(s -> false),
    // STATE PUT LATENCY
    LIST_STATE_ADD_MEAN_LATENCY(s -> s.endsWith(".listStateAddLatency_mean")),
    MAP_STATE_PUT_MEAN_LATENCY(s -> s.endsWith(".mapStatPuttLatency_mean")),
    VALUE_STATE_UPDATE_MEAN_LATENCY(s -> s.endsWith(".valueStateUpdateLatency_mean")),
    AGGREGATE_STATE_ADD_MEAN_LATENCY(s -> s.endsWith(".aggregateStateAddLatency_mean")),
    REDUCING_STATE_ADD_MEAN_LATENCY(s -> s.endsWith(".reducingStateAddLatency_mean")),
    LIST_STATE_ADD_COUNT(s -> false),
    MAP_STATE_PUT_COUNT(s -> false),
    VALUE_STATE_UPDATE_COUNT(s -> false),
    AGGREGATE_STATE_ADD_COUNT(s -> false),
    REDUCING_STATE_ADD_COUNT(s -> false),
    ;

    public static final Map<FlinkMetric, AggregatedMetric> FINISHED_METRICS =
            Map.ofEntries(
                    Map.entry(FlinkMetric.BUSY_TIME_PER_SEC, zero()),
                    Map.entry(FlinkMetric.PENDING_RECORDS, zero()),
                    Map.entry(FlinkMetric.NUM_RECORDS_IN_PER_SEC, zero()),
                    Map.entry(FlinkMetric.NUM_RECORDS_OUT_PER_SEC, zero()),
                    Map.entry(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC, zero()),
                    Map.entry(FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC, zero())
            );

    public static final Set<FlinkMetric> JUSTIN_METRICS =
            Set.of(
                    FlinkMetric.ROCKS_DB_BLOCK_CACHE_HIT,
                    FlinkMetric.ROCKS_DB_BLOCK_CACHE_MISS,
                    FlinkMetric.ROCKS_DB_BLOCK_CACHE_USAGE,
                    FlinkMetric.ROCKS_DB_ESTIMATE_NUM_KEYS,
                    FlinkMetric.ROCKS_DB_LIVE_SST_FILES_SIZE,
                    FlinkMetric.LIST_STATE_GET_MEAN_LATENCY,
                    FlinkMetric.MAP_STATE_GET_MEAN_LATENCY,
                    FlinkMetric.VALUE_STATE_GET_MEAN_LATENCY,
                    FlinkMetric.AGGREGATE_STATE_GET_MEAN_LATENCY,
                    FlinkMetric.REDUCING_STATE_GET_MEAN_LATENCY,
                    FlinkMetric.LIST_STATE_ADD_MEAN_LATENCY,
                    FlinkMetric.MAP_STATE_PUT_MEAN_LATENCY,
                    FlinkMetric.VALUE_STATE_UPDATE_MEAN_LATENCY,
                    FlinkMetric.AGGREGATE_STATE_ADD_MEAN_LATENCY,
                    FlinkMetric.REDUCING_STATE_ADD_MEAN_LATENCY,
                    FlinkMetric.LIST_STATE_GET_COUNT,
                    FlinkMetric.MAP_STATE_GET_COUNT,
                    FlinkMetric.VALUE_STATE_GET_COUNT,
                    FlinkMetric.AGGREGATE_STATE_GET_COUNT,
                    FlinkMetric.REDUCING_STATE_GET_COUNT,
                    FlinkMetric.LIST_STATE_ADD_COUNT,
                    FlinkMetric.MAP_STATE_PUT_COUNT,
                    FlinkMetric.VALUE_STATE_UPDATE_COUNT,
                    FlinkMetric.AGGREGATE_STATE_ADD_COUNT,
                    FlinkMetric.REDUCING_STATE_ADD_COUNT);

    public final Predicate<String> predicate;

    FlinkMetric(Predicate<String> predicate) {
        this.predicate = predicate;
    }

    public Optional<String> findAny(Collection<AggregatedMetric> metrics) {
        return metrics.stream().map(AggregatedMetric::getId).filter(predicate).findAny();
    }

    public List<String> findAll(Collection<AggregatedMetric> metrics) {
        return metrics.stream()
                .map(AggregatedMetric::getId)
                .filter(predicate)
                .collect(Collectors.toList());
    }

    private static AggregatedMetric zero() {
        return new AggregatedMetric("", 0., 0., 0., 0.);
    }
}
