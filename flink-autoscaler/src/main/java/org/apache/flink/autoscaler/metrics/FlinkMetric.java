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

package org.apache.flink.autoscaler.metrics;

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
    SOURCE_TASK_NUM_RECORDS_IN_PER_SEC(
            s -> s.startsWith("Source__") && s.endsWith(".numRecordsInPerSecond")),
    SOURCE_TASK_NUM_RECORDS_OUT(s -> s.startsWith("Source__") && s.endsWith(".numRecordsOut")),
    SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC(
            s -> s.startsWith("Source__") && s.endsWith(".numRecordsOutPerSecond")),
    SOURCE_TASK_NUM_RECORDS_IN(s -> s.startsWith("Source__") && s.endsWith(".numRecordsIn")),
    PENDING_RECORDS(s -> s.endsWith(".pendingRecords")),
    BACKPRESSURE_TIME_PER_SEC(s -> s.equals("backPressuredTimeMsPerSecond")),

    ROCKS_DB_BLOCK_CACHE_HIT(s -> s.endsWith(".rocksdb_block_cache_hit")),
    ROCKS_DB_BLOCK_CACHE_MISS(s -> s.endsWith(".rocksdb_block_cache_miss")),
    ROCKS_DB_BLOCK_CACHE_USAGE(s -> s.endsWith(".rocksdb_block-cache-usage")),
    ROCKS_DB_ESTIMATE_NUM_KEYS(s -> s.endsWith(".rocksdb_estimate-num-keys")),
    ROCKS_DB_LIVE_SST_FILES_SIZE(s -> s.endsWith(".rocksdb_live-sst-files-size")),

    HEAP_MEMORY_MAX(s -> s.equals("Status.JVM.Memory.Heap.Max")),
    HEAP_MEMORY_USED(s -> s.equals("Status.JVM.Memory.Heap.Used")),
    MANAGED_MEMORY_USED(s -> s.equals("Status.Flink.Memory.Managed.Used")),
    METASPACE_MEMORY_USED(s -> s.equals("Status.JVM.Memory.Metaspace.Used")),

    TOTAL_GC_TIME_PER_SEC(s -> s.equals("Status.JVM.GarbageCollector.All.TimeMsPerSecond")),

    NUM_TASK_SLOTS_TOTAL(s -> s.equals("taskSlotsTotal")),
    NUM_TASK_SLOTS_AVAILABLE(s -> s.equals("taskSlotsAvailable")),


    LIST_STATE_GET_MEAN_LATENCY(s -> s.endsWith(".listStateGetLatency_p90")),;

    public static final Map<FlinkMetric, AggregatedMetric> FINISHED_METRICS =
            Map.of(
                    FlinkMetric.BUSY_TIME_PER_SEC, zero(),
                    FlinkMetric.PENDING_RECORDS, zero(),
                    FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC, zero(),
                    FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN, zero(),
                    FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT, zero());

    public static final Set<FlinkMetric> JUSTIN_METRICS =
            Set.of(
                    FlinkMetric.ROCKS_DB_BLOCK_CACHE_HIT,
                    FlinkMetric.ROCKS_DB_BLOCK_CACHE_MISS,
                    FlinkMetric.ROCKS_DB_BLOCK_CACHE_USAGE,
                    FlinkMetric.ROCKS_DB_ESTIMATE_NUM_KEYS,
                    FlinkMetric.ROCKS_DB_LIVE_SST_FILES_SIZE,
                    FlinkMetric.LIST_STATE_GET_MEAN_LATENCY);

    public final Predicate<String> predicate;

    FlinkMetric(Predicate<String> predicate) {
        this.predicate = predicate;
    }

    public Optional<String> findAny(Collection<String> metrics) {
        return metrics.stream().filter(predicate).findAny();
    }

    public List<String> findAll(Collection<String> metrics) {
        return metrics.stream().filter(predicate).collect(Collectors.toList());
    }

    private static AggregatedMetric zero() {
        return new AggregatedMetric("", 0., 0., 0., 0., 0.);
    }
}
