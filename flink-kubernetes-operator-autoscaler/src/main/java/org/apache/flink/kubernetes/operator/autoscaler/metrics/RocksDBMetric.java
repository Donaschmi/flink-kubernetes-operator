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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Enum representing the collected Flink metrics for autoscaling. The actual metric names depend on
 * the JobGraph.
 */
public enum RocksDBMetric {
    ROCKS_DB_BLOCK_CACHE_HIT(s -> s.endsWith(".rocksdb_block-cache-hit")),
    ROCKS_DB_BLOCK_CACHE_MISS(s -> s.endsWith(".rocksdb_block-cache-miss")),
    ROCKS_DB_BLOCK_CACHE_USAGE(s -> s.endsWith(".rocksdb_block-cache-usage")),
    ROCKS_DB_ESTIMATE_NUM_KEYS(s -> s.endsWith(".rocksdb_estimate-num-keys")),
    ROCKS_DB_LIVE_SST_FILES_SIZE(s -> s.endsWith(".rocksdb_live-sst-files-size"));

    public static final Map<RocksDBMetric, AggregatedMetric> FINISHED_METRICS =
            Map.of(
                    RocksDBMetric.ROCKS_DB_BLOCK_CACHE_HIT, zero(),
                    RocksDBMetric.ROCKS_DB_BLOCK_CACHE_MISS, zero(),
                    RocksDBMetric.ROCKS_DB_BLOCK_CACHE_USAGE, zero(),
                    RocksDBMetric.ROCKS_DB_ESTIMATE_NUM_KEYS, zero(),
                    RocksDBMetric.ROCKS_DB_LIVE_SST_FILES_SIZE, zero());

    public final Predicate<String> predicate;

    RocksDBMetric(Predicate<String> predicate) {
        this.predicate = predicate;
    }

    private static AggregatedMetric zero() {
        return new AggregatedMetric("", 0., 0., 0., 0.);
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
}
