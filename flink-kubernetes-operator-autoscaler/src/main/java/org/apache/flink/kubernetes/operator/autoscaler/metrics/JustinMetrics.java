package org.apache.flink.kubernetes.operator.autoscaler.metrics;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JustinMetrics extends ScalingMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(JustinMetrics.class);


    public static void computeBlockCacheHitRate(
            JobVertexID jobVertexID,
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        double blockCacheHit = getBlockCacheHit(rocksDBMetrics, jobVertexID);
        double blockCacheMiss = getBlockCacheMiss(rocksDBMetrics, jobVertexID);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE, blockCacheHit / (blockCacheHit + blockCacheMiss));
    }

    public static void computeBlockCacheHit(
            JobVertexID jobVertexID,
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        double blockCacheHit = getBlockCacheHit(rocksDBMetrics, jobVertexID);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT, blockCacheHit);
    }

    public static void computeBlockCacheMiss(
            JobVertexID jobVertexID,
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        double blockCacheMiss = getBlockCacheMiss(rocksDBMetrics, jobVertexID);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_MISS, blockCacheMiss);
    }

    public static void computeBlockCacheUsage(
            JobVertexID jobVertexID,
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        double blockCacheUsage = getBlockCacheUsage(rocksDBMetrics, jobVertexID);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_BLOCK_CACHE_USAGE, blockCacheUsage);
    }


    public static void computeLiveSSTFilesSize(
            JobVertexID jobVertexID,
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        double liveSSTFilesSize = getLiveSSTFilesSize(rocksDBMetrics, jobVertexID);
        scalingMetrics.put(ScalingMetric.ROCKS_DB_LIVE_SST_FILES_SIZE, liveSSTFilesSize);
    }

    private static double getBlockCacheHit(
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetricAggregatedMetricMap,
            JobVertexID jobVertexId) {
        var blockCacheHit = rocksDBMetricAggregatedMetricMap.get(RocksDBMetric.ROCKS_DB_BLOCK_CACHE_HIT);
        if (blockCacheHit == null) {
            LOG.warn("Received null output rate for {}. Returning NaN.", jobVertexId);
            return Double.NaN;
        }
        return blockCacheHit.getSum();
    }

    private static double getBlockCacheMiss(
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetricAggregatedMetricMap,
            JobVertexID jobVertexId) {
        var blockCacheMiss = rocksDBMetricAggregatedMetricMap.get(RocksDBMetric.ROCKS_DB_BLOCK_CACHE_MISS);
        if (blockCacheMiss == null) {
            LOG.warn("Received null output rate for {}. Returning NaN.", jobVertexId);
            return Double.NaN;
        }
        return blockCacheMiss.getSum();
    }

    private static double getBlockCacheUsage(
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetricAggregatedMetricMap,
            JobVertexID jobVertexId) {
        var blockCacheUsage = rocksDBMetricAggregatedMetricMap.get(RocksDBMetric.ROCKS_DB_BLOCK_CACHE_USAGE);
        if (blockCacheUsage == null) {
            LOG.warn("Received null output rate for {}. Returning NaN.", jobVertexId);
            return Double.NaN;
        }
        return blockCacheUsage.getSum();
    }

    private static double getLiveSSTFilesSize(
            Map<RocksDBMetric, AggregatedMetric> rocksDBMetricAggregatedMetricMap,
            JobVertexID jobVertexId) {
        var liveSSTFilesSize = rocksDBMetricAggregatedMetricMap.get(RocksDBMetric.ROCKS_DB_LIVE_SST_FILES_SIZE);
        if (liveSSTFilesSize == null) {
            LOG.warn("Received null output rate for {}. Returning NaN.", jobVertexId);
            return Double.NaN;
        }
        return liveSSTFilesSize.getSum();
    }
}
