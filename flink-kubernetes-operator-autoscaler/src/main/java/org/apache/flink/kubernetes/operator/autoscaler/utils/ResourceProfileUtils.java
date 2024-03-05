package org.apache.flink.kubernetes.operator.autoscaler.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.runtime.jobgraph.justin.ResourceProfile;

public class ResourceProfileUtils {

    public static Tuple2<ResourceProfile.Builder, Integer> getDefaultResourceProfile(Configuration conf) {
        var memory = conf.getInteger(AutoScalerOptions.TASKMANAGER_MEMORY);
        if (memory == 2) {
            return Tuple2.of(ResourceProfile.newBuilder()
                            .setCpuCores(1.0)
                            .setTaskHeapMemoryMB(134)
                            .setTaskOffHeapMemoryMB(0)
                            .setManagedMemoryMB(0)
                            .setNetworkMemoryMB(39)
                    , 158);
        } else if (memory == 4) {
            return Tuple2.of(
                    ResourceProfile.newBuilder()
                            .setCpuCores(1.0)
                            .setTaskHeapMemoryMB(364)
                            .setTaskOffHeapMemoryMB(0)
                            .setManagedMemoryMB(0)
                            .setNetworkMemoryMB(85),
                    343);
        } else if (memory == 8) {
            return Tuple2.of(ResourceProfile.newBuilder()
                            .setCpuCores(1.0)
                            .setTaskHeapMemoryMB(825)
                            .setTaskOffHeapMemoryMB(0)
                            .setManagedMemoryMB(0)
                            .setNetworkMemoryMB(177),
                    550);
        } else {
            return Tuple2.of(null, null);
        }
    }
}
