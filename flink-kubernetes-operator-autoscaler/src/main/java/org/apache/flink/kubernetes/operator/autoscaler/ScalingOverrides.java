package org.apache.flink.kubernetes.operator.autoscaler;

import java.util.HashMap;
import java.util.Map;

public class ScalingOverrides {

    private final Map<String, String> parallelismOverrides;

    private final Map<String, String> resourceProfileOverrides;

    public ScalingOverrides() {
        this.parallelismOverrides = new HashMap<>();
        this.resourceProfileOverrides = new HashMap<>();
    }

    public void putParallelismOverride(String id, String override) {
        parallelismOverrides.put(id, override);
    }

    public void putResourceProfileOverride(String id, String override) {
        resourceProfileOverrides.put(id, override);
    }


    public Map<String, String> getParallelismOverrides() {
        return parallelismOverrides;
    }

    public Map<String, String> getResourceProfileOverrides() {
        return resourceProfileOverrides;
    }
}
