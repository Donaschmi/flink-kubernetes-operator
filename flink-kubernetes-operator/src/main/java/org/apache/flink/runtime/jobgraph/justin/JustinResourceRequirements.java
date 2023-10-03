package org.apache.flink.runtime.jobgraph.justin;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class JustinResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final JustinResourceRequirements EMPTY =
            new JustinResourceRequirements(Collections.emptyMap());
    private final Map<JobVertexID, JustinVertexResourceRequirements> vertexResources;

    public JustinResourceRequirements(
            Map<JobVertexID, JustinVertexResourceRequirements> vertexResources) {
        this.vertexResources =
                Collections.unmodifiableMap(new HashMap<>(checkNotNull(vertexResources)));
    }

    public static JustinResourceRequirements empty() {
        return JustinResourceRequirements.EMPTY;
    }

    public static JustinResourceRequirements.Builder newBuilder() {
        return new JustinResourceRequirements.Builder();
    }

    public JustinVertexResourceRequirements.Parallelism getParallelism(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(JustinVertexResourceRequirements::getParallelism)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));
    }

    public ResourceProfile getResourceProfile(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(JustinVertexResourceRequirements::getResourceProfile)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));
    }

    public Set<JobVertexID> getJobVertices() {
        return vertexResources.keySet();
    }

    public Map<JobVertexID, JustinVertexResourceRequirements> getJobVertexParallelisms() {
        return vertexResources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JustinResourceRequirements that = (JustinResourceRequirements) o;
        return Objects.equals(vertexResources, that.vertexResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexResources);
    }

    @Override
    public String toString() {
        return "JustinResourceRequirements{" + "vertexResources=" + vertexResources + '}';
    }

    public static final class Builder {

        private final Map<JobVertexID, JustinVertexResourceRequirements> vertexResources =
                new HashMap<>();

        public JustinResourceRequirements.Builder setParallelismForJobVertex(
                JobVertexID jobVertexId,
                int lowerBound,
                int upperBound,
                ResourceProfile resourceProfile) {
            vertexResources.put(
                    jobVertexId,
                    new JustinVertexResourceRequirements(
                            new JustinVertexResourceRequirements.Parallelism(
                                    lowerBound, upperBound),
                            resourceProfile));
            return this;
        }

        public JustinResourceRequirements build() {
            return new JustinResourceRequirements(vertexResources);
        }
    }
}
