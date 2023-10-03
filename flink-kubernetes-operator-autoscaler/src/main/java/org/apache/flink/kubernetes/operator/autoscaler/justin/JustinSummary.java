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

package org.apache.flink.kubernetes.operator.autoscaler.justin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.kubernetes.operator.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.Map;

/**
 * Scaling summary returned by the {@link ScalingMetricEvaluator}.
 */
@Data
@NoArgsConstructor
public class JustinSummary {

    private ResourceProfile currentResourceProfile;

    private ResourceProfile newResourceProfile;

    private Map<ScalingMetric, EvaluatedScalingMetric> metrics;

    public JustinSummary(
            ResourceProfile currentResourceProfile,
            ResourceProfile newResourceProfile,
            Map<ScalingMetric, EvaluatedScalingMetric> metrics) {
        if (currentResourceProfile.equals(newResourceProfile)) {
            throw new IllegalArgumentException(
                    "Current resource profile should not be equal to new resource profile during scaling.");
        }
        this.currentResourceProfile = currentResourceProfile;
        this.newResourceProfile = newResourceProfile;
        this.metrics = metrics;
    }

    @JsonIgnore
    public boolean isScaledUp() {
        return !newResourceProfile.equals(currentResourceProfile);
    }
}
