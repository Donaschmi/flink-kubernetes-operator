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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Metric collector using flink rest api.
 */
public class PrometheusMetricsCollector {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsCollector.class);

    private static final Map<FlinkMetric, String> queries = Map.of(
            FlinkMetric.VALUE_STATE_GET_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_valueStateGetLatency_count[30s])",
            FlinkMetric.LIST_STATE_GET_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_listStateGetLatency_count[30s])",
            FlinkMetric.MAP_STATE_GET_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_mapStateGetLatency_count[30s])",
            FlinkMetric.AGGREGATE_STATE_GET_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_aggregatingStateGetLatency_count[30s])",
            FlinkMetric.REDUCING_STATE_GET_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_reducingStateGetLatency_count[30s])",
            FlinkMetric.VALUE_STATE_UPDATE_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_valueStateUpdateLatency_count[30s])",
            FlinkMetric.LIST_STATE_ADD_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_listStateAddLatency_count[30s])",
            FlinkMetric.MAP_STATE_PUT_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_mapStatePutLatency_count[30s])",
            FlinkMetric.AGGREGATE_STATE_ADD_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_aggregatingStateAddLatency_count[30s])",
            FlinkMetric.REDUCING_STATE_ADD_COUNT,
            "rate(flink_taskmanager_job_task_operator_state_name_reducingStateAddLatency_count[30s])"
    );

    protected static Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> queryAllAggregatedMetrics(
            Configuration conf,
            Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames) {

        return filteredVertexMetricNames.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                        queryAggregatedVertexMetrics(conf, e.getKey())));
    }

    @SneakyThrows
    protected static Map<FlinkMetric, AggregatedMetric> queryAggregatedVertexMetrics(
            Configuration conf,
            JobVertexID jobVertexID) {
        Map<FlinkMetric, AggregatedMetric> ret = new HashMap<>();
        for (FlinkMetric metric : queries.keySet()) {
            ret.put(metric, getPromMetric(conf, jobVertexID, metric));
        }
        return ret;
    }

    private static AggregatedMetric getPromMetric(Configuration conf, JobVertexID jobVertexID, FlinkMetric flinkMetric) {
        try {
            URL url = new URL(conf.getString(KubernetesOperatorConfigOptions.PROM_ENDPOINT));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            Map<String, String> parameters = new HashMap<>();
            parameters.put("query", queries.get(flinkMetric));

            con.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            out.writeBytes(ParameterStringBuilder.getParamsString(parameters));
            out.flush();
            out.close();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            var mapping = new ObjectMapper().readValue(content.toString(), HashMap.class);
            var result = (ArrayList) ((HashMap) mapping.get("data")).get("result");
            if (result.isEmpty()) {
                return new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, Double.NaN);
            }
            var doubleStream = (ArrayList) result.stream().filter(
                    r -> ((HashMap) ((HashMap) r).get("metric")).get("operator_id").toString().compareTo(jobVertexID.toString()) == 0
            ).map(
                    r -> Double.parseDouble(((ArrayList) ((HashMap) r).get("value")).get(1).toString())
            ).collect(Collectors.toList());
            if (doubleStream.isEmpty()) {
                return new AggregatedMetric(flinkMetric.name(), Double.NaN, Double.NaN, Double.NaN, Double.NaN);
            }
            Supplier<Stream<Object>> supplier = () -> Stream.of(doubleStream.toArray());
            in.close();
            con.disconnect();
            return new AggregatedMetric(
                    flinkMetric.name(),
                    supplier.get().mapToDouble(a -> new Double(a.toString())).min().getAsDouble(),
                    supplier.get().mapToDouble(a -> new Double(a.toString())).max().getAsDouble(),
                    supplier.get().mapToDouble(a -> new Double(a.toString())).average().getAsDouble(),
                    supplier.get().mapToDouble(a -> new Double(a.toString())).sum());
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return new AggregatedMetric(flinkMetric.name(), Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }
    }

    public static class ParameterStringBuilder {
        public static String getParamsString(Map<String, String> params)
                throws UnsupportedEncodingException {
            StringBuilder result = new StringBuilder();

            for (Map.Entry<String, String> entry : params.entrySet()) {
                result.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
                result.append("=");
                result.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
                result.append("&");
            }

            String resultString = result.toString();
            return resultString.length() > 0
                    ? resultString.substring(0, resultString.length() - 1)
                    : resultString;
        }
    }
}
