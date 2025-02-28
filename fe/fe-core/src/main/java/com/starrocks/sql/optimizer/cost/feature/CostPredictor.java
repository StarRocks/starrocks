// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.cost.feature;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class CostPredictor {

    public abstract long predictMemoryBytes(ExecPlan plan);

    public static ServiceBasedCostPredictor getServiceBasedCostPredictor() {
        return ServiceBasedCostPredictor.getInstance();
    }

    /**
     * Use a remote HTTP service to predict the query cost
     */
    public static class ServiceBasedCostPredictor extends CostPredictor implements Closeable {

        public static final String PREDICT_URL = "/predict_csv";
        public static final String HEALTH_URL = "/health_check";
        private static final ServiceBasedCostPredictor INSTANCE = new ServiceBasedCostPredictor();
        private static final Logger LOG = LogManager.getLogger(ServiceBasedCostPredictor.class);

        private static final ScheduledExecutorService DAEMON;
        private static final Duration HEALTH_CHECK_INTERVAL = Duration.ofSeconds(30);
        private volatile int lastHealthCheckStatusCode = HttpStatus.SC_OK;

        static {
            DAEMON = Executors.newSingleThreadScheduledExecutor();
            DAEMON.scheduleAtFixedRate(
                    () -> ServiceBasedCostPredictor.getInstance().healthCheck(),
                    HEALTH_CHECK_INTERVAL.getSeconds(),
                    HEALTH_CHECK_INTERVAL.getSeconds(),
                    TimeUnit.SECONDS);
        }

        private final CloseableHttpClient httpClient = HttpClients.createDefault();

        /**
         * Return the singleton instance of predictor, which is thread-safe to be shared among threads
         */
        public static ServiceBasedCostPredictor getInstance() {
            return INSTANCE;
        }

        private ServiceBasedCostPredictor() {
        }

        @Override
        public long predictMemoryBytes(ExecPlan plan) {
            PlanFeatures planFeatures = FeatureExtractor.extractFeatures(plan.getPhysicalPlan());
            String header = PlanFeatures.featuresHeader();
            String featureString = planFeatures.toFeatureCsv();

            try {
                // Use Apache HttpClient to send the HTTP request
                HttpPost httpPost = new HttpPost(Config.query_cost_prediction_service_address + PREDICT_URL);

                // Encode the request in CSV format
                String csvData = header + "\n" + featureString;
                StringEntity entity = new StringEntity(csvData);
                entity.setContentType("text/csv");
                httpPost.setEntity(entity);

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    int status = response.getStatusLine().getStatusCode();
                    if (status == HttpStatus.SC_OK) {
                        HttpEntity responseEntity = response.getEntity();
                        String responseBody = EntityUtils.toString(responseEntity);
                        return (long) Double.parseDouble(responseBody);
                    } else {
                        // Handle the error
                        throw new IOException("Failed to predict memory bytes: HTTP error code " + status);
                    }
                }
            } catch (IOException e) {
                // Log the error or handle it appropriately
                throw new RuntimeException("Failed to predict memory bytes", e);
            }
        }

        @Override
        public void close() throws IOException {
            httpClient.close();
        }

        public boolean isAvailable() {
            return Config.enable_query_cost_prediction && lastHealthCheckStatusCode == HttpStatus.SC_OK;
        }

        @VisibleForTesting
        protected void doHealthCheck() {
            healthCheck();
        }

        /**
         * Do health check of the service
         */
        private void healthCheck() {
            if (!Config.enable_query_cost_prediction) {
                return;
            }
            String address = Config.query_cost_prediction_service_address + HEALTH_URL;
            HttpGet httpGet = new HttpGet(address);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                lastHealthCheckStatusCode = response.getStatusLine().getStatusCode();
                if (lastHealthCheckStatusCode != HttpStatus.SC_OK) {
                    LOG.warn("service is not healthy, address={} status_code={}", address, lastHealthCheckStatusCode);
                }
            } catch (Throwable e) {
                lastHealthCheckStatusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
                LOG.warn("service is not healthy, address={}", address, e);
            }
        }
    }
}
