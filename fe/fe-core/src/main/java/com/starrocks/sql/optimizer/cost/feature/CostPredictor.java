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

import com.starrocks.common.Config;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;

public interface CostPredictor {

    long predictMemoryBytes(ExecPlan plan);

    /**
     * Use a remote HTTP service to predict the query cost
     */
    class ServiceBasedCostPredictor implements CostPredictor, Closeable {

        private static final String memCostUrl = "/predict_csv";
        private static final ServiceBasedCostPredictor INSTANCE = new ServiceBasedCostPredictor();

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
                HttpPost httpPost = new HttpPost(Config.query_cost_prediction_service_address + memCostUrl);

                // Encode the request in CSV format
                String csvData = header + "\n" + featureString;
                StringEntity entity = new StringEntity(csvData);
                entity.setContentType("text/csv");
                httpPost.setEntity(entity);

                CloseableHttpResponse response = httpClient.execute(httpPost);
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    HttpEntity responseEntity = response.getEntity();
                    String responseBody = EntityUtils.toString(responseEntity);
                    return (long) Double.parseDouble(responseBody);
                } else {
                    // Handle the error
                    throw new IOException("Failed to predict memory bytes: HTTP error code " + status);
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
    }
}
