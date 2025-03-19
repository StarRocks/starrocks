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
import com.starrocks.common.Pair;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;

public class CostPredictorTest extends PlanTestBase {

    @Test
    public void testServiceBasedPredictor() throws Exception {
        String sql = "select count(*) from t0 where v1 < 100 limit 100 ";
        Pair<String, ExecPlan> planAndFragment = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        var instance = CostPredictor.ServiceBasedCostPredictor.getInstance();

        Assertions.assertThrows(RuntimeException.class, () -> instance.predictMemoryBytes(planAndFragment.second));

        // Create a simple HTTP server to mock the prediction service
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        final long predictedResult = 10000L;
        server.createContext(CostPredictor.ServiceBasedCostPredictor.PREDICT_URL, exchange -> {
            String response = String.valueOf(predictedResult);
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });
        server.createContext(CostPredictor.ServiceBasedCostPredictor.HEALTH_URL, exchange -> {
            exchange.sendResponseHeaders(200, 0);
        });
        server.start();
        Config.query_cost_prediction_service_address = "http://localhost:" + port;

        // Running the test case predictMemoryBytes again with the mocked service
        // This time, the test should not throw an exception
        Assertions.assertEquals(predictedResult, instance.predictMemoryBytes(planAndFragment.second));

        // test health check
        Config.enable_query_cost_prediction = true;
        instance.doHealthCheck();
        Assertions.assertTrue(instance.isAvailable());

        server.removeContext(CostPredictor.ServiceBasedCostPredictor.HEALTH_URL);
        instance.doHealthCheck();
        Assertions.assertFalse(instance.isAvailable());

        server.stop(0);
    }
}