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

package com.starrocks.http;

import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ProcWarehousesTest extends StarRocksHttpTestCase {

    private static final String HTTP_URI = "/api/show_proc";

    private void sendHttpAndValidateResponse(String path, String expectedBody) throws IOException {
        Request request = new Request.Builder().get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + HTTP_URI + "?path=" + path)
                .build();

        Response response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String body = response.body().string();
        Assert.assertEquals(200, response.code());
        Assert.assertEquals(expectedBody, body);
    }

    @Override
    public void doSetUp() {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentWarehouseMgr();
        warehouseManager.initDefaultWarehouse();
    }

    @Test
    public void testGetWarehousesInfoSharedNothing() throws IOException {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };

        // list all warehouses
        sendHttpAndValidateResponse("/warehouses",
                "[{\"Id\":\"0\",\"Warehouse\":\"default_warehouse\"," +
                        "\"State\":\"INITIALIZING\",\"ClusterCount\":\"1\"}]"
        );
        // list cluster in warehouse:0
        sendHttpAndValidateResponse("/warehouses/0",
                "[{\"ClusterId\":\"0\",\"WorkerGroupId\":\"0\"," +
                        "\"ComputeNodeIds\":\"[]\",\"Pending\":\"-1\",\"Running\":\"-1\"}]"
        );
        // list clusters in warehouse("default_warehouse")
        sendHttpAndValidateResponse("/warehouses/default_warehouse",
                "[{\"ClusterId\":\"0\",\"WorkerGroupId\":\"0\"," +
                        "\"ComputeNodeIds\":\"[]\",\"Pending\":\"-1\",\"Running\":\"-1\"}]"
        );
        sendHttpAndValidateResponse("/warehouses/11111", "[]");
        sendHttpAndValidateResponse("/warehouses/notexistwarehouse", "[]");
    }

    @Test
    public void testGetWarehousesInfoSharedData(@Mocked StarOSAgent agent) throws IOException {
        GlobalStateMgr.getCurrentState().setStarOSAgent(agent);
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) {
                return Arrays.asList(1L, 2L, 3L);
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        // list all warehouses
        sendHttpAndValidateResponse("/warehouses",
                "[{\"Id\":\"0\",\"Warehouse\":\"default_warehouse\"," +
                        "\"State\":\"INITIALIZING\",\"ClusterCount\":\"1\"}]"
        );
        // list cluster in warehouse:0
        sendHttpAndValidateResponse("/warehouses/0",
                "[{\"ClusterId\":\"0\",\"WorkerGroupId\":\"0\"," +
                        "\"ComputeNodeIds\":\"[1, 2, 3]\",\"Pending\":\"-1\",\"Running\":\"-1\"}]"
        );
        // list clusters in warehouse("default_warehouse")
        sendHttpAndValidateResponse("/warehouses/default_warehouse",
                "[{\"ClusterId\":\"0\",\"WorkerGroupId\":\"0\"," +
                        "\"ComputeNodeIds\":\"[1, 2, 3]\",\"Pending\":\"-1\",\"Running\":\"-1\"}]"
        );
        sendHttpAndValidateResponse("/warehouses/11111", "[]");
        sendHttpAndValidateResponse("/warehouses/notexistwarehouse", "[]");
    }
}
