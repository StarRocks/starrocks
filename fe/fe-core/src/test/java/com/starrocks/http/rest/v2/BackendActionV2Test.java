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

package com.starrocks.http.rest.v2;

import com.google.gson.reflect.TypeToken;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.http.rest.v2.vo.BackendInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

public class BackendActionV2Test extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/api/v2/backend";

    @Test
    public void testGetBackend() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        RestBaseResultV2<List<BackendInfo>> resp = parseResponseBody(respStr);
        List<BackendInfo> backendInfos = resp.getResult();
        Assertions.assertEquals(200, response.code());
        Assertions.assertEquals(3, backendInfos.size());
        Assertions.assertEquals(1000, backendInfos.get(0).getId());
        Assertions.assertEquals(1001, backendInfos.get(1).getId());
        Assertions.assertEquals(1002, backendInfos.get(2).getId());
    }

    @Test
    public void testGetComputeNode() throws IOException {
        ComputeNode cn1 = new ComputeNode(10001, "host1", 1003);
        cn1.setAlive(true);
        ComputeNode cn2 = new ComputeNode(10002, "host2", 1004);
        cn2.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn1);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn2);

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?is_request_compute_node=true")
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        RestBaseResultV2<List<BackendInfo>> resp = parseResponseBody(respStr);
        List<BackendInfo> backendInfos = resp.getResult();
        Assertions.assertEquals(200, response.code());
        Assertions.assertEquals(2, backendInfos.size());
        Assertions.assertEquals(10001, backendInfos.get(0).getId());
        Assertions.assertTrue(backendInfos.get(0).isAlive());
        Assertions.assertEquals(10002, backendInfos.get(1).getId());
        Assertions.assertTrue(backendInfos.get(1).isAlive());
    }

    private static RestBaseResultV2<List<BackendInfo>> parseResponseBody(String body) {
        try {
            return GsonUtils.GSON.fromJson(
                    body,
                    new TypeToken<RestBaseResultV2<List<BackendInfo>>>() {
                    }.getType());
        } catch (Exception e) {
            fail(e.getMessage() + ", resp: " + body);
            throw new IllegalStateException(e);
        }
    }
}
