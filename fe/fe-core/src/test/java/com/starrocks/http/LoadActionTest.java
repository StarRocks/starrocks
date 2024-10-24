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

import com.fasterxml.jackson.core.type.TypeReference;
import com.starrocks.load.batchwrite.BatchWriteMgr;
import com.starrocks.load.batchwrite.RequestCoordinatorBackendResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LoadActionTest extends StarRocksHttpTestCase {

    private OkHttpClient noRedirectClient = new OkHttpClient.Builder()
            .readTimeout(100, TimeUnit.SECONDS)
            .followRedirects(false)
            .build();

    @Test
    public void testBatchWriteStreamLoadSuccess() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(HTTP_ENABLE_BATCH_WRITE, "true");
        Request request = buildRequest(map);
        List<ComputeNode> computeNodes = new ArrayList<>();
        List<String> redirectLocations = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            String host = "192.0.0." + i;
            int httpPort = 8040;
            computeNodes.add(new ComputeNode(i, host, 9050));
            computeNodes.get(i - 1).setHttpPort(httpPort);
            redirectLocations.add(getLoadUrl(host, httpPort));
        }

        new MockUp<BatchWriteMgr>() {
            @Mock
            public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
                return new RequestCoordinatorBackendResult(new TStatus(TStatusCode.OK), computeNodes);
            }
        };

        try (Response response = noRedirectClient.newCall(request).execute()) {
            assertEquals(307, response.code());
            String location = response.header("Location");
            assertTrue(redirectLocations.contains(location));
        }
    }

    @Test
    public void testBatchWriteStreamLoadFailure() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(HTTP_ENABLE_BATCH_WRITE, "true");
        Request request = buildRequest(map);

        new MockUp<BatchWriteMgr>() {
            @Mock
            public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
                TStatus status = new TStatus();
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.addToError_msgs("artificial failure");
                return new RequestCoordinatorBackendResult(status, null);
            }
        };

        try (Response response = noRedirectClient.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("INTERNAL_ERROR", result.get("code"));
            assertEquals("FAILED", result.get("status"));
            assertEquals("artificial failure", result.get("message"));
            assertEquals("artificial failure", result.get("msg"));
        }
    }

    private Request buildRequest(Map<String, String> headers) {
        Request.Builder builder = new Request.Builder();
        builder.addHeader("Authorization", rootAuth);
        builder.addHeader("Expect", "100-continue");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.addHeader(entry.getKey(), entry.getValue());
        }
        builder.put(RequestBody.create(new byte[0]));
        builder.url(String.format("%s/api/%s/%s/_stream_load", BASE_URL, DB_NAME, TABLE_NAME));
        return builder.build();
    }

    private String getLoadUrl(String host, int port) {
        return String.format("http://%s:%d/api/%s/%s/_stream_load", host, port, DB_NAME, TABLE_NAME);
    }

    private static Map<String, Object> parseResponseBody(Response response) throws IOException {
        ResponseBody body = response.body();
        assertNotNull(body);
        String bodyStr = body.string();
        return objectMapper.readValue(bodyStr, new TypeReference<>() {});
    }
}
