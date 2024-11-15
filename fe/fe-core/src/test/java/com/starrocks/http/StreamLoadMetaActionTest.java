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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.starrocks.load.batchwrite.BatchWriteMgr;
import com.starrocks.load.batchwrite.RequestCoordinatorBackendResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE;
import static org.junit.Assert.assertEquals;

public class StreamLoadMetaActionTest extends StarRocksHttpTestCase {

    private final OkHttpClient client = new OkHttpClient.Builder()
            .readTimeout(1000, TimeUnit.SECONDS)
            .followRedirects(false)
            .build();


    @Test
    public void testNormalStreamLoad() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("format", "csv");
        Request request = buildRequest(headers, HashMultimap.create());
        try (Response response = client.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals(4, result.size());
            assertEquals("OK", result.get("status"));
            assertEquals("OK", result.get("code"));
            assertEquals("", result.get("msg"));
            assertEquals("", result.get("message"));
        }
    }

    @Test
    public void testBatchWriteRequireOnlyHttpNodes() throws Exception {
        testBatchWriteRequireNodesBase(Collections.singletonList("http"));
    }

    @Test
    public void testBatchWriteRequireOnlyBrpcNodes() throws Exception {
        testBatchWriteRequireNodesBase(Collections.singletonList("brpc"));
    }

    @Test
    public void testBatchWriteRequireAllTypeNodes() throws Exception {
        testBatchWriteRequireNodesBase(List.of("http", "brpc"));
        testBatchWriteRequireNodesBase(Collections.emptyList());
    }

    private void testBatchWriteRequireNodesBase(List<String> serviceTypes) throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP_ENABLE_BATCH_WRITE, "true");
        Multimap<String, String> parameters = HashMultimap.create();
        parameters.put("type", "nodes");
        for (String serviceType : serviceTypes) {
            parameters.put("node_service", serviceType);
        }
        Request request = buildRequest(headers, parameters);
        List<ComputeNode> computeNodes = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            String host = "192.0.0." + i;
            computeNodes.add(new ComputeNode(i, host, 9050));
            computeNodes.get(i - 1).setHttpPort(8040);
            computeNodes.get(i - 1).setBrpcPort(8060);
        }
        Map<String, String> expectedNodes = new HashMap<>();
        if (serviceTypes.isEmpty() || serviceTypes.contains("http")) {
            expectedNodes.put("http", "192.0.0.1:8040,192.0.0.2:8040,192.0.0.3:8040");
        }
        if (serviceTypes.isEmpty() || serviceTypes.contains("brpc")) {
            expectedNodes.put("brpc", "192.0.0.1:8060,192.0.0.2:8060,192.0.0.3:8060");
        }

        new MockUp<BatchWriteMgr>() {
            @Mock
            public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
                return new RequestCoordinatorBackendResult(new TStatus(TStatusCode.OK), computeNodes);
            }
        };

        try (Response response = client.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals(5, result.size());
            assertEquals("OK", result.get("status"));
            assertEquals("OK", result.get("code"));
            assertEquals("", result.get("msg"));
            assertEquals("", result.get("message"));
            assertEquals(expectedNodes, result.get("nodes"));
        }
    }

    @Test
    public void testBatchWriteRequireAllMetas() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP_ENABLE_BATCH_WRITE, "true");
        Request request = buildRequest(headers, HashMultimap.create());
        new MockUp<BatchWriteMgr>() {
            @Mock
            public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
                ComputeNode node = new ComputeNode(1, "192.0.0.1", 9050);
                node.setHttpPort(8040);
                node.setBrpcPort(8060);
                return new RequestCoordinatorBackendResult(new TStatus(TStatusCode.OK), Collections.singletonList(node));
            }
        };

        Map<String, String> expectedNodes = new HashMap<>();
        expectedNodes.put("http", "192.0.0.1:8040");
        expectedNodes.put("brpc", "192.0.0.1:8060");

        try (Response response = client.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals(5, result.size());
            assertEquals("OK", result.get("status"));
            assertEquals("OK", result.get("code"));
            assertEquals("", result.get("msg"));
            assertEquals("", result.get("message"));
            assertEquals(expectedNodes, result.get("nodes"));
        }
    }

    @Test
    public void testBatchWriteFail() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP_ENABLE_BATCH_WRITE, "true");
        Request request = buildRequest(headers, HashMultimap.create());

        new MockUp<BatchWriteMgr>() {
            @Mock
            public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
                TStatus status = new TStatus();
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.addToError_msgs("artificial failure");
                return new RequestCoordinatorBackendResult(status, null);
            }
        };
        try (Response response = client.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("INTERNAL_ERROR", result.get("code"));
            assertEquals("FAILED", result.get("status"));
            assertEquals("artificial failure", result.get("message"));
            assertEquals("artificial failure", result.get("msg"));
        }
    }

    @Test
    public void testDatabaseAndTableInvalid() throws Exception {
        Request emptyDbRequest = new Request.Builder()
                .addHeader("Authorization", rootAuth)
                .addHeader("format", "csv")
                .url(String.format(
                        "http://localhost:%s/api/%s/%s/_stream_load_meta",
                        HTTP_PORT, "", TABLE_NAME))
                .build();
        try (Response response = client.newCall(emptyDbRequest).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("INVALID_ARGUMENT", result.get("code"));
            assertEquals("FAILED", result.get("status"));
            assertEquals("No database selected", result.get("message"));
            assertEquals("No database selected", result.get("msg"));
        }

        Request emptyTableRequest = new Request.Builder()
                .addHeader("Authorization", rootAuth)
                .addHeader("format", "csv")
                .url(String.format(
                        "http://localhost:%s/api/%s/%s/_stream_load_meta",
                        HTTP_PORT, DB_NAME, ""))
                .build();
        try (Response response = client.newCall(emptyTableRequest).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("INVALID_ARGUMENT", result.get("code"));
            assertEquals("FAILED", result.get("status"));
            assertEquals("No table selected", result.get("message"));
            assertEquals("No table selected", result.get("msg"));
        }
    }

    private Request buildRequest(Map<String, String> headers, Multimap<String, String> parameters) {
        Request.Builder builder = new Request.Builder();
        builder.addHeader("Authorization", rootAuth);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.addHeader(entry.getKey(), entry.getValue());
        }
        HttpUrl.Builder urlBuilder = new HttpUrl.Builder()
                .scheme("http")
                .host("localhost")
                .port(HTTP_PORT)
                .addPathSegment("api")
                .addPathSegment(DB_NAME)
                .addPathSegment(TABLE_NAME)
                .addPathSegment("_stream_load_meta");
        parameters.forEach(urlBuilder::addQueryParameter);
        builder.url(urlBuilder.build());
        return builder.build();
    }
}
