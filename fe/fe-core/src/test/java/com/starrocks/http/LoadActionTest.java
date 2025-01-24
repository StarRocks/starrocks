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
import com.google.common.collect.Multimap;
import com.starrocks.load.batchwrite.BatchWriteMgr;
import com.starrocks.load.batchwrite.RequestCoordinatorBackendResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.HttpHeaders;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE;
import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    private HttpPut buildPutRequest(int bodyLength, boolean setExpectHeader) {
        HttpPut put = new HttpPut(String.format("%s/api/%s/%s/_stream_load", BASE_URL, DB_NAME, TABLE_NAME));
        if (setExpectHeader) {
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
        }
        put.setHeader(HttpHeaders.AUTHORIZATION, rootAuth);
        StringEntity entity = new StringEntity(Arrays.toString(new byte[bodyLength]), "UTF-8");
        put.setEntity(entity);
        return put;
    }

    private String getLoadUrl(String host, int port) {
        return String.format("http://%s:%d/api/%s/%s/_stream_load", host, port, DB_NAME, TABLE_NAME);
    }

    @Test
    public void testLoadTest100ContinueRespondHTTP307() throws Exception {
        new MockUp<NodeSelector>() {
            @Mock
            public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable,
                                                  boolean isCreate, Multimap<String, String> locReq) {
                List<Long> result = new ArrayList<>();
                result.add(testBackendId1);
                return result;
            }
        };

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .build();

        // reuse the same client
        // NOTE: okhttp client will close the connection and create a new connection, so the issue can't be reproduced.
        CloseableHttpClient client = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return false;
                    }
                })
                .setDefaultRequestConfig(requestConfig)
                .build();

        int repeat = 3;
        for (int i = 0; i < repeat; ++i) {
            // NOTE: Just a few bytes, so the next request header is corrupted but not completely available at all.
            // otherwise FE will discard bytes from the connection as many as X bytes, and possibly skip the
            // next request entirely, so it will be looked like the server never respond at all from client side.
            HttpPut put = buildPutRequest(2, true);
            try (CloseableHttpResponse response = client.execute(put)) {
                Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                        response.getStatusLine().getStatusCode());
                // The server indicates that the connection should be closed.
                Assert.assertEquals(HttpHeaderValues.CLOSE.toString(),
                        response.getFirstHeader(HttpHeaderNames.CONNECTION.toString()).getValue());
            }
        }
        client.close();
    }

    @Test
    public void testLoad100ContinueBackwardsCompatible() throws Exception {
        new MockUp<NodeSelector>() {
            @Mock
            public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable,
                                                  boolean isCreate, Multimap<String, String> locReq) {
                List<Long> result = new ArrayList<>();
                result.add(testBackendId1);
                return result;
            }
        };

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .build();

        CloseableHttpClient client = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return false;
                    }
                })
                .setDefaultRequestConfig(requestConfig)
                .build();

        {
            // HTTP/1.1, has 'Expect: 100-Continue', responds HTTP 307
            HttpPut put = buildPutRequest(128, true);
            put.setProtocolVersion(new ProtocolVersion("HTTP", 1, 1));
            try (CloseableHttpResponse response = client.execute(put)) {
                Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                        response.getStatusLine().getStatusCode());
                // The server indicates that the connection should be closed.
                Assert.assertEquals(HttpHeaderValues.CLOSE.toString(),
                        response.getFirstHeader(HttpHeaderNames.CONNECTION.toString()).getValue());
            }
        }

        {
            // HTTP/1.1, no 'Expect: 100-Continue'. responds HTTP 200 with error msg
            HttpPut put = buildPutRequest(256, false);
            put.setProtocolVersion(new ProtocolVersion("HTTP", 1, 1));
            try (CloseableHttpResponse response = client.execute(put)) {
                Assert.assertEquals(HttpResponseStatus.OK.code(),
                        response.getStatusLine().getStatusCode());
                // The server indicates that the connection should be closed.
                Assert.assertEquals(HttpHeaderValues.CLOSE.toString(),
                        response.getFirstHeader(HttpHeaderNames.CONNECTION.toString()).getValue());

                String body = new BasicResponseHandler().handleResponse(response);
                Map<String, Object> result = objectMapper.readValue(body, new TypeReference<>() {});

                // {"Status":"FAILED","Message":"class com.starrocks.common.DdlException: There is no 100-continue header"}
                Assert.assertEquals("FAILED", result.get("Status"));
                Assert.assertEquals("class com.starrocks.common.DdlException: There is no 100-continue header",
                        result.get("Message"));
            }
        }

        {
            // HTTP/1.0, no 'Expect: 100-Continue'. responds HTTP 307
            HttpPut put = buildPutRequest(512, false);
            put.setProtocolVersion(new ProtocolVersion("HTTP", 1, 0));
            try (CloseableHttpResponse response = client.execute(put)) {
                Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                        response.getStatusLine().getStatusCode());
                // The server indicates that the connection should be closed.
                Assert.assertEquals(HttpHeaderValues.CLOSE.toString(),
                        response.getFirstHeader(HttpHeaderNames.CONNECTION.toString()).getValue());
            }
        }
        client.close();
    }

    @Test
    public void testStreamLoadSkipNonAliveNodesForSharedNothing() throws Exception {
        new MockUp<NodeSelector>() {
            @Mock
            public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable,
                                                  boolean isCreate, Multimap<String, String> locReq) {
                assertEquals(1, backendNum);
                assertTrue(needAvailable);
                assertFalse(isCreate);
                return new ArrayList<>();
            }
        };

        Map<String, String> map = new HashMap<>();
        Request request = buildRequest(map);
        try (Response response = noRedirectClient.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("FAILED", result.get("Status"));
            assertEquals("class com.starrocks.common.DdlException: No backend alive.", result.get("Message"));
        }
    }

    @Test
    public void testStreamLoadSkipNonAliveNodesForSharedData() throws Exception {
        SystemInfoService service = new SystemInfoService();
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        backend.setAlive(false);
        service.addBackend(backend);

        List<Long> nodeIds = new ArrayList<>();
        nodeIds.add(1L);

        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(DEFAULT_WAREHOUSE_NAME);
                result = nodeIds;
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = service;
            }
        };

        Map<String, String> map = new HashMap<>();
        Request request = buildRequest(map);
        try (Response response = noRedirectClient.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("FAILED", result.get("Status"));
            assertEquals("class com.starrocks.common.DdlException: No backend alive.", result.get("Message"));
        }
    }
}
