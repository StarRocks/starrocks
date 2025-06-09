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
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
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
import java.util.List;
import java.util.Map;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadActionTest extends StarRocksHttpTestCase {

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

    @Test
    public void testLoadTest100ContinueRespondHTTP307() throws Exception {
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                List<Backend> bes = new ArrayList<>();
                bes.add(new Backend());
                bes.get(0).setId(testBackendId1);
                return bes;
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
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                List<Backend> bes = new ArrayList<>();
                bes.add(new Backend());
                bes.get(0).setId(testBackendId1);
                return bes;
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
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getAvailableBackends() {
                return new ArrayList<>();
            }
        };

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .build();
        try (CloseableHttpClient client = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return false;
                    }
                })
                .setDefaultRequestConfig(requestConfig)
                .build()) {

            HttpPut put = buildPutRequest(2, true);
            try (CloseableHttpResponse response = client.execute(put)) {
                Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
                String body = new BasicResponseHandler().handleResponse(response);
                Map<String, Object> result =
                        objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                assertEquals("FAILED", result.get("Status"));
                assertEquals("class com.starrocks.common.DdlException: No backend alive.", result.get("Message"));
            }
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

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .build();
        try (CloseableHttpClient client = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return false;
                    }
                })
                .setDefaultRequestConfig(requestConfig)
                .build()) {

            HttpPut put = buildPutRequest(2, true);
            try (CloseableHttpResponse response = client.execute(put)) {
                Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
                String body = new BasicResponseHandler().handleResponse(response);
                Map<String, Object> result =
                        objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                assertEquals("FAILED", result.get("Status"));
                assertEquals("class com.starrocks.common.DdlException: No backend alive.", result.get("Message"));
            }
        }
    }

    @Test
    public void testBlackListForStreamLoad() throws Exception {
        List<ComputeNode> computeNodes = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            String host = "192.0.0." + i;
            int httpPort = 8040;
            computeNodes.add(new ComputeNode(i, host, 9050));
            computeNodes.get(i - 1).setHttpPort(httpPort);
        }

        {
            new MockUp<SystemInfoService>() {
                @Mock
                public List<Backend> getAvailableBackends() {
                    List<Backend> bes = new ArrayList<>();
                    bes.add(new Backend());
                    bes.add(new Backend());
                    bes.add(new Backend());
                    bes.get(0).setId(1L);
                    bes.get(1).setId(2L);
                    bes.get(2).setId(3L);
                    return bes;
                }
            };

            new MockUp<SystemInfoService>() {
                @Mock
                public ComputeNode getBackendOrComputeNode(long nodeId) {
                    return computeNodes.get(((int) nodeId) - 1);
                }
            };

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectionRequestTimeout(3000)
                    .build();
            try (CloseableHttpClient client = HttpClients
                        .custom()
                        .setRedirectStrategy(new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return false;
                            }
                        })
                        .setDefaultRequestConfig(requestConfig)
                        .build()) {

                SimpleScheduler.addToBlocklist(1L);
                int loop = 10;
                while (loop > 0) {
                    HttpPut put = buildPutRequest(2, true);
                    try (CloseableHttpResponse response = client.execute(put)) {
                        Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                                response.getStatusLine().getStatusCode());
                        String location = response.getFirstHeader(HttpHeaderNames.LOCATION.toString()).getValue();
                        assertTrue(location.contains("192.0.0.2") || location.contains("192.0.0.3"));
                    }
                    loop = loop - 1;
                }
                SimpleScheduler.removeFromBlocklist(1L);

                SimpleScheduler.addToBlocklist(2L);
                loop = 10;
                while (loop > 0) {
                    HttpPut put = buildPutRequest(2, true);
                    try (CloseableHttpResponse response = client.execute(put)) {
                        Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                                response.getStatusLine().getStatusCode());
                        String location = response.getFirstHeader(HttpHeaderNames.LOCATION.toString()).getValue();
                        assertTrue(location.contains("192.0.0.1") || location.contains("192.0.0.3"));
                    }
                    loop = loop - 1;
                }
                SimpleScheduler.removeFromBlocklist(2L);

                SimpleScheduler.addToBlocklist(3L);
                loop = 10;
                while (loop > 0) {
                    HttpPut put = buildPutRequest(2, true);
                    try (CloseableHttpResponse response = client.execute(put)) {
                        Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                                response.getStatusLine().getStatusCode());
                        String location = response.getFirstHeader(HttpHeaderNames.LOCATION.toString()).getValue();
                        assertTrue(location.contains("192.0.0.1") || location.contains("192.0.0.2"));
                    }
                    loop = loop - 1;
                }
                SimpleScheduler.removeFromBlocklist(3L);

                SimpleScheduler.addToBlocklist(1L);
                SimpleScheduler.addToBlocklist(2L);
                SimpleScheduler.addToBlocklist(3L);
                loop = 10;
                while (loop > 0) {
                    HttpPut put = buildPutRequest(2, true);
                    try (CloseableHttpResponse response = client.execute(put)) {
                        Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT.code(),
                                response.getStatusLine().getStatusCode());
                        String location = response.getFirstHeader(HttpHeaderNames.LOCATION.toString()).getValue();
                        assertTrue(location.contains("192.0.0.1") ||
                                location.contains("192.0.0.2") ||
                                location.contains("192.0.0.3"));
                    }
                    loop = loop - 1;
                }
                SimpleScheduler.removeFromBlocklist(1L);
                SimpleScheduler.removeFromBlocklist(2L);
                SimpleScheduler.removeFromBlocklist(3L);
            }
        }
    }
}
