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
import com.starrocks.system.NodeSelector;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
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
}
