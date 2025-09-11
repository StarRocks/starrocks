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

import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HttpUtilsTest extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/system";

    @Test
    public void testGetHttpClient() {
        CloseableHttpClient httpClient1 = HttpUtils.getInstance();
        CloseableHttpClient httpClient2 = HttpUtils.getInstance();
        Assertions.assertEquals(httpClient1, httpClient2);
    }

    @Test
    public void testHttpGet() throws Exception {
        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = "http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?path=/backends";
        String result = HttpUtils.get(url, header);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testHttpPostFails()  {
        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = URI + "/_query_plan";
        StringEntity entity = new StringEntity(
                "{ \"sql\" :  \" select k1 as alias_1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }",
                StandardCharsets.UTF_8);
        Assertions.assertThrows(HttpRequestException.class, () -> {
            HttpUtils.post(url, entity, header);
        });
    }

    @Test
    public void testHttpPostFailsWithoutEntity()  {

        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = URI + "/_query_plan";
        StringEntity entity = null;
        Assertions.assertThrows(HttpRequestException.class, () -> {
            HttpUtils.post(url, entity, header);
        });
    }

    @Test
    public void testHttpPostFailsWithIOException()  {

        new MockUp<CloseableHttpClient>() {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                throw new IOException("http execute failed");
            }
        };

        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = URI + "/_query_plan";
        StringEntity entity = null;
        Assertions.assertThrows(HttpRequestException.class, () -> {
            HttpUtils.post(url, entity, header);
        });
    }

    @Test
    public void testHttpPostFailsWhenHttpClientUnavailable()  {

        new MockUp<HttpUtils>() {
            @Mock
            public static CloseableHttpClient getInstance() {
                return null;
            }
        };

        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = URI + "/_query_plan";
        StringEntity entity = new StringEntity(
                "{ \"sql\" :  \" select k1 as alias_1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }",
                StandardCharsets.UTF_8);
        Assertions.assertThrows(HttpRequestException.class, () -> {
            HttpUtils.post(url, entity, header);
        });
    }

    @Test
    public void testGetInstance() {
        CloseableHttpClient client1 = HttpUtils.getInstance();
        CloseableHttpClient client2 = HttpUtils.getInstance();
        Assertions.assertSame(client1, client2);
    }
}
