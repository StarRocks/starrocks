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

package com.starrocks.connector.elasticsearch;

import com.google.common.io.Resources;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Timeout;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EsRestClientTest {

    private static final String[] MAPPING_INDICES = {"index_user", "index_order", "index_product"};

    @Test
    public void testGetRowCount() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return "[{\"docs.count\":\"1234567\"}]";
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(1234567L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountNullResponse() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return null;
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(-1L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountOnException() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                throw new StarRocksConnectorException("connection failed");
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(-1L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountEmptyArray() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return "[]";
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(-1L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountMultipleIndices() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return "[{\"docs.count\":\"1000000\"},{\"docs.count\":\"2000000\"},{\"docs.count\":\"500000\"}]";
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(3500000L, client.getRowCount("logs-*"));
    }

    @Test
    public void testGetMappingSingleRequest() throws IOException {
        Map<String, String> mappings = loadIndexMappings();
        mockMappingRequests(mappings);

        EsRestClient client = new EsRestClient(new String[] {"http://127.0.0.1:9200"}, "", "");
        String mapping = client.getMapping("index_user");

        Assertions.assertEquals(mappings.get("index_user"), mapping);
        Assertions.assertTrue(mapping.contains("user_id"));
        Assertions.assertTrue(mapping.contains("user_name"));
        Assertions.assertFalse(mapping.contains("order_id"));
        Assertions.assertFalse(mapping.contains("product_id"));
    }

    @Test
    public void testGetMappingsConcurrently() throws Exception {
        Map<String, String> mappings = loadIndexMappings();
        mockMappingRequests(mappings);

        int roundsPerIndex = 10;
        int taskCount = MAPPING_INDICES.length * roundsPerIndex;
        CountDownLatch ready = new CountDownLatch(taskCount);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(taskCount);
        List<Future<?>> futures = new ArrayList<>();
        for (String index : MAPPING_INDICES) {
            String expected = mappings.get(index);
            for (int i = 0; i < roundsPerIndex; i++) {
                futures.add(pool.submit(() -> {
                    Assertions.assertTrue(start.await(10, TimeUnit.SECONDS));
                    EsRestClient client = new EsRestClient(new String[] {"http://127.0.0.1:9200"}, "", "");
                    String mapping = client.getMapping(index);
                    Assertions.assertEquals(expected, mapping);
                    return null;
                }));
                ready.countDown();
            }
        }

        Assertions.assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        for (Future<?> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("concurrent mapping request failed", e.getCause());
            }
        }
        pool.shutdown();
    }

    private static Map<String, String> loadIndexMappings() throws IOException {
        Map<String, String> mappings = new LinkedHashMap<>();
        for (String index : MAPPING_INDICES) {
            String body = Resources.toString(
                    Resources.getResource("data/es/" + index + "_mapping.json"), StandardCharsets.UTF_8);
            mappings.put(index, body);
        }
        return mappings;
    }

    private static void mockMappingRequests(Map<String, String> mappingsByIndex) {
        new MockUp<OkHttpClient>() {
            @Mock
            Call newCall(Request request) {
                String index = request.url().pathSegments().get(0);
                String body = mappingsByIndex.get(index);
                Assertions.assertNotNull(body, "unexpected mapping request for index: " + index);
                return new FakeMappingCall(request, body);
            }
        };
    }

    private static class FakeMappingCall implements Call {
        private final Request request;
        private final String body;

        FakeMappingCall(Request request, String body) {
            this.request = request;
            this.body = body;
        }

        @Override
        @SuppressWarnings("deprecation")
        public Response execute() {
            return new Response.Builder()
                    .request(request)
                    .protocol(Protocol.HTTP_1_1)
                    .code(200)
                    .message("OK")
                    .body(ResponseBody.create(MediaType.parse("application/json"), body))
                    .build();
        }

        @Override
        public Request request() {
            return request;
        }

        @Override
        public void enqueue(Callback responseCallback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel() {
        }

        @Override
        public boolean isExecuted() {
            return false;
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public Timeout timeout() {
            return Timeout.NONE;
        }

        @Override
        public Call clone() {
            return new FakeMappingCall(request, body);
        }
    }
}
