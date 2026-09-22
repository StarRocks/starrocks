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
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Timeout;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
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
        List<Request> captured = Collections.synchronizedList(new ArrayList<>());
        mockMappingRequests(mappings, captured);

        EsRestClient client = new EsRestClient(new String[] {"http://127.0.0.1:9200"}, "", "");
        String mapping = client.getMapping("index_user");

        Assertions.assertEquals(mappings.get("index_user"), mapping);
        Assertions.assertTrue(mapping.contains("user_id"));
        Assertions.assertTrue(mapping.contains("user_name"));
        Assertions.assertFalse(mapping.contains("order_id"));
        Assertions.assertFalse(mapping.contains("product_id"));
        Assertions.assertEquals(1, captured.size());
        Assertions.assertEquals("http://127.0.0.1:9200/index_user/_mapping", captured.get(0).url().toString());
    }

    @Test
    public void testGetMappingsConcurrently() throws Exception {
        Map<String, String> mappings = loadIndexMappings();
        List<Request> captured = Collections.synchronizedList(new ArrayList<>());
        mockMappingRequests(mappings, captured);

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
        Assertions.assertEquals(taskCount, captured.size());
    }

    @Test
    public void testGetMappingWithoutHttpScheme() throws IOException {
        Map<String, String> mappings = loadIndexMappings();
        List<Request> captured = Collections.synchronizedList(new ArrayList<>());
        mockMappingRequests(mappings, captured);

        // node address without http(s) scheme and with surrounding spaces, as users may configure
        EsRestClient client = new EsRestClient(new String[] {"  127.0.0.1:9200  "}, "", "");
        String mapping = client.getMapping("index_product");

        Assertions.assertEquals(mappings.get("index_product"), mapping);
        Assertions.assertEquals(1, captured.size());
        // spaces are trimmed and "http://" is prepended automatically
        Assertions.assertEquals("http://127.0.0.1:9200/index_product/_mapping", captured.get(0).url().toString());
    }

    @Test
    public void testGetMappingWithBasicAuth() throws IOException {
        Map<String, String> mappings = loadIndexMappings();
        List<Request> captured = Collections.synchronizedList(new ArrayList<>());
        mockMappingRequests(mappings, captured);

        String authUser = "es_user";
        String authPassword = "es_password";
        EsRestClient client = new EsRestClient(new String[] {"127.0.0.1:9200"}, authUser, authPassword);
        String mapping = client.getMapping("index_order");

        // the fake server ignores credentials, but the client must attach the basic auth header
        Assertions.assertEquals(mappings.get("index_order"), mapping);
        Assertions.assertEquals(1, captured.size());
        Assertions.assertEquals(Credentials.basic(authUser, authPassword),
                captured.get(0).header("Authorization"));
        Assertions.assertEquals("http://127.0.0.1:9200/index_order/_mapping", captured.get(0).url().toString());
    }

    @Test
    public void testGetMappingFailsOverToNextNodeOnIOException() throws IOException {
        Map<String, String> mappings = loadIndexMappings();
        List<Request> captured = Collections.synchronizedList(new ArrayList<>());
        int deadPort = 19200;
        mockOkHttpClient(request -> {
            captured.add(request);
            if (request.url().port() == deadPort) {
                throw new IOException("connection refused");
            }
            String index = request.url().pathSegments().get(0);
            String body = mappings.get(index);
            Assertions.assertNotNull(body, "unexpected mapping request for index: " + index);
            return body;
        });

        EsRestClient client = new EsRestClient(
                new String[] {"http://127.0.0.1:" + deadPort, "http://127.0.0.1:9200"}, "", "");
        String mapping = client.getMapping("index_user");

        Assertions.assertEquals(mappings.get("index_user"), mapping);
        Assertions.assertEquals(2, captured.size());
        Assertions.assertEquals(deadPort, captured.get(0).url().port());
        Assertions.assertEquals(9200, captured.get(1).url().port());
    }

    @Test
    public void testGetMappingWithTraceLogEnabled() throws IOException {
        Map<String, String> mappings = loadIndexMappings();
        mockMappingRequests(mappings, Collections.synchronizedList(new ArrayList<>()));

        String loggerName = EsRestClient.class.getName();
        Configurator.setLevel(loggerName, Level.TRACE);
        try {
            EsRestClient client = new EsRestClient(new String[] {"127.0.0.1:9200"}, "", "");
            Assertions.assertEquals(mappings.get("index_user"), client.getMapping("index_user"));
        } finally {
            Configurator.setLevel(loggerName, Level.INFO);
        }
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

    private interface MappingResponder {
        String respond(Request request) throws IOException;
    }

    private static void mockMappingRequests(Map<String, String> mappingsByIndex, List<Request> captured) {
        mockOkHttpClient(request -> {
            captured.add(request);
            String index = request.url().pathSegments().get(0);
            String body = mappingsByIndex.get(index);
            Assertions.assertNotNull(body, "unexpected mapping request for index: " + index);
            return body;
        });
    }

    private static void mockOkHttpClient(MappingResponder responder) {
        new MockUp<OkHttpClient>() {
            @Mock
            Call newCall(Request request) {
                return new FakeMappingCall(request, responder);
            }
        };
    }

    private static class FakeMappingCall implements Call {
        private final Request request;
        private final MappingResponder responder;

        FakeMappingCall(Request request, MappingResponder responder) {
            this.request = request;
            this.responder = responder;
        }

        @Override
        @SuppressWarnings("deprecation")
        public Response execute() throws IOException {
            String body = responder.respond(request);
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
            return new FakeMappingCall(request, responder);
        }
    }
}
