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

package com.starrocks.context.ai;

import com.starrocks.context.error.ContextException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link FeRerankClient}'s retry/deadline policy — the latency-bounding contract:
 * <ul>
 *   <li>a 2xx response returns reordered indices on the first try (no retry),</li>
 *   <li>HTTP 5xx is retried up to the attempt cap,</li>
 *   <li>HTTP 4xx is NOT retried (client/config error, fail fast),</li>
 *   <li>a request timeout is NOT retried (degrade now, don't multiply the stall),</li>
 *   <li>the overall {@code deadline_ms} budget bounds total latency even when {@code timeout_ms}
 *       is large, and</li>
 *   <li>a genuine connection failure (refused) IS retried up to the attempt cap.</li>
 * </ul>
 */
public class FeRerankClientTest {

    private static HttpServer startServer(HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/rerank", handler);
        // Cached pool so a handler that sleeps (timeout/deadline tests) never blocks the next request.
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        return server;
    }

    private static String endpoint(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/rerank";
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static AIProvider rerankProvider(String endpoint, Integer timeoutMs, Integer deadlineMs) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(AIProvider.PROPERTY_ENDPOINT, endpoint);
        p.put(AIProvider.PROPERTY_MODEL, "test-model");
        if (timeoutMs != null) {
            p.put(AIProvider.PROPERTY_TIMEOUT_MS, String.valueOf(timeoutMs));
        }
        if (deadlineMs != null) {
            p.put(AIProvider.PROPERTY_DEADLINE_MS, String.valueOf(deadlineMs));
        }
        return new AIProvider("id-rr", "rr", AIProviderType.RERANK, p, "");
    }

    @Test
    public void testSuccessReturnsReorderedNoRetry() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startServer(ex -> {
            hits.incrementAndGet();
            respond(ex, 200,
                    "{\"results\":[{\"index\":1,\"relevance_score\":0.9},{\"index\":0,\"relevance_score\":0.1}]}");
        });
        try {
            List<FeRerankClient.ScoredIndex> out = FeRerankClient.rerank(
                    rerankProvider(endpoint(server), 2000, 5000), "q", Arrays.asList("a", "b"));
            Assertions.assertEquals(1, hits.get(), "a 2xx must not be retried");
            Assertions.assertEquals(2, out.size());
            Assertions.assertEquals(1, out.get(0).index, "highest relevance first");
            Assertions.assertEquals(0, out.get(1).index);
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testServerErrorRetriedThenThrows() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startServer(ex -> {
            hits.incrementAndGet();
            respond(ex, 503, "overloaded");
        });
        try {
            ContextException e = Assertions.assertThrows(ContextException.class, () ->
                    FeRerankClient.rerank(rerankProvider(endpoint(server), 2000, 5000), "q", Arrays.asList("a")));
            Assertions.assertEquals(FeRerankClient.MAX_SEND_ATTEMPTS, hits.get(), "5xx must be retried to the cap");
            Assertions.assertTrue(e.getMessage().contains("503"), e.getMessage());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testClientErrorNotRetried() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startServer(ex -> {
            hits.incrementAndGet();
            respond(ex, 400, "bad request");
        });
        try {
            Assertions.assertThrows(ContextException.class, () ->
                    FeRerankClient.rerank(rerankProvider(endpoint(server), 2000, 5000), "q", Arrays.asList("a")));
            Assertions.assertEquals(1, hits.get(), "4xx is terminal and must not be retried");
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testTimeoutNotRetried() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startServer(ex -> {
            hits.incrementAndGet();
            sleep(2000);
            try {
                respond(ex, 200, "{\"results\":[]}");
            } catch (IOException ignore) {
                // client already gave up; nothing to do
            }
        });
        try {
            long t0 = System.nanoTime();
            ContextException e = Assertions.assertThrows(ContextException.class, () ->
                    FeRerankClient.rerank(rerankProvider(endpoint(server), 300, 5000), "q", Arrays.asList("a")));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            Assertions.assertEquals(1, hits.get(), "a timeout must not be retried");
            Assertions.assertTrue(e.getMessage().toLowerCase().contains("timed out"), e.getMessage());
            Assertions.assertTrue(ms < 1500, "should fail near the 300ms request timeout, took " + ms + "ms");
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testOverallDeadlineBoundsLatency() throws Exception {
        HttpServer server = startServer(ex -> {
            sleep(30000);
            try {
                respond(ex, 200, "{\"results\":[]}");
            } catch (IOException ignore) {
                // client already gave up
            }
        });
        try {
            long t0 = System.nanoTime();
            // Large per-request timeout, tiny deadline: the deadline must clamp the request timeout to
            // ~500ms so the call degrades fast instead of waiting out timeout_ms (20s) or the server (30s).
            Assertions.assertThrows(ContextException.class, () ->
                    FeRerankClient.rerank(rerankProvider(endpoint(server), 20000, 500), "q", Arrays.asList("a")));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            Assertions.assertTrue(ms < 3000, "deadline=500ms must bound latency well under the server sleep, took "
                    + ms + "ms");
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testConnectionFailureRetried() throws Exception {
        // Bind, capture the port, then stop the server so the port is closed -> connections are
        // refused (a fast IOException, not a timeout) and must be retried up to the attempt cap.
        HttpServer server = startServer(ex -> respond(ex, 200, "{\"results\":[]}"));
        String ep = endpoint(server);
        server.stop(0);

        ContextException e = Assertions.assertThrows(ContextException.class, () ->
                FeRerankClient.rerank(rerankProvider(ep, 2000, 5000), "q", Arrays.asList("a")));
        Assertions.assertTrue(
                e.getMessage().contains("connect failed after " + FeRerankClient.MAX_SEND_ATTEMPTS + " attempts"),
                "connection refused should be retried to the cap; got: " + e.getMessage());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
