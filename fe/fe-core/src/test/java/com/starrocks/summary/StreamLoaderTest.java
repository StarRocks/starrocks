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

package com.starrocks.summary;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(15)
public class StreamLoaderTest {
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).build();
    private static final String RESPONSE = "{\"Status\":\"Success\",\"Message\":\"OK\"}";

    private HttpServer server;
    private ExecutorService executor;
    private final CountDownLatch releaseResponse = new CountDownLatch(1);

    @BeforeEach
    public void setUp() throws IOException {
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/success", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] bytes = RESPONSE.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        releaseResponse.countDown();
        server.stop(0);
        executor.shutdownNow();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private HttpRequest request(String path, Duration timeout) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path))
                .timeout(timeout).PUT(HttpRequest.BodyPublishers.ofString("[{\"queryId\":\"q1\"}]")).build();
    }

    private void awaitRelease() {
        try {
            releaseResponse.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendPartialBody(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().readAllBytes();
        exchange.sendResponseHeaders(200, RESPONSE.length());
        exchange.getResponseBody().write('{');
        exchange.getResponseBody().flush();
    }

    @Test
    public void testReadsCompleteResponse() throws Exception {
        String payload = "x".repeat(2 * 1024 * 1024);
        AtomicReference<String> received = new AtomicReference<>();
        server.createContext("/upload", exchange -> {
            received.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = RESPONSE.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        HttpRequest request = HttpRequest.newBuilder(request("/upload", Duration.ofSeconds(5)).uri())
                .timeout(Duration.ofSeconds(5)).PUT(HttpRequest.BodyPublishers.ofString(payload)).build();
        HttpResponse<String> response = StreamLoader.sendRequest(CLIENT, request);
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(RESPONSE, response.body());
        Assertions.assertEquals(payload, received.get());
    }

    @Test
    public void testTimeoutWaitingForHeaders() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        server.createContext("/no-headers", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                received.countDown();
                awaitRelease();
            }
        });
        Future<?> result = executor.submit(() -> Assertions.assertThrows(HttpTimeoutException.class,
                () -> StreamLoader.sendRequest(CLIENT, request("/no-headers", Duration.ofSeconds(1)))));
        Assertions.assertTrue(received.await(3, TimeUnit.SECONDS));
        result.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTimeoutWhileUploading() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        server.createContext("/blocked-upload", exchange -> {
            try (exchange) {
                received.countDown();
                // Leave a body larger than the socket buffers unread, so the upload cannot finish.
                awaitRelease();
            }
        });
        HttpRequest request = HttpRequest.newBuilder(request("/blocked-upload", Duration.ofSeconds(1)).uri())
                .timeout(Duration.ofSeconds(1))
                .PUT(HttpRequest.BodyPublishers.ofString("x".repeat(16 * 1024 * 1024))).build();
        Future<?> result = executor.submit(() -> Assertions.assertThrows(HttpTimeoutException.class,
                () -> StreamLoader.sendRequest(CLIENT, request)));
        Assertions.assertTrue(received.await(3, TimeUnit.SECONDS));
        result.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testRepeatedBodyTimeoutsThenRecovery() throws Exception {
        CountDownLatch headersSent = new CountDownLatch(3);
        server.createContext("/stalled-body", exchange -> {
            try (exchange) {
                sendPartialBody(exchange);
                headersSent.countDown();
                awaitRelease();
            }
        });
        for (int i = 0; i < 3; i++) {
            Assertions.assertThrows(HttpTimeoutException.class,
                    () -> StreamLoader.sendRequest(CLIENT, request("/stalled-body", Duration.ofSeconds(1))));
        }
        Assertions.assertEquals(0, headersSent.getCount(), "the deadlines must cover stalls after receiving headers");
        // Stalled handlers are still waiting: recovery must not depend on releasing their bodies.
        Assertions.assertEquals(RESPONSE,
                StreamLoader.sendRequest(CLIENT, request("/success", Duration.ofSeconds(3))).body());
    }

    @Test
    public void testTricklingBodyDoesNotResetDeadlineAndConnectionIsCancelled() throws Exception {
        CountDownLatch disconnected = new CountDownLatch(1);
        server.createContext("/trickling-body", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                exchange.sendResponseHeaders(200, 0);
                while (!releaseResponse.await(50, TimeUnit.MILLISECONDS)) {
                    exchange.getResponseBody().write(' ');
                    exchange.getResponseBody().flush();
                }
            } catch (IOException e) {
                disconnected.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Assertions.assertThrows(HttpTimeoutException.class,
                () -> StreamLoader.sendRequest(CLIENT, request("/trickling-body", Duration.ofSeconds(1))));
        Assertions.assertTrue(disconnected.await(3, TimeUnit.SECONDS), "cancellation must reach the HTTP exchange");
    }

    @Test
    public void testInterruptWhileReadingBody() throws Exception {
        CountDownLatch headersSent = new CountDownLatch(1);
        server.createContext("/interrupted-body", exchange -> {
            try (exchange) {
                sendPartialBody(exchange);
                headersSent.countDown();
                awaitRelease();
            }
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                StreamLoader.sendRequest(CLIENT, request("/interrupted-body", Duration.ofSeconds(10)));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        caller.start();
        try {
            Assertions.assertTrue(headersSent.await(3, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(3000);
            Assertions.assertFalse(caller.isAlive());
            Assertions.assertInstanceOf(InterruptedException.class, failure.get());
        } finally {
            caller.interrupt();
            releaseResponse.countDown();
            caller.join(3000);
        }
    }

    @Test
    public void testTimeoutCancelsOriginalFuture() {
        HttpClient client = Mockito.mock(HttpClient.class);
        HttpRequest request = request("/unused", Duration.ofMillis(50));
        CompletableFuture<HttpResponse<String>> future = Mockito.spy(new CompletableFuture<>());
        Mockito.when(client.sendAsync(Mockito.eq(request), Mockito.<HttpResponse.BodyHandler<String>>any()))
                .thenReturn(future);
        Assertions.assertThrows(HttpTimeoutException.class, () -> StreamLoader.sendRequest(client, request));
        Mockito.verify(future).cancel(true);
        Assertions.assertTrue(future.isCancelled());
    }

    @Test
    public void testInterruptCancelsOriginalFuture() throws Exception {
        HttpClient client = Mockito.mock(HttpClient.class);
        HttpRequest request = request("/unused", Duration.ofSeconds(5));
        CompletableFuture<HttpResponse<String>> future = Mockito.spy(new CompletableFuture<>());
        Mockito.when(client.sendAsync(Mockito.eq(request), Mockito.<HttpResponse.BodyHandler<String>>any()))
                .thenReturn(future);
        try {
            Thread.currentThread().interrupt();
            Assertions.assertThrows(InterruptedException.class, () -> StreamLoader.sendRequest(client, request));
            Mockito.verify(future).cancel(true);
            Assertions.assertTrue(future.isCancelled());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testPreservesOriginalFailure() {
        HttpClient client = Mockito.mock(HttpClient.class);
        HttpRequest request = request("/unused", Duration.ofSeconds(5));
        for (Throwable cause : new Throwable[] {new IOException("connection reset"),
                new HttpTimeoutException("request timeout"), new IllegalArgumentException("bad request"),
                new AssertionError("client failure")}) {
            Mockito.when(client.sendAsync(Mockito.eq(request), Mockito.<HttpResponse.BodyHandler<String>>any()))
                    .thenReturn(CompletableFuture.failedFuture(cause));
            Throwable actual = Assertions.assertThrows(cause.getClass(), () -> StreamLoader.sendRequest(client, request));
            Assertions.assertSame(cause, actual);
        }
    }

    @Test
    public void testSuccessfulResponseIncludesRowStatistics() {
        StreamLoader.Response response = StreamLoader.parseResponse(200,
                "{\"Status\":\"Success\",\"Message\":\"OK\",\"NumberTotalRows\":20,"
                        + "\"NumberLoadedRows\":19,\"NumberFilteredRows\":1,\"ErrorURL\":\"http://be/errors/1\"}");
        Assertions.assertEquals(200, response.status());
        Assertions.assertEquals(20L, response.stats().totalRows());
        Assertions.assertEquals(19L, response.stats().loadedRows());
        Assertions.assertEquals(1L, response.stats().filteredRows());
        Assertions.assertEquals("http://be/errors/1", response.stats().errorUrl());
    }

    @Test
    public void testMissingAndInvalidStatisticsDoNotFailSuccessfulLoad() {
        for (String value : new String[] {"null", "-1", "1.5", "9223372036854775808", "\"bad\"",
                "{}", "[]", "true", "\"1\""}) {
            StreamLoader.Response response = StreamLoader.parseResponse(200,
                    "{\"Status\":\"Success\",\"Message\":\"OK\",\"NumberTotalRows\":20,"
                            + "\"NumberLoadedRows\":19,\"NumberFilteredRows\":" + value + ",\"ErrorURL\":{}}");
            Assertions.assertEquals(200, response.status(), value);
            Assertions.assertNull(response.stats().filteredRows(), value);
            Assertions.assertEquals(19L, response.stats().loadedRows(), "retain independently valid fields");
            Assertions.assertNull(response.stats().errorUrl());
        }
        StreamLoader.Response missing = StreamLoader.parseResponse(200, RESPONSE);
        Assertions.assertEquals(200, missing.status());
        Assertions.assertNull(missing.stats().totalRows());
        Assertions.assertNull(missing.stats().loadedRows());
        Assertions.assertNull(missing.stats().filteredRows());
    }

    @Test
    public void testCountsAreParsedExactlyWithoutTruncationOrOverflow() {
        StreamLoader.Response response = StreamLoader.parseResponse(200,
                "{\"Status\":\"Success\",\"Message\":\"OK\",\"NumberTotalRows\":9223372036854775807,"
                        + "\"NumberLoadedRows\":0,\"NumberFilteredRows\":0}");
        Assertions.assertEquals(Long.MAX_VALUE, response.stats().totalRows());
        Assertions.assertEquals(0L, response.stats().loadedRows());
        Assertions.assertEquals(0L, response.stats().filteredRows());
    }

    @Test
    public void testFailedResponsesDoNotExposeCommittedRowStatistics() {
        StreamLoader.Response failed = StreamLoader.parseResponse(200,
                "{\"Status\":\"Fail\",\"Message\":\"rejected\",\"NumberFilteredRows\":1}");
        Assertions.assertEquals(500, failed.status());
        Assertions.assertEquals("rejected", failed.msg());
        Assertions.assertNull(failed.stats());
        StreamLoader.Response unauthorized = StreamLoader.parseResponse(401, "unauthorized");
        Assertions.assertEquals(401, unauthorized.status());
        Assertions.assertEquals("unauthorized", unauthorized.msg());
        Assertions.assertNull(unauthorized.stats());
    }
}
