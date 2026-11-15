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

import com.google.common.collect.Multimap;
import com.starrocks.common.Config;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.sun.net.httpserver.HttpServer;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(15)
public class AuditStreamLoaderTest {
    private HttpServer server;
    private final List<String> labels = new ArrayList<>();
    private final List<String> payloads = new ArrayList<>();
    private final List<Long> times = new ArrayList<>();
    private final AtomicLong clock = new AtomicLong();
    private String responseBody = "{\"Status\":\"Success\",\"Message\":\"OK\"}";
    private int httpStatus = 200;

    @BeforeAll
    public static void initialize() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/", exchange -> {
            labels.add(exchange.getRequestHeaders().getFirst("label"));
            payloads.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            times.add(clock.get());
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(httpStatus, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
        Backend be = new Backend(7L, "127.0.0.1", 9050);
        be.setHttpPort(server.getAddress().getPort());
        new MockUp<NodeSelector>() {
            @Mock
            public List<Long> seqChooseBackendIds(int count, boolean available, boolean create,
                                                  Multimap<String, String> location) {
                return List.of(7L);
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long id) {
                return be;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        server.stop(0);
    }

    private static AuditEvent event() {
        AuditEvent event = new AuditEvent();
        event.type = AuditEvent.EventType.AFTER_QUERY;
        event.timestamp = 1000;
        event.queryId = "audit-http-test";
        event.stmt = "SELECT '中文😀\\\"'";
        return event;
    }

    @Test
    public void testRealHttpPublishTimeoutCommitsOnlyOnce() {
        responseBody = "{\"Status\":\"Publish Timeout\",\"Message\":\"committed; not visible yet\","
                + "\"NumberTotalRows\":1,\"NumberLoadedRows\":1,\"NumberFilteredRows\":0}";
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(event());
        String expectedPayload = "[" + mgr.formatRowJson(event()) + "]";
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.SUCCESS, mgr.flush());
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.EMPTY, mgr.flush());
        Assertions.assertEquals(1, labels.size());
        Assertions.assertEquals(expectedPayload, payloads.get(0));
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
        Assertions.assertEquals(0, mgr.droppedEvents());
        Assertions.assertEquals(1, mgr.batchesWithPublishTimeout());
    }

    @Test
    public void testLostCommitResponseUsesTheSameLabelUntilFinished() {
        Set<String> committed = new HashSet<>();
        server.removeContext("/api/");
        server.createContext("/api/", exchange -> {
            String label = exchange.getRequestHeaders().getFirst("label");
            labels.add(label);
            payloads.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            // Simulate BE committing once, followed by a damaged response that the caller cannot parse.
            String reply;
            if (committed.add(label)) {
                reply = "unreadable commit response";
            } else {
                reply = "{\"Status\":\"Label Already Exists\",\"Label\":\"" + label
                        + "\",\"ExistingJobStatus\":\"FINISHED\",\"NumberTotalRows\":0,\"NumberLoadedRows\":0,"
                        + "\"NumberFilteredRows\":0}";
            }
            byte[] bytes = reply.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(event());
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.RETRY, mgr.flush());
        AuditEvent later = event();
        later.queryId = "later";
        mgr.offerEvent(later);
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.SUCCESS, mgr.flush());
        Assertions.assertEquals(1, committed.size());
        Assertions.assertEquals(2, labels.size());
        Assertions.assertEquals(labels.get(0), labels.get(1));
        Assertions.assertEquals(payloads.get(0), payloads.get(1));
        Assertions.assertEquals(1, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.droppedEvents());
        Assertions.assertEquals(1, mgr.batchesAlreadyCommitted());
        Assertions.assertEquals(0, mgr.batchesWithPublishTimeout());
        Assertions.assertEquals(1, mgr.batchesWithUnknownStatistics(), "duplicate counters do not describe the original load");
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.RETRY, mgr.flush());
        Assertions.assertEquals(2, committed.size());
        Assertions.assertNotEquals(labels.get(1), labels.get(2));
    }

    @Test
    public void testRunningDuplicateWaitsForFinishedWithTheSameLabel() {
        server.removeContext("/api/");
        server.createContext("/api/", exchange -> {
            String label = exchange.getRequestHeaders().getFirst("label");
            labels.add(label);
            exchange.getRequestBody().readAllBytes();
            String state = labels.size() == 1 ? "RUNNING" : "FINISHED";
            byte[] bytes = ("{\"Status\":\"Label Already Exists\",\"Label\":\"" + label
                    + "\",\"ExistingJobStatus\":\"" + state + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(event());
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.RETRY, mgr.flush());
        Assertions.assertEquals(1, mgr.bufferedRows());
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.SUCCESS, mgr.flush());
        Assertions.assertEquals(labels.get(0), labels.get(1));
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(1, mgr.batchesAlreadyCommitted());
    }

    @Test
    public void testOnlyFinishedDuplicateWithMatchingLabelConfirmsCommit() {
        String finished = "{\"Status\":\"Label Already Exists\",\"Label\":\"ours\",\"ExistingJobStatus\":\"FINISHED\"}";
        var response = AuditStreamLoader.parseResponse(200, finished, "ours");
        Assertions.assertTrue(response.committed());
        Assertions.assertNull(response.stats());
        Assertions.assertEquals(AuditStreamLoader.Outcome.ALREADY_COMMITTED, response.outcome());
        Assertions.assertFalse(AuditStreamLoader.parseResponse(200, finished, "other").committed());
        Assertions.assertFalse(AuditStreamLoader.parseResponse(200, finished).committed());
        for (String state : List.of("RUNNING", "ABORTED", "UNKNOWN", "")) {
            String body = finished.replace("FINISHED", state);
            Assertions.assertFalse(AuditStreamLoader.parseResponse(200, body, "ours").committed());
        }
        Assertions.assertFalse(AuditStreamLoader.parseResponse(200,
                "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\"}", "ours").committed());
        Assertions.assertFalse(AuditStreamLoader.parseResponse(200,
                "{\"Status\":\"Label Already Exists\",\"Label\":\"ours\"}", "ours").committed());
        Assertions.assertFalse(AuditStreamLoader.parseResponse(503, finished, "ours").committed());
    }

    @Test
    public void testRealHttpContradictoryPartialStatsRemainTerminal() {
        responseBody = "{\"Status\":\"Success\",\"NumberLoadedRows\":1,\"NumberFilteredRows\":1}";
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(event());
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.SUCCESS, mgr.flush());
        Assertions.assertEquals(AuditLoaderMgr.FlushResult.EMPTY, mgr.flush());
        Assertions.assertEquals(1, labels.size());
        Assertions.assertEquals(0, mgr.droppedEvents());
        Assertions.assertEquals(0, mgr.filteredEvents());
        Assertions.assertEquals(1, mgr.batchesWithUnknownStatistics());
    }

    @Test
    public void testRealHttpFailureStillGetsThreeAttemptsAndBackoff() {
        // A phrase in Message must never override the actual failed Status.
        responseBody = "{\"Status\":\"Fail\",\"Message\":\"Publish Timeout is not the status\"}";
        long interval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            AuditLoaderMgr mgr = new AuditLoaderMgr(clock::get);
            mgr.offerEvent(event());
            mgr.maybeFlush();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(4999));
            mgr.maybeFlush();
            Assertions.assertEquals(1, labels.size());
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
            mgr.maybeFlush();
            clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            mgr.maybeFlush();
            Assertions.assertEquals(3, labels.size());
            Assertions.assertEquals(1, labels.stream().distinct().count());
            Assertions.assertEquals(List.of(0L, 5000000000L, 10000000000L), times);
            Assertions.assertEquals(1, payloads.stream().distinct().count());
            Assertions.assertEquals(1, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.batchesWithPublishTimeout());
            Assertions.assertEquals(0, mgr.bufferedRows());
        } finally {
            Config.audit_loader_load_interval_seconds = interval;
        }
    }

    @Test
    public void testRequestRetainsAuditHeadersTimeoutAndSingletonClient() throws Exception {
        AtomicReference<HttpClient> firstClient = new AtomicReference<>();
        new MockUp<StreamLoader>() {
            @Mock
            public HttpResponse<String> sendRequest(HttpClient client, HttpRequest request) {
                if (firstClient.get() == null) {
                    firstClient.set(client);
                }
                Assertions.assertSame(firstClient.get(), client);
                Assertions.assertTrue(request.expectContinue());
                Assertions.assertEquals(Duration.ofSeconds(10), request.timeout().orElseThrow());
                Assertions.assertEquals("/api/starrocks_audit_db__/starrocks_audit_tbl__/_stream_load", request.uri().getPath());
                Assertions.assertEquals("0.05", request.headers().firstValue("max_filter_ratio").orElseThrow());
                Assertions.assertEquals("json", request.headers().firstValue("format").orElseThrow());
                Assertions.assertEquals("true", request.headers().firstValue("strip_outer_array").orElseThrow());
                Assertions.assertTrue(request.headers().firstValue("columns").isEmpty());
                var fe = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
                String auth = new String(Base64.getDecoder().decode(
                        request.headers().firstValue("Authorization").orElseThrow().substring(6)), StandardCharsets.UTF_8);
                Assertions.assertEquals(fe.getHost() + ":" + fe.getNodeName(), auth);
                HttpResponse<String> response = Mockito.mock(HttpResponse.class);
                Mockito.when(response.statusCode()).thenReturn(200);
                Mockito.when(response.body()).thenReturn(responseBody);
                return response;
            }
        };
        for (int i = 0; i < 2; i++) {
            AuditStreamLoader loader = new AuditStreamLoader(AuditLoaderMgr.AUDIT_DB_NAME, AuditLoaderMgr.AUDIT_TABLE_NAME);
            Assertions.assertTrue(loader.loadBatch("audit_loader", "[]").committed());
        }
    }

    @Test
    public void testMissingBackendDoesNotSubmitHttp() throws Exception {
        new MockUp<NodeSelector>() {
            @Mock
            public List<Long> seqChooseBackendIds(int count, boolean available, boolean create,
                                                  Multimap<String, String> location) {
                return List.of();
            }
        };
        var response = new AuditStreamLoader("db", "table").loadBatch("audit_loader", "[]");
        Assertions.assertEquals(412, response.status());
        Assertions.assertFalse(response.committed());
        Assertions.assertTrue(labels.isEmpty());
    }

    @Test
    public void testSharedDataSelectsOnlyAvailableNodesFromDefaultResource() throws Exception {
        ComputeNode alive = new ComputeNode(7L, "127.0.0.1", 9050);
        alive.setHttpPort(server.getAddress().getPort());
        alive.setAlive(true);
        ComputeNode dead = new ComputeNode(8L, "127.0.0.1", 9050);
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource resource) {
                Assertions.assertSame(WarehouseManager.DEFAULT_RESOURCE, resource);
                return List.of(7L, 8L, 9L);
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long id) {
                return id == 7L ? alive : id == 8L ? dead : null;
            }
        };
        AuditStreamLoader loader = new AuditStreamLoader("db", "table");
        Assertions.assertTrue(loader.loadBatch("audit_loader", "[]").committed());
        Assertions.assertEquals(1, labels.size());
        alive.setAlive(false);
        Assertions.assertEquals(412, loader.loadBatch("audit_loader", "[]").status());
        Assertions.assertEquals(1, labels.size());
    }

    @Test
    public void testOnlyExplicitCommitStatusesAreTerminal() {
        for (String status : List.of("Success", "success", "Publish Timeout", "publish timeout")) {
            var response = AuditStreamLoader.parseResponse(200, "{\"Status\":\"" + status + "\"}");
            Assertions.assertTrue(response.committed());
        }
        for (String body : List.of("{}", "{\"Status\":42}", "{\"Status\":\"Fail\",\"Message\":\"Publish Timeout\"}",
                "{\"Status\":\"Label Already Exists\"}", "{\"Status\":\"unknown\"}")) {
            Assertions.assertFalse(AuditStreamLoader.parseResponse(200, body).committed());
        }
        Assertions.assertFalse(AuditStreamLoader.parseResponse(503, "{\"Status\":\"Publish Timeout\"}").committed());
    }

    @Test
    public void testMalformedCountsAreDistinctFromMissingCounts() {
        for (String value : List.of("\"20\"", "1.5", "1e40", "true", "[]", "{}")) {
            var response = AuditStreamLoader.parseResponse(200,
                    "{\"Status\":\"Success\",\"NumberLoadedRows\":" + value + ",\"NumberFilteredRows\":1}");
            Assertions.assertTrue(response.committed());
            Assertions.assertTrue(response.stats().invalidCounts());
        }
        var response = AuditStreamLoader.parseResponse(200,
                "{\"Status\":\"Publish Timeout\",\"NumberLoadedRows\":null,\"NumberFilteredRows\":1,\"ErrorURL\":\"error-url\"}");
        Assertions.assertTrue(response.committed());
        Assertions.assertFalse(response.stats().invalidCounts());
        Assertions.assertNull(response.stats().loadedRows());
        Assertions.assertEquals(1L, response.stats().filteredRows());
        Assertions.assertEquals("error-url", response.stats().errorUrl());
        Assertions.assertThrows(RuntimeException.class, () -> AuditStreamLoader.parseResponse(200, "invalid JSON"));
    }
}
