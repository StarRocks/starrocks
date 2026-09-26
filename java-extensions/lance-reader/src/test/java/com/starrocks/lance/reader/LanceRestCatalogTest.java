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

package com.starrocks.lance.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.utils.Platform;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.namespace.model.DescribeTableRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceRestCatalogTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir
    public Path temp;
    private HttpServer server;
    private String uri;
    private final List<Throwable> failures = new CopyOnWriteArrayList<>();
    private final List<String> auth = new CopyOnWriteArrayList<>();
    private Handler handler;

    private interface Handler {
        Object handle(HttpExchange exchange) throws Exception;
    }

    @BeforeEach
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/warehouse";
        server.createContext("/warehouse", exchange -> {
            try {
                auth.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
                byte[] body = JSON.writeValueAsBytes(handler.handle(exchange));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (Throwable e) {
                failures.add(e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    @AfterEach
    public void stop() {
        server.stop(0);
        assertTrue(failures.isEmpty(), failures.toString());
    }

    @Test
    public void testPaginatedNestedNamespacesAndTables() throws Exception {
        handler = exchange -> {
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();
            assertEquals("GET", exchange.getRequestMethod());
            if (path.endsWith("/namespace/\u001f/list")) {
                return query.contains("page_token") ? Map.of("namespaces", List.of("empty"))
                        : Map.of("namespaces", List.of("parent"), "page_token", "next /+?");
            }
            if (path.endsWith("/namespace/parent/list")) {
                return Map.of("namespaces", List.of("a.b%"));
            }
            if (path.endsWith("/table/list")) {
                assertTrue(path.endsWith("/namespace/parent\u001fa.b%/table/list"));
                assertTrue(query.contains("include_declared=false"));
                return query.contains("page_token") ? Map.of("tables", List.of("second"))
                        : Map.of("tables", List.of("first"), "page_token", "more /+?");
            }
            return Map.of("namespaces", List.of());
        };
        assertEquals(JSON.readTree("[[],[\"parent\"],[\"empty\"],[\"parent\",\"a.b%\"]]"),
                JSON.readTree(LanceRestCatalog.listNamespaces(uri, "")));
        assertEquals(JSON.readTree("[\"first\",\"second\"]"), JSON.readTree(
                LanceRestCatalog.listTables(uri, "", "[\"parent\",\"a.b%\"]")));
        assertTrue(auth.stream().allMatch("null"::equals));
    }

    @Test
    public void testRejectRepeatedPaginationToken() {
        handler = exchange -> Map.of("namespaces", List.of(), "page_token", "repeat");
        assertTrue(assertThrows(IllegalStateException.class,
                () -> LanceRestCatalog.listNamespaces(uri, "")).getMessage().contains("pagination"));
    }

    @Test
    public void testTokenRotationStorageOptionsAndLocationGuard() throws Exception {
        Path token = temp.resolve("token");
        Files.writeString(token, "first-token\n");
        handler = exchange -> {
            assertEquals("POST", exchange.getRequestMethod());
            assertEquals("/warehouse/v1/table/ns\u001ftable/describe", exchange.getRequestURI().getPath());
            assertTrue(JSON.readTree(exchange.getRequestBody()).path("vend_credentials").asBoolean());
            return Map.of("location", "s3://bucket/data.lance", "storage_options",
                    Map.of("aws_access_key_id", "temporary-key", "aws_session_token", "temporary-token",
                            "expires_at_millis", "9999999999999"));
        };
        var request = new DescribeTableRequest().id(List.of("ns", "table"));
        try (var client = new LanceRestCatalog(uri, token.toString(), request.getId(), "s3://bucket/data.lance")) {
            assertEquals("temporary-token", client.describeTable(request).getStorageOptions().get("aws_session_token"));
            Files.writeString(token, "rotated-token");
            client.describeTable(request);
            assertEquals(List.of("Bearer first-token", "Bearer rotated-token"), auth);
            handler = exchange -> Map.of("location", "s3://bucket/other.lance");
            assertThrows(IllegalStateException.class, () -> client.describeTable(request));
        }
    }

    @Test
    public void testAzureCredentialsAndInvalidDescriptions() throws Exception {
        List<String> id = List.of("ns", "table");
        String location = "abfss://data@example.dfs.core.windows.net/table.lance";
        try (var client = new LanceRestCatalog(uri, "", id, location)) {
            handler = exchange -> Map.of("location", location, "table", "table", "namespace", List.of("ns"),
                    "storage_options", Map.of("azure_storage_account_name", "example",
                            "azure_storage_sas_key", "test-sas", "expires_at_millis", "9999999999999"));
            assertEquals("test-sas", client.describeTable(new DescribeTableRequest().id(id))
                    .getStorageOptions().get("azure_storage_sas_key"));
            handler = exchange -> Map.of("location", location + "?sig=do-not-expose");
            assertFalse(assertThrows(IllegalStateException.class,
                    () -> client.describeTable(new DescribeTableRequest().id(id))).getMessage().contains("do-not-expose"));
            handler = exchange -> Map.of("location", location, "managed_versioning", true);
            assertThrows(IllegalStateException.class, () -> client.describeTable(new DescribeTableRequest().id(id)));
            handler = exchange -> Map.of("location", location, "table", "different");
            assertThrows(IllegalStateException.class, () -> client.describeTable(new DescribeTableRequest().id(id)));
        }
    }

    @Test
    public void testInvalidIdentifiersAndMissingTokenFailClosed() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> LanceRestCatalog.parseIdentifier("[1]"));
        assertThrows(IllegalArgumentException.class, () -> LanceRestCatalog.parseIdentifier("[\"\"]"));
        assertThrows(IllegalArgumentException.class, () -> new LanceRestCatalog("http://user:secret@host", "", List.of(), null));
        assertThrows(IllegalStateException.class,
                () -> LanceRestCatalog.listNamespaces(uri, temp.resolve("missing").toString()));
        assertTrue(auth.isEmpty());
    }

    @Test
    public void testHttpErrorsDoNotExposeResponseBody() throws Exception {
        server.removeContext("/warehouse");
        server.createContext("/warehouse", exchange -> {
            byte[] body = "do-not-expose".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(403, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        Exception error = assertThrows(IllegalStateException.class, () -> LanceRestCatalog.listNamespaces(uri, ""));
        assertTrue(error.getMessage().contains("403"));
        assertFalse(error.toString().contains("do-not-expose"));
        assertEquals(null, error.getCause());
    }

    @Test
    public void testCatalogToNativeScanner() throws Exception {
        String location = temp.resolve("data.lance").toString();
        try (var allocator = new RootAllocator(); var ids = new BigIntVector("id", allocator);
                var bytes = new ByteArrayOutputStream()) {
            ids.allocateNew(7);
            for (int i = 0; i < 7; i++) {
                ids.setSafe(i, i);
            }
            ids.setValueCount(7);
            try (var root = VectorSchemaRoot.of(ids); var writer = new ArrowStreamWriter(root, null, bytes)) {
                root.setRowCount(7);
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            try (var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
                    var dataset = Dataset.write().allocator(allocator).reader(reader).uri(location).maxRowsPerFile(3).execute()) {
                assertEquals(3, dataset.getFragments().size());
            }
        }
        handler = exchange -> Map.of("location", location, "managed_versioning", false, "storage_options", Map.of());
        String metadata = LanceRestCatalog.loadTable(uri, "", "[\"ns\",\"table\"]");
        assertEquals(location, JSON.readTree(metadata).get("location").asText());
        assertTrue(JSON.readTree(metadata).get("schema").asText().contains("id"));
        long version = JSON.readTree(metadata).get("version").asLong();
        // A commit between FE schema discovery and BE open must not change this query's snapshot.
        try (var changed = Dataset.open(location)) {
            changed.delete("id >= 4");
            assertEquals(4, changed.countRows());
            assertTrue(changed.version() > version);
        }
        String previous = System.getProperty(Platform.UT_KEY);
        System.setProperty(Platform.UT_KEY, "true");
        var scanner = new LanceSplitScanner(3, Map.of("required_fields", "id", "lance_dataset_uri", location,
                "lance_split_info", JSON.writeValueAsString(Map.of("catalog_uri", uri, "token_file", "",
                        "table_id", List.of("ns", "table"), "dataset_version", version))));
        try {
            scanner.open();
            int id = 0;
            for (int count : new int[] {3, 3, 1, 0}) {
                scanner.getNextOffHeapChunk();
                try {
                    assertEquals(count, scanner.getOffHeapTable().getNumRows());
                    StringBuilder expected = new StringBuilder();
                    for (int row = 0; row < count; row++) {
                        expected.append("row").append(row).append(": [id:").append(id++).append("]\n");
                    }
                    assertEquals(expected.toString(), scanner.getOffHeapTable().dump(count));
                } finally {
                    scanner.getOffHeapTable().close();
                }
            }
        } finally {
            scanner.close();
            if (previous == null) {
                System.clearProperty(Platform.UT_KEY);
            } else {
                System.setProperty(Platform.UT_KEY, previous);
            }
        }
    }

    @Test
    public void testRejectFragmentSplits() {
        var scanner = new LanceSplitScanner(3, Map.of("required_fields", "id", "lance_dataset_uri", "/missing",
                "lance_split_info", "{\"fragment_ids\":[1]}"));
        assertThrows(IOException.class, scanner::open);
    }
}
