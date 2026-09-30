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

package com.starrocks.connector.delta;

import com.azure.storage.blob.BlobContainerClientBuilder;
import com.google.common.cache.CacheBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureDeltaFileIOTest {
    private static final String ROOT = "abfss://container@account.dfs.core.windows.net/";
    private final byte[] data = new byte[AzureDeltaInputFile.READ_SIZE + 37];
    private final List<String> ranges = new ArrayList<>();
    private final AtomicInteger lists = new AtomicInteger();
    private HttpServer server;
    private AzureDeltaFileIO fileIO;
    private volatile Map<String, byte[]> metadata;
    private volatile String requestedPath;
    private volatile String etag = "\"version-1\"";

    @BeforeEach
    public void setUp() throws IOException {
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::serve);
        server.start();
        fileIO = new AzureDeltaFileIO(ROOT, new Configuration(false), new BlobContainerClientBuilder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort()).containerName("container").buildClient());
    }

    @AfterEach
    public void tearDown() {
        server.stop(0);
    }

    @Test
    public void testRangeReadsSeekEofAndClose() throws IOException {
        SeekableInputStream stream = fileIO.newInputFile(ROOT + "file%20name", data.length).newStream();
        assertEquals(0, ranges.size());
        assertEquals(data[0] & 0xff, stream.read());
        assertEquals("/container/file%20name", requestedPath);
        stream.seek(10);
        assertEquals(data[10] & 0xff, stream.read());
        assertEquals(1, ranges.size());
        stream.seek(AzureDeltaInputFile.READ_SIZE - 5);
        byte[] bytes = new byte[42];
        stream.readFully(bytes, 0, bytes.length);
        assertArrayEquals(java.util.Arrays.copyOfRange(data, data.length - 42, data.length), bytes);
        assertEquals(2, ranges.size());
        assertEquals("bytes=0-1048575", ranges.get(0));
        assertEquals("bytes=1048576-1048612", ranges.get(1));
        assertEquals(data.length, stream.getPos());
        assertEquals(-1, stream.read());
        assertEquals(0, stream.read(bytes, 0, 0));
        assertThrows(EOFException.class, () -> stream.readFully(bytes, 0, 1));
        assertThrows(IOException.class, () -> stream.seek(-1));
        stream.seek(0);
        assertEquals(data[0] & 0xff, stream.read());
        stream.close();
        assertThrows(IOException.class, stream::read);
    }

    @Test
    public void testChangedObjectFailsConsistentRead() throws IOException {
        try (SeekableInputStream stream = fileIO.newInputFile(ROOT + "file", data.length).newStream()) {
            stream.read();
            etag = "\"version-2\"";
            stream.seek(AzureDeltaInputFile.READ_SIZE);
            IOException error = assertThrows(IOException.class, stream::read);
            assertTrue(error.getMessage().contains("412"));
        }
    }

    @Test
    public void testStatUnknownLengthAndMissingFile() throws IOException {
        assertEquals(data.length, fileIO.getFileStatus(ROOT + "file").getSize());
        assertEquals(data.length, fileIO.newInputFile(ROOT + "file", -1).length());
        assertThrows(FileNotFoundException.class, () -> fileIO.getFileStatus(ROOT + "missing"));
    }

    @Test
    public void testPaginatedOrderedListingFiltersPrefixAndEncodesNames() throws IOException {
        try (CloseableIterator<FileStatus> files = fileIO.listFrom(ROOT + "_delta_log/002")) {
            assertTrue(files.hasNext());
            assertEquals(ROOT + "_delta_log/002.json", files.next().getPath());
            assertEquals(ROOT + "_delta_log/003 space.json", files.next().getPath());
            assertFalse(files.hasNext());
        }
        assertEquals(2, lists.get());
    }

    @Test
    public void testCloseStopsPaginationAndCancellationPreservesInterrupt() throws IOException {
        CloseableIterator<FileStatus> files = fileIO.listFrom(ROOT + "_delta_log/001");
        files.close();
        assertFalse(files.hasNext());
        assertEquals(0, lists.get());
        try (SeekableInputStream stream = fileIO.newInputFile(ROOT + "file", data.length).newStream()) {
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedIOException.class, stream::read);
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    public void testScopeReadOnlyAndUriValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> fileIO.newInputFile(ROOT.replace("account.", "other.") + "file", 0));
        assertThrows(IllegalArgumentException.class, () -> fileIO.resolvePath(ROOT + "file?sig=secret"));
        assertThrows(IllegalArgumentException.class,
                () -> AzureDeltaFileIO.parseUri("abfss://container@custom.invalid/file"));
        assertThrows(UnsupportedOperationException.class, () -> fileIO.delete(ROOT + "file"));
        assertThrows(UnsupportedOperationException.class, () -> fileIO.newOutputFile(ROOT + "file"));
        assertThrows(UnsupportedOperationException.class, () -> fileIO.mkdirs(ROOT));
        assertThrows(UnsupportedOperationException.class, () -> fileIO.copyFileAtomically(ROOT, ROOT, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testKernelSnapshotThroughNativeIO(boolean cached) throws IOException {
        String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\","
                + "\"nullable\":true,\"metadata\":{}}]}";
        String log = "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
                + "{\"metaData\":{\"id\":\"test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                + "\"schemaString\":\"" + schema.replace("\"", "\\\"")
                + "\",\"partitionColumns\":[],\"configuration\":{}}}\n"
                + "{\"add\":{\"path\":\"part.parquet\",\"partitionValues\":{},\"size\":123,"
                + "\"modificationTime\":0,\"dataChange\":true}}\n";
        metadata = Map.of("_delta_log/00000000000000000000.json", log.getBytes(StandardCharsets.UTF_8));
        DeltaLakeEngine engine = DeltaLakeEngine.create(fileIO, new DeltaLakeCatalogProperties(Map.of(
                        DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, String.valueOf(cached))),
                CacheBuilder.newBuilder().build(), CacheBuilder.newBuilder().build());
        var snapshot = Table.forPath(engine, ROOT).getLatestSnapshot(engine);
        assertEquals(0, snapshot.getVersion());
        try (CloseableIterator<FilteredColumnarBatch> batches = snapshot.getScanBuilder().build().getScanFiles(engine)) {
            assertTrue(batches.hasNext());
            try (var rows = batches.next().getRows()) {
                assertTrue(rows.hasNext());
                rows.next();
                assertFalse(rows.hasNext());
            }
        }
    }

    private void serve(HttpExchange exchange) throws IOException {
        try {
            String query = exchange.getRequestURI().getRawQuery();
            requestedPath = exchange.getRequestURI().getPath();
            byte[] content = metadata == null ? data : requestedPath.startsWith("/container/")
                    ? metadata.get(requestedPath.substring("/container/".length())) : null;
            exchange.getResponseHeaders().set("ETag", etag);
            exchange.getResponseHeaders().set("Last-Modified", "Wed, 30 Sep 2026 00:00:00 GMT");
            exchange.getResponseHeaders().set("x-ms-request-id", "test-request");
            if (query != null && query.contains("comp=list")) {
                lists.incrementAndGet();
                boolean second = query.contains("marker=next");
                String entries = second ? blob("_delta_log/003 space.json") :
                        blob("_delta_log/001.json") + blob("_delta_log/002.json") +
                                "<BlobPrefix><Name>_delta_log/_sidecars/</Name></BlobPrefix>";
                if (metadata != null) {
                    entries = metadata.entrySet().stream().map(entry -> blob(entry.getKey())
                            .replace("<Content-Length>10</Content-Length>",
                                    "<Content-Length>" + entry.getValue().length + "</Content-Length>"))
                            .collect(java.util.stream.Collectors.joining());
                    second = true;
                }
                String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults " +
                        "ServiceEndpoint=\"http://localhost\" ContainerName=\"container\"><Blobs>" + entries +
                        "</Blobs><NextMarker>" + (second ? "" : "next") + "</NextMarker></EnumerationResults>";
                byte[] body = xml.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } else if (content == null || exchange.getRequestURI().getPath().endsWith("missing")) {
                exchange.sendResponseHeaders(404, -1);
            } else if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Length", String.valueOf(content.length));
                exchange.getResponseHeaders().set("x-ms-blob-type", "BlockBlob");
                exchange.sendResponseHeaders(200, -1);
            } else if (exchange.getRequestHeaders().getFirst("If-Match") != null &&
                    !etag.replace("\"", "").equals(exchange.getRequestHeaders().getFirst("If-Match").replace("\"", ""))) {
                exchange.sendResponseHeaders(412, -1);
            } else {
                String range = exchange.getRequestHeaders().getFirst("x-ms-range");
                if (range == null) {
                    range = exchange.getRequestHeaders().getFirst("Range");
                }
                ranges.add(range);
                String[] offsets = range.substring("bytes=".length()).split("-");
                int start = Integer.parseInt(offsets[0]);
                int end = Integer.parseInt(offsets[1]);
                exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + content.length);
                exchange.sendResponseHeaders(206, end - start + 1);
                exchange.getResponseBody().write(content, start, end - start + 1);
            }
        } finally {
            exchange.close();
        }
    }

    private static String blob(String name) {
        return "<Blob><Name>" + name + "</Name><Properties>" +
                "<Last-Modified>Wed, 30 Sep 2026 00:00:00 GMT</Last-Modified><Etag>etag</Etag>" +
                "<Content-Length>10</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>";
    }
}
