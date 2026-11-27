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

import com.starrocks.common.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSServiceThrottledException;
import org.apache.hadoop.fs.s3a.AWSStatus500Exception;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs a real S3AFileSystem against an in-process S3 endpoint, so the S3A internals are those of our Hadoop. */
public class DeltaLakeS3LogStoreTest {
    private static final String BUCKET = "bucket";
    private static final String LOG_PREFIX = "table/_delta_log/";
    private static final long VERSION = 712884;
    private static final String VERSION_PREFIX = "00000000000000712884";

    private FakeS3 s3;

    @BeforeEach
    public void startS3() throws IOException {
        s3 = new FakeS3();
    }

    @AfterEach
    public void stopS3() throws IOException {
        // The filesystem cache keys on scheme and bucket only, so every test needs its own instances.
        FileSystem.closeAll();
        s3.close();
        Config.enable_delta_lake_s3_start_after_listing = true;
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3", "s3a"})
    public void testListingStartsAtRequestedVersion(String scheme) throws IOException {
        // Every file at the requested version (including checkpoint parts), checksums and later commits, in
        // lexical order; nothing before it.
        String checkpoint = VERSION_PREFIX + ".checkpoint.0000000001.0000000002.parquet";
        String checkpointPart2 = VERSION_PREFIX + ".checkpoint.0000000002.0000000002.parquet";
        String checksum = VERSION_PREFIX + ".crc";
        String commit = VERSION_PREFIX + ".json";
        String laterCommit = "00000000000000712902.json";
        s3.putLog("00000000000000712883.json", "00000000000000712883.crc", checkpoint, checkpointPart2, checksum,
                commit, laterCommit, "_last_checkpoint");
        Configuration conf = s3Conf(scheme);
        // Two keys per page, so the start-after must survive continuation.
        conf.setInt("fs.s3a.paging.maximum", 2);
        Path logDir = new Path(scheme + "://" + BUCKET + "/table/_delta_log");

        DeltaLakeEngine engine = newEngine(conf);
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient()
                .listFrom(startFrom(logDir, VERSION).toString())) {
            List<FileStatus> listed = files.toInMemoryList();
            assertEquals(List.of(checkpoint, checkpointPart2, checksum, commit, laterCommit, "_last_checkpoint"),
                    listed.stream().map(file -> new Path(file.getPath()).getName()).toList());
            assertEquals(new Path(logDir, checkpoint).toString(), listed.get(0).getPath());
            assertEquals(FakeS3.SIZE, listed.get(0).getSize());
        }

        Map<String, String> first = s3.listRequests.get(0);
        assertEquals(LOG_PREFIX, first.get("prefix"));
        assertEquals("/", first.get("delimiter"));
        assertEquals(LOG_PREFIX + VERSION_PREFIX + "-", first.get("start-after"));
        assertEquals("2", first.get("list-type"));
        assertTrue(s3.listRequests.size() > 1, "the listing should have paged");
        assertEquals(0, s3.fullListings(), "must not list all of _delta_log");
        assertEquals(0, s3.headRequests.get(), "a non-empty listing needs no existence check");
    }

    @Test
    public void testStagedCommitsAreNotListedAsCommits() throws IOException {
        // Catalog-managed commits that are not published yet, named like Kernel's UUID commit files.
        String staged = "_staged_commits/00000000000000712885.3c5b3c0e-6f2b-4a5e-9a8e-1a2b3c4d5e6f.json";
        String commit = VERSION_PREFIX + ".json";
        s3.putLog(commit, staged, "_sidecars/00000000000000712884.checkpoint.abc.parquet");
        Configuration conf = s3Conf("s3a");
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        List<org.apache.hadoop.fs.FileStatus> listed = list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf);
        // Subdirectories come back as single entries, as in a full listing; Kernel ignores them by name.
        assertEquals(List.of(commit, "_sidecars", "_staged_commits"),
                listed.stream().map(file -> file.getPath().getName()).toList());
        assertTrue(listed.get(1).isDirectory());
        assertTrue(listed.get(2).isDirectory());
        assertTrue(FileNames.isCommitFile(new Path(LOG_PREFIX + staged).getName()),
                "Kernel would read the staged commit as a commit if the listing returned it");
    }

    @Test
    public void testMissingLogDirectoryIsReported() throws IOException {
        Configuration conf = s3Conf("s3a");
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        FileNotFoundException thrown = assertThrows(FileNotFoundException.class,
                () -> list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf));
        assertTrue(thrown.getMessage().contains(logDir.toString()), thrown.getMessage());
        assertEquals(0, s3.fullListings());
    }

    @Test
    public void testExistingLogDirectoryWithoutLaterFilesIsEmpty() throws IOException {
        s3.putLog("00000000000000712883.json");
        Configuration conf = s3Conf("s3a");
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        assertTrue(list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf).isEmpty());
        assertEquals(0, s3.fullListings(), "only the existence check may list without start-after");
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 501})
    public void testRejectedStartAfterFallsBackToFullListing(int status) throws IOException {
        // Some S3-compatible stores do not accept start-after.
        s3.startAfterStatus = status;
        s3.putLog("00000000000000712883.json", VERSION_PREFIX + ".json", "_staged_commits/00000000000000712885.a.json");
        Configuration conf = s3Conf("s3a");
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        assertEquals(List.of(VERSION_PREFIX + ".json", "_staged_commits"),
                names(list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf)));
        assertTrue(s3.fullListings() > 0);
    }

    @ParameterizedTest
    @ValueSource(ints = {403, 500, 503})
    public void testFailureTheFullListingWouldRepeatIsRethrown(int status) throws IOException {
        s3.startAfterStatus = status;
        s3.putLog(VERSION_PREFIX + ".json");
        Configuration conf = s3Conf("s3a");
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        IOException thrown = assertThrows(IOException.class,
                () -> list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf));
        Class<? extends IOException> expected = switch (status) {
            case 403 -> AccessDeniedException.class;
            case 500 -> AWSStatus500Exception.class;
            default -> AWSServiceThrottledException.class;
        };
        assertInstanceOf(expected, thrown);
        assertEquals(0, s3.fullListings());
    }

    @Test
    public void testDisabledByConfig() throws IOException {
        s3.putLog("00000000000000712883.json", VERSION_PREFIX + ".json");
        Configuration conf = s3Conf("s3a");
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");
        Config.enable_delta_lake_s3_start_after_listing = false;

        // Registration does not depend on the config, so it can be switched either way at runtime.
        Configuration catalogConf = new Configuration(false);
        DeltaLakeS3LogStore.registerAsDefault(catalogConf);
        assertEquals(DeltaLakeS3LogStore.class.getName(), catalogConf.get("io.delta.kernel.logStore.s3a.impl"));

        assertEquals(List.of(VERSION_PREFIX + ".json"), names(list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf)));
        assertTrue(s3.fullListings() > 0);
        assertEquals(0, s3.startAfterListings());
    }

    @Test
    public void testListV1UsesFullListing() throws IOException {
        s3.putLog(VERSION_PREFIX + ".json");
        Configuration conf = s3Conf("s3a");
        conf.setInt("fs.s3a.list.version", 1);
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        assertEquals(List.of(VERSION_PREFIX + ".json"), names(list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf)));
        assertEquals(0, s3.startAfterListings());
    }

    @Test
    public void testStockFastListFlagDoesNotListRecursively() throws IOException {
        s3.startAfterStatus = 400;
        s3.putLog(VERSION_PREFIX + ".json", "_staged_commits/00000000000000712885.a.json");
        Configuration conf = s3Conf("s3a");
        // Would make the inherited listing use Delta's recursive start-after listing.
        conf.setBoolean("delta.enableFastS3AListFrom", true);
        Path logDir = new Path("s3a://" + BUCKET + "/table/_delta_log");

        assertEquals(List.of(VERSION_PREFIX + ".json", "_staged_commits"),
                names(list(new DeltaLakeS3LogStore(conf), logDir, VERSION, conf)));
        assertTrue(s3.listRequests.stream().allMatch(request -> "/".equals(request.get("delimiter"))));
    }

    @Test
    public void testNonS3AFileSystemUsesFullListing(@TempDir java.nio.file.Path tableDir) throws IOException {
        java.nio.file.Path logDir = Files.createDirectory(tableDir.resolve("_delta_log"));
        Files.writeString(logDir.resolve("00000000000000712883.json"), "{}\n");
        Files.writeString(logDir.resolve(VERSION_PREFIX + ".json"), "{}\n");
        Configuration conf = new Configuration();

        assertEquals(List.of(VERSION_PREFIX + ".json"),
                names(list(new DeltaLakeS3LogStore(conf), new Path(logDir.toUri()), VERSION, conf)));
        assertThrows(FileNotFoundException.class, () -> list(new DeltaLakeS3LogStore(conf),
                new Path(tableDir.resolve("missing").toUri()), VERSION, conf));
    }

    @Test
    public void testKeyBefore() {
        assertEquals("_delta_log/00000000000000712884-", DeltaLakeS3LogStore.keyBefore("_delta_log/00000000000000712884."));
        assertEquals("_delta_log/00000000000000712884.jsom",
                DeltaLakeS3LogStore.keyBefore("_delta_log/00000000000000712884.json"));
        // A non-ASCII last character starts at the first key; the name filter trims the rest.
        assertNull(DeltaLakeS3LogStore.keyBefore("_delta_log/caf" + (char) 0xE9));
    }

    @Test
    public void testMetastoreRegistersForS3Schemes() {
        Configuration conf = new Configuration(false);
        new HMSBackedDeltaMetastore("delta0", null, conf, new DeltaLakeCatalogProperties(Map.of()));
        // Per-table configurations are copies of the catalog configuration.
        Configuration perTable = new Configuration(conf);
        for (String scheme : List.of("s3", "s3a", "s3n")) {
            String key = "io.delta.kernel.logStore." + scheme + ".impl";
            assertEquals(DeltaLakeS3LogStore.class.getName(), conf.get(key));
            assertEquals(DeltaLakeS3LogStore.class.getName(), perTable.get(key));
        }
        assertNull(conf.get("io.delta.kernel.logStore.abfss.impl"));
        assertNull(conf.get("io.delta.kernel.logStore.gs.impl"));
    }

    @Test
    public void testCustomS3LogStoreIsPreserved() {
        Configuration conf = new Configuration(false);
        conf.set("io.delta.kernel.logStore.s3a.impl", "com.example.CustomLogStore");
        DeltaLakeS3LogStore.registerAsDefault(conf);
        assertEquals("com.example.CustomLogStore", conf.get("io.delta.kernel.logStore.s3a.impl"));
        assertEquals(DeltaLakeS3LogStore.class.getName(), conf.get("io.delta.kernel.logStore.s3.impl"));
    }

    private Configuration s3Conf(String scheme) {
        Configuration conf = new Configuration();
        conf.set("fs." + scheme + ".impl", S3AFileSystem.class.getName());
        conf.set("fs.s3a.endpoint", "http://127.0.0.1:" + s3.port());
        conf.set("fs.s3a.endpoint.region", "us-east-1");
        conf.setBoolean("fs.s3a.path.style.access", true);
        conf.setBoolean("fs.s3a.connection.ssl.enabled", false);
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        conf.set("fs.s3a.access.key", "test-access-key");
        conf.set("fs.s3a.secret.key", "test-secret-key");
        conf.setInt("fs.s3a.bucket.probe", 0);
        // Fail fast on injected errors instead of retrying them.
        conf.setInt("fs.s3a.attempts.maximum", 0);
        conf.setInt("fs.s3a.retry.limit", 0);
        conf.set("fs.s3a.retry.interval", "1ms");
        conf.setInt("fs.s3a.retry.throttle.limit", 0);
        conf.set("fs.s3a.retry.throttle.interval", "1ms");
        return conf;
    }

    private static DeltaLakeEngine newEngine(Configuration conf) {
        DeltaLakeS3LogStore.registerAsDefault(conf);
        return DeltaLakeEngine.create(conf, new DeltaLakeCatalogProperties(Map.of()), null, null);
    }

    private static List<org.apache.hadoop.fs.FileStatus> list(DeltaLakeS3LogStore logStore, Path logDir, long version,
                                                             Configuration conf) throws IOException {
        List<org.apache.hadoop.fs.FileStatus> listed = new ArrayList<>();
        Iterator<org.apache.hadoop.fs.FileStatus> files = logStore.listFrom(startFrom(logDir, version), conf);
        files.forEachRemaining(listed::add);
        return listed;
    }

    private static List<String> names(List<org.apache.hadoop.fs.FileStatus> files) {
        return files.stream().map(file -> file.getPath().getName()).toList();
    }

    /** The listing start Kernel requests for {@code version}, e.g. {@code _delta_log/00000000000000712884.}. */
    private static Path startFrom(Path logDir, long version) {
        return new Path(FileNames.listingPrefix(new io.delta.kernel.internal.fs.Path(logDir.toString()), version));
    }

    /**
     * The parts of the S3 API S3A uses for listings and existence checks: HEAD object and ListObjects (V1 and
     * V2, with prefix, delimiter, start-after, max-keys and continuation). Objects live in one bucket.
     */
    private static final class FakeS3 implements AutoCloseable {
        static final long SIZE = 123;
        private static final String LAST_MODIFIED = "2026-11-23T10:00:00.000Z";

        private final HttpServer server;
        private final NavigableSet<String> keys = Collections.synchronizedNavigableSet(new TreeSet<>());
        final List<Map<String, String>> listRequests = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger headRequests = new AtomicInteger();
        // When set, listings with start-after are answered with this status.
        volatile Integer startAfterStatus;

        FakeS3() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        int port() {
            return server.getAddress().getPort();
        }

        void putLog(String... names) {
            for (String name : names) {
                keys.add(LOG_PREFIX + name);
            }
        }

        /** Listings of _delta_log itself without start-after, i.e. not an existence probe of at most 2 keys. */
        long fullListings() {
            return snapshot().stream()
                    .filter(request -> !request.containsKey("start-after") && LOG_PREFIX.equals(request.get("prefix")))
                    .filter(request -> Integer.parseInt(request.getOrDefault("max-keys", "1000")) > 2)
                    .count();
        }

        long startAfterListings() {
            return snapshot().stream().filter(request -> request.containsKey("start-after")).count();
        }

        private List<Map<String, String>> snapshot() {
            synchronized (listRequests) {
                return new ArrayList<>(listRequests);
            }
        }

        private void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                String path = exchange.getRequestURI().getRawPath();
                if (!path.startsWith("/" + BUCKET)) {
                    send(exchange, 404, error("NoSuchBucket"));
                    return;
                }
                String key = URLDecoder.decode(path.substring(Math.min(path.length(), BUCKET.length() + 2)),
                        StandardCharsets.UTF_8);
                if (exchange.getRequestMethod().equals("HEAD")) {
                    headRequests.incrementAndGet();
                    if (keys.contains(key)) {
                        exchange.getResponseHeaders().add("Last-Modified", "Mon, 23 Nov 2026 10:00:00 GMT");
                        exchange.getResponseHeaders().add("ETag", "\"etag\"");
                        exchange.getResponseHeaders().add("Content-Length", Long.toString(SIZE));
                        exchange.sendResponseHeaders(200, -1);
                    } else {
                        exchange.sendResponseHeaders(404, -1);
                    }
                    return;
                }
                if (exchange.getRequestMethod().equals("GET") && key.isEmpty()) {
                    list(exchange, query(exchange.getRequestURI().getRawQuery()));
                    return;
                }
                send(exchange, 501, error("NotImplemented"));
            }
        }

        private void list(HttpExchange exchange, Map<String, String> query) throws IOException {
            listRequests.add(query);
            if (startAfterStatus != null && query.containsKey("start-after")) {
                send(exchange, startAfterStatus, error("InjectedFailure"));
                return;
            }
            String prefix = query.getOrDefault("prefix", "");
            String delimiter = query.get("delimiter");
            int maxKeys = Integer.parseInt(query.getOrDefault("max-keys", "1000"));
            boolean v2 = "2".equals(query.get("list-type"));
            String after = v2 ? query.getOrDefault("continuation-token", query.get("start-after")) : query.get("marker");

            // Keys and common prefixes in lexical order, as S3 returns them.
            Set<String> entries = new LinkedHashSet<>();
            for (String key : keys.tailSet(prefix, true)) {
                if (!key.startsWith(prefix)) {
                    break;
                }
                if (after != null && key.compareTo(after) <= 0) {
                    continue;
                }
                int slash = delimiter == null ? -1 : key.indexOf(delimiter, prefix.length());
                String entry = slash < 0 ? key : key.substring(0, slash + 1);
                if (after != null && entry.endsWith("/") && after.startsWith(entry)) {
                    continue;
                }
                entries.add(entry);
            }
            List<String> page = new ArrayList<>(entries).subList(0, Math.min(maxKeys, entries.size()));
            boolean truncated = page.size() < entries.size();

            StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
                    .append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
                    .append("<Name>").append(BUCKET).append("</Name>")
                    .append("<Prefix>").append(prefix).append("</Prefix>")
                    .append("<MaxKeys>").append(maxKeys).append("</MaxKeys>")
                    .append("<IsTruncated>").append(truncated).append("</IsTruncated>");
            if (delimiter != null) {
                xml.append("<Delimiter>").append(delimiter).append("</Delimiter>");
            }
            if (v2) {
                xml.append("<KeyCount>").append(page.size()).append("</KeyCount>");
                if (truncated) {
                    xml.append("<NextContinuationToken>").append(page.get(page.size() - 1))
                            .append("</NextContinuationToken>");
                }
            } else if (truncated) {
                xml.append("<NextMarker>").append(page.get(page.size() - 1)).append("</NextMarker>");
            }
            for (String entry : page) {
                if (entry.endsWith("/")) {
                    xml.append("<CommonPrefixes><Prefix>").append(entry).append("</Prefix></CommonPrefixes>");
                } else {
                    xml.append("<Contents><Key>").append(entry).append("</Key>")
                            .append("<LastModified>").append(LAST_MODIFIED).append("</LastModified>")
                            .append("<ETag>\"etag\"</ETag><Size>").append(SIZE).append("</Size>")
                            .append("<StorageClass>STANDARD</StorageClass></Contents>");
                }
            }
            send(exchange, 200, xml.append("</ListBucketResult>").toString());
        }

        private static Map<String, String> query(String rawQuery) {
            Map<String, String> query = new HashMap<>();
            if (rawQuery == null) {
                return query;
            }
            for (String part : rawQuery.split("&")) {
                int eq = part.indexOf('=');
                String name = URLDecoder.decode(eq < 0 ? part : part.substring(0, eq), StandardCharsets.UTF_8);
                String value = eq < 0 ? "" : URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
                query.put(name, value);
            }
            return query;
        }

        private static String error(String code) {
            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code + "</Code><Message>" + code +
                    "</Message><RequestId>test</RequestId></Error>";
        }

        private static void send(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/xml");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
