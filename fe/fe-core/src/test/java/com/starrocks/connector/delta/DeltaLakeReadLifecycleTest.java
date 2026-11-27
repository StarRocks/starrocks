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

import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.ScanBuilderImpl;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DeltaLakeReadLifecycleTest {
    @TempDir
    java.nio.file.Path directory;
    private Configuration conf;
    private String authority;
    private GuardedFileSystem fs;

    public static class GuardedFileSystem extends RawLocalFileSystem {
        private URI uri;
        boolean closed;
        boolean failOpen;
        boolean failRead;
        boolean failClose;
        int openStreams;
        CountDownLatch openStarted;
        CountDownLatch allowOpen;

        @Override
        public void initialize(URI uri, Configuration conf) throws IOException {
            super.initialize(uri, conf);
            this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority() + "/");
        }

        @Override
        public URI getUri() {
            return uri == null ? super.getUri() : uri;
        }

        @Override
        public File pathToFile(Path path) {
            return new File(path.toUri().getPath());
        }

        private void checkAccess() throws IOException {
            HadoopExt ext = HadoopExt.getInstance();
            if (closed || !ext.hasActiveFileSystemAccess(
                    ext.getCloudConfString(getConf()), ext.getCredentialFingerprint(getConf()))) {
                throw new IOException("FileSystem used without an active access scope");
            }
        }

        @Override
        public org.apache.hadoop.fs.FileStatus getFileStatus(Path path) throws IOException {
            checkAccess();
            return super.getFileStatus(path);
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize) throws IOException {
            checkAccess();
            if (openStarted != null) {
                openStarted.countDown();
                try {
                    if (!allowOpen.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("timed out waiting to open");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            if (failOpen) {
                throw new IOException("open failed");
            }
            FSDataInputStream delegate = super.open(path, bufferSize);
            openStreams++;
            return new FSDataInputStream(new FSInputStream() {
                boolean streamClosed;

                private void checkRead() throws IOException {
                    checkAccess();
                    if (failRead) {
                        throw new IOException("read failed");
                    }
                }

                @Override
                public void seek(long position) throws IOException {
                    checkRead();
                    delegate.seek(position);
                }

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public boolean seekToNewSource(long position) {
                    return false;
                }

                @Override
                public int read() throws IOException {
                    checkRead();
                    return delegate.read();
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    checkRead();
                    return delegate.read(bytes, offset, length);
                }

                @Override
                public void close() throws IOException {
                    if (!streamClosed) {
                        checkAccess();
                        streamClosed = true;
                        openStreams--;
                        delegate.close();
                        if (failClose) {
                            throw new IOException("close failed");
                        }
                    }
                }
            });
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        conf = new Configuration(false);
        authority = UUID.randomUUID().toString();
        conf.setClass("fs.readtest.impl", GuardedFileSystem.class, FileSystem.class);
        conf.set(HadoopExt.HADOOP_CLOUD_CONFIGURATION_STRING, "cloud-" + authority);
        conf.set(HadoopExt.HADOOP_DYNAMIC_CREDENTIAL_PREFIX + "delta.azure", "digest");
        conf.setInt("delta.kernel.default.json.reader.batch-size", 1);
        conf.setInt("delta.kernel.default.parquet.reader.batch-size", 1);
        fs = (GuardedFileSystem) FileSystem.get(URI.create("readtest://" + authority + "/"), conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        fs.close();
        Assertions.assertFalse(active(), "all access scopes must be released");
        Assertions.assertEquals(0, fs.openStreams, "all streams must be closed");
    }

    private boolean active() {
        HadoopExt ext = HadoopExt.getInstance();
        return ext.hasActiveFileSystemAccess(ext.getCloudConfString(conf), ext.getCredentialFingerprint(conf));
    }

    private String path(java.nio.file.Path localPath) {
        return "readtest://" + authority + localPath.toAbsolutePath();
    }

    private String json(String content) throws IOException {
        return path(Files.writeString(directory.resolve("log.json"), content));
    }

    private DeltaLakeEngine engine(boolean cached) {
        Map<String, String> properties = Map.of(
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, Boolean.toString(cached),
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, Boolean.toString(cached));
        return DeltaLakeEngine.create(conf, new DeltaLakeCatalogProperties(properties),
                CacheBuilder.newBuilder().build(), CacheBuilder.newBuilder().build());
    }

    @Test
    public void testClosedUnusedIteratorDoesNotRegisterAccess() throws Exception {
        try (CloseableIterator<ColumnarBatch> rows = engine(false).withFileSystemAccess(
                engine(false).getJsonHandler().readJsonFiles(
                    Utils.singletonCloseableIterator(FileStatus.of(json("{}\n"), 3, 0)),
                    new StructType(), Optional.empty()))) {
            Assertions.assertFalse(active());
            rows.close();
            Assertions.assertFalse(rows.hasNext());
        }
    }

    @Test
    public void testOpenAndCloseFailuresReleaseAccess() throws Exception {
        String path = json("{}\n");
        fs.failOpen = true;
        try (CloseableIterator<ColumnarBatch> rows = scopedJsonRows(false, path, 3)) {
            Assertions.assertThrows(RuntimeException.class, rows::hasNext);
            Assertions.assertFalse(active());
        }
        fs.failOpen = false;
        CloseableIterator<ColumnarBatch> rows = scopedJsonRows(false, path, 3);
        Assertions.assertTrue(rows.hasNext());
        fs.failClose = true;
        Assertions.assertThrows(RuntimeException.class, rows::close);
        Assertions.assertFalse(active());
        rows.close();
    }

    private CloseableIterator<ColumnarBatch> scopedJsonRows(boolean cached, String path, long size) throws IOException {
        DeltaLakeEngine engine = engine(cached);
        return engine.withFileSystemAccess(engine.getJsonHandler().readJsonFiles(
                Utils.singletonCloseableIterator(FileStatus.of(path, size, 0)), new StructType(), Optional.empty()));
    }

    @Test
    public void testJsonReadersWithAndWithoutCache() throws Exception {
        String path = json("{\"value\":1}\n".repeat(20000));
        for (boolean cached : new boolean[] {false, true}) {
            try (CloseableIterator<ColumnarBatch> rows = engine(false).withFileSystemAccess(
                    engine(cached).getJsonHandler().readJsonFiles(
                        Utils.singletonCloseableIterator(FileStatus.of(path, 240000, 0)),
                        new StructType(), Optional.empty()))) {
                int count = 0;
                while (rows.hasNext()) {
                    count += rows.next().getSize();
                }
                Assertions.assertEquals(20000, count);
                Assertions.assertFalse(active());
                Assertions.assertEquals(0, fs.openStreams);
            }
        }
    }

    @Test
    public void testJsonCancellationAndParseFailureReleaseAccess() throws Exception {
        String path = json("{\"value\":1}\n".repeat(2000));
        CloseableIterator<ColumnarBatch> rows = engine(false).withFileSystemAccess(
                engine(false).getJsonHandler().readJsonFiles(
                    Utils.singletonCloseableIterator(FileStatus.of(path, 24000, 0)), new StructType(), Optional.empty()));
        Assertions.assertTrue(rows.hasNext());
        rows.next();
        Assertions.assertTrue(active());
        rows.close();
        rows.close();
        Assertions.assertFalse(active());
        String malformed = json("not-json\n");
        for (boolean cached : new boolean[] {false, true}) {
            try (CloseableIterator<ColumnarBatch> invalid = engine(false).withFileSystemAccess(
                    engine(cached).getJsonHandler().readJsonFiles(
                        Utils.singletonCloseableIterator(FileStatus.of(malformed, 9, 0)),
                        new StructType(), Optional.empty()))) {
                Assertions.assertThrows(RuntimeException.class, () -> {
                    invalid.hasNext();
                    invalid.next();
                });
                Assertions.assertFalse(active());
                Assertions.assertEquals(0, fs.openStreams);
            }
        }
    }

    @Test
    public void testParquetReadersWithAndWithoutCache() throws Exception {
        java.nio.file.Path local = java.nio.file.Path.of(getClass().getClassLoader().getResource(
                "connector/deltalake/00000000000000000030.checkpoint.parquet").toURI());
        FileStatus status = FileStatus.of(path(local), Files.size(local), 0);
        int expectedRows = -1;
        for (boolean cached : new boolean[] {false, true}) {
            try (CloseableIterator<FileReadResult> rows = engine(false).withFileSystemAccess(
                    engine(cached).getParquetHandler().readParquetFiles(
                        Utils.singletonCloseableIterator(status), LogReplay.getAddRemoveReadSchema(true), Optional.empty()))) {
                int count = 0;
                while (rows.hasNext()) {
                    count += rows.next().getData().getSize();
                }
                Assertions.assertTrue(count > 0);
                if (expectedRows >= 0) {
                    Assertions.assertEquals(expectedRows, count);
                }
                expectedRows = count;
                Assertions.assertFalse(active());
                Assertions.assertEquals(0, fs.openStreams);
            }
        }
    }

    @Test
    public void testSnapshotAndPartitionOperationsReleaseAccess() throws Exception {
        java.nio.file.Path log = Files.createDirectories(directory.resolve("table/_delta_log"));
        String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"p\",\"type\":\"string\","
                + "\"nullable\":true,\"metadata\":{}}]}";
        String contents = "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
                + "{\"metaData\":{\"id\":\"test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                + "\"schemaString\":" + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(schema)
                + ",\"partitionColumns\":[\"p\"],\"configuration\":{},\"createdTime\":0}}\n"
                + "{\"add\":{\"path\":\"p=a/data.parquet\",\"partitionValues\":{\"p\":\"a\"},"
                + "\"size\":1,\"modificationTime\":0,\"dataChange\":true}}\n";
        Files.writeString(log.resolve("00000000000000000000.json"), contents);
        IHiveMetastore delegate = Mockito.mock(IHiveMetastore.class);
        Mockito.when(delegate.getMetastoreTable("db", "table")).thenReturn(
                new MetastoreTable("db", "table", path(directory.resolve("table")), 0));
        for (boolean cached : new boolean[] {false, true}) {
            HMSBackedDeltaMetastore metastore = Mockito.spy(new HMSBackedDeltaMetastore(
                    "lifecycle-" + authority + cached, delegate, conf,
                    new DeltaLakeCatalogProperties(Map.of(
                            DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, Boolean.toString(cached)))));
            try {
                DeltaLakeSnapshot first = metastore.getLatestSnapshot("db", "table");
                Assertions.assertNotNull(first);
                Assertions.assertFalse(active(), "cached snapshot must not hold an access scope");
                Assertions.assertEquals(List.of("p=a"), metastore.getPartitionKeys("db", "table"));
                Assertions.assertFalse(active());
                fs.close();
                Mockito.doReturn(first).when(metastore).getLatestSnapshot("db", "table");
                Assertions.assertEquals(List.of("p=a"), metastore.getPartitionKeys("db", "table"));
                Assertions.assertFalse(active());
                Mockito.doCallRealMethod().when(metastore).getLatestSnapshot("db", "table");
                Assertions.assertNotNull(metastore.getLatestSnapshot("db", "table"));
                fs = (GuardedFileSystem) FileSystem.get(URI.create(path(directory.resolve("table"))), conf);
                Assertions.assertFalse(fs.closed);
                Assertions.assertFalse(active());
                fs.failRead = true;
                metastore.invalidateAll();
                Assertions.assertThrows(StarRocksConnectorException.class,
                        () -> metastore.getLatestSnapshot("db", "table"));
                fs.failRead = false;
                Assertions.assertFalse(active());
                Assertions.assertEquals(0, fs.openStreams);
            } finally {
                metastore.releaseCaches();
            }
        }
    }

    @Test
    public void testIteratorFailurePreservesCauseAndClosesOnce() throws Exception {
        RuntimeException failure = new IllegalStateException("iteration failed");
        int[] closes = {0};
        CloseableIterator<Object> iterator = engine(false).withFileSystemAccess(new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                throw failure;
            }

            @Override
            public Object next() {
                throw failure;
            }

            @Override
            public void close() throws IOException {
                closes[0]++;
                throw new IOException("close failed");
            }
        });
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, iterator::hasNext));
        Assertions.assertEquals(1, failure.getSuppressed().length);
        iterator.close();
        Assertions.assertFalse(iterator.hasNext());
        Assertions.assertEquals(1, closes[0]);
    }

    @Test
    public void testCancellationWaitsForInFlightOpenAndReleasesReader() throws Exception {
        String path = json("{}\n".repeat(10000));
        fs.openStarted = new CountDownLatch(1);
        fs.allowOpen = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CloseableIterator<ColumnarBatch> rows = engine(false).withFileSystemAccess(
                engine(false).getJsonHandler().readJsonFiles(
                    Utils.singletonCloseableIterator(FileStatus.of(path, 30000, 0)), new StructType(), Optional.empty()));
        try {
            Future<Boolean> opening = executor.submit(rows::hasNext);
            Assertions.assertTrue(fs.openStarted.await(10, TimeUnit.SECONDS));
            CountDownLatch cancelStarted = new CountDownLatch(1);
            Future<?> cancel = executor.submit(() -> {
                cancelStarted.countDown();
                rows.close();
                return null;
            });
            Assertions.assertTrue(cancelStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> cancel.get(100, TimeUnit.MILLISECONDS),
                    "close must not complete while a reader is still being opened");
            fs.allowOpen.countDown();
            Assertions.assertTrue(opening.get(10, TimeUnit.SECONDS));
            cancel.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(active());
            Assertions.assertEquals(0, fs.openStreams);
            Assertions.assertFalse(rows.hasNext());
            rows.close();
        } finally {
            fs.allowOpen.countDown();
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            rows.close();
        }
    }

    @Test
    public void testScanCancellationBeforeKernelIteratorIsPublished() throws Exception {
        String path = json("{}\n".repeat(10000));
        DeltaLakeEngine engine = engine(false);
        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        ScanBuilderImpl builder = Mockito.mock(ScanBuilderImpl.class);
        ScanImpl scan = Mockito.mock(ScanImpl.class);
        Mockito.when(table.getDeltaEngine()).thenReturn(engine);
        Mockito.when(table.getDeltaSnapshot()).thenReturn(snapshot);
        Mockito.when(table.getDeltaMetadata()).thenReturn(metadata);
        Mockito.when(table.getColumns()).thenReturn(List.of());
        Mockito.when(metadata.getSchema()).thenReturn(new StructType());
        Mockito.when(metadata.getPartitionColNames()).thenReturn(Set.of());
        Mockito.when(snapshot.getScanBuilder()).thenReturn(builder);
        Mockito.when(builder.withFilter(Mockito.any())).thenReturn(builder);
        Mockito.when(builder.build()).thenReturn(scan);
        Mockito.when(scan.getScanFiles(engine, true)).thenAnswer(invocation -> {
            CloseableIterator<ColumnarBatch> rows = engine.getJsonHandler().readJsonFiles(
                    Utils.singletonCloseableIterator(FileStatus.of(path, 30000, 0)),
                    new StructType(), Optional.empty());
            rows.hasNext();
            return rows.map(batch -> new FilteredColumnarBatch(batch, Optional.empty()));
        });
        DeltaLakeMetadata connector = new DeltaLakeMetadata(null, "test", null, null,
                new ConnectorProperties(ConnectorType.DELTALAKE));
        RemoteFileInfoSource source = connector.getRemoteFilesAsync(table, GetRemoteFilesParams.newBuilder().build());
        fs.openStarted = new CountDownLatch(1);
        fs.allowOpen = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> opening = executor.submit(source::hasMoreOutput);
            Assertions.assertTrue(fs.openStarted.await(10, TimeUnit.SECONDS));
            CountDownLatch cancelStarted = new CountDownLatch(1);
            Future<?> cancel = executor.submit(() -> {
                cancelStarted.countDown();
                source.close();
                return null;
            });
            Assertions.assertTrue(cancelStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> cancel.get(100, TimeUnit.MILLISECONDS));
            fs.allowOpen.countDown();
            Assertions.assertTrue(opening.get(10, TimeUnit.SECONDS));
            cancel.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(active());
            Assertions.assertEquals(0, fs.openStreams);
            Assertions.assertFalse(source.hasMoreOutput());
        } finally {
            fs.allowOpen.countDown();
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            source.close();
        }
    }

    private static Field field(Class<?> type, String name) throws Exception {
        Field result = type.getDeclaredField(name);
        result.setAccessible(true);
        return result;
    }

    @Test
    public void testActiveJsonReaderSurvivesRealCacheEviction() throws Exception {
        Object cache = field(FileSystem.class, "CACHE").get(null);
        Method sweep;
        try {
            sweep = cache.getClass().getDeclaredMethod("sweepNowForTesting");
        } catch (NoSuchMethodException e) {
            Assumptions.assumeTrue(false, "Requires hadoop-ext FileSystem ahead of hadoop-common on classpath");
            return;
        }
        sweep.setAccessible(true);
        Field ttl = field(cache.getClass(), "credentialIdleTtlMs");
        long savedTtl = ttl.getLong(null);
        String path = json("{\"value\":1}\n".repeat(20000));
        try (CloseableIterator<ColumnarBatch> rows = engine(false).withFileSystemAccess(
                engine(false).getJsonHandler().readJsonFiles(
                    Utils.singletonCloseableIterator(FileStatus.of(path, 240000, 0)),
                    new StructType(), Optional.empty()))) {
            Assertions.assertTrue(rows.hasNext());
            int count = rows.next().getSize();
            ttl.setLong(null, 0);
            sweep.invoke(cache);
            sweep.invoke(cache);
            Assertions.assertFalse(fs.closed, "active reader must survive both eviction phases");
            while (rows.hasNext()) {
                count += rows.next().getSize();
            }
            Assertions.assertEquals(20000, count);
            Assertions.assertFalse(active());
            sweep.invoke(cache);
            sweep.invoke(cache);
            Assertions.assertTrue(fs.closed, "released reader must not prevent reclamation");
        } finally {
            ttl.setLong(null, savedTtl);
        }
    }

    @Test
    public void testActiveParquetReaderSurvivesRealCacheEviction() throws Exception {
        Object cache = field(FileSystem.class, "CACHE").get(null);
        Method sweep;
        try {
            sweep = cache.getClass().getDeclaredMethod("sweepNowForTesting");
        } catch (NoSuchMethodException e) {
            Assumptions.assumeTrue(false, "Requires hadoop-ext FileSystem ahead of hadoop-common on classpath");
            return;
        }
        sweep.setAccessible(true);
        Field ttl = field(cache.getClass(), "credentialIdleTtlMs");
        long savedTtl = ttl.getLong(null);
        java.nio.file.Path local = java.nio.file.Path.of(getClass().getClassLoader().getResource(
                "connector/deltalake/00000000000000000030.checkpoint.parquet").toURI());
        FileStatus status = FileStatus.of(path(local), Files.size(local), 0);
        try (CloseableIterator<FileReadResult> rows = engine(false).withFileSystemAccess(
                engine(false).getParquetHandler().readParquetFiles(
                    Utils.singletonCloseableIterator(status), LogReplay.getAddRemoveReadSchema(true), Optional.empty()))) {
            Assertions.assertTrue(rows.hasNext());
            Assertions.assertTrue(active());
            ttl.setLong(null, 0);
            sweep.invoke(cache);
            sweep.invoke(cache);
            Assertions.assertFalse(fs.closed);
            int count = 0;
            while (rows.hasNext()) {
                count += rows.next().getData().getSize();
            }
            Assertions.assertTrue(count > 0);
            Assertions.assertFalse(active());
            sweep.invoke(cache);
            sweep.invoke(cache);
            Assertions.assertTrue(fs.closed);
        } finally {
            ttl.setLong(null, savedTtl);
        }
    }

    @Test
    public void testReadFailuresReleaseAccess() throws Exception {
        String path = json("{}\n");
        fs.failRead = true;
        for (boolean cached : new boolean[] {false, true}) {
            try (CloseableIterator<ColumnarBatch> rows = engine(false).withFileSystemAccess(
                    engine(cached).getJsonHandler().readJsonFiles(
                        Utils.singletonCloseableIterator(FileStatus.of(path, 3, 0)), new StructType(), Optional.empty()))) {
                Assertions.assertThrows(RuntimeException.class, rows::hasNext);
                Assertions.assertFalse(active());
                Assertions.assertEquals(0, fs.openStreams);
            }
            try (CloseableIterator<FileReadResult> rows = engine(false).withFileSystemAccess(
                    engine(cached).getParquetHandler().readParquetFiles(
                        Utils.singletonCloseableIterator(FileStatus.of(path, 3, 0)),
                        LogReplay.getAddRemoveReadSchema(true), Optional.empty()))) {
                Assertions.assertThrows(RuntimeException.class, rows::hasNext);
                Assertions.assertFalse(active());
                Assertions.assertEquals(0, fs.openStreams);
            }
        }
    }
}
