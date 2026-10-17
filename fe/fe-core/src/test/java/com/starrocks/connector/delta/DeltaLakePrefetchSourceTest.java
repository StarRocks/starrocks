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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
public class DeltaLakePrefetchSourceTest {
    private ExecutorService executor;
    private ConnectContext context;

    @BeforeEach
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
        context = new ConnectContext();
        context.setThreadLocalInfo();
        Tracers.register(context);
        Tracers.init(Tracers.Mode.VARS, Tracers.Module.EXTERNAL, true, false);
    }

    @AfterEach
    public void cleanup() throws Exception {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        Tracers.close();
        ConnectContext.remove();
    }

    private static class Files implements RemoteFileInfoSource {
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final CountDownLatch closed = new CountDownLatch(1);
        final int count;

        Files(int count) {
            this.count = count;
        }

        @Override
        public RemoteFileInfo getOutput() {
            return new DeltaRemoteFileInfo(new FileScanTask(FileStatus.of("s3://bucket/" + reads.getAndIncrement(), 10, 0),
                    1, Map.of("p", "value"), null));
        }

        @Override
        public boolean hasMoreOutput() {
            return reads.get() < count;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            closed.countDown();
        }
    }

    @Test
    public void testBoundedReadAheadAndEarlyClose() throws Exception {
        Files files = new Files(1000);
        DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, executor, 2, 100000);
        assertEquals(0, files.reads.get());
        source.getOutput();
        // A barrier after the prefetch task makes this deterministic without sleeps.
        executor.submit(() -> { }).get();
        assertEquals(4, files.reads.get());
        source.close();
        source.close();
        assertTrue(files.closed.await(5, TimeUnit.SECONDS));
        assertEquals(1, files.closes.get());
        assertEquals(4, files.reads.get());
        assertFalse(source.hasMoreOutput());
    }

    @Test
    public void testByteBudgetStopsAfterOneOversizedFile() throws Exception {
        Files files = new Files(1000);
        try (DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, executor, 256, 1)) {
            source.getOutput();
            executor.submit(() -> { }).get();
            assertEquals(2, files.reads.get());
        }
    }

    @Test
    public void testOrderedDeliveryAndSingleCloseAtEof() throws Exception {
        Files files = new Files(11);
        try (DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, executor, 2, 100000)) {
            List<RemoteFileInfo> result = source.getAllOutputs();
            assertEquals(11, result.size());
            for (int i = 0; i < result.size(); i++) {
                assertEquals("s3://bucket/" + i, ((DeltaRemoteFileInfo) result.get(i)).getFileScanTask()
                        .getFileStatus().getPath());
            }
            assertFalse(source.hasMoreOutput());
        }
        assertEquals(1, files.closes.get());
        assertTrue(Tracers.printScopeTimer().contains("DELTA_LAKE.prefetchRead"));
    }

    @Test
    public void testEmptySource() {
        Files files = new Files(0);
        try (DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, executor)) {
            assertFalse(source.hasMoreOutput());
        }
        assertEquals(1, files.closes.get());
    }

    @Test
    public void testReaderFailureReachesConsumer() {
        RuntimeException failure = new IllegalStateException("bad metadata");
        Files files = new Files(1) {
            @Override
            public boolean hasMoreOutput() {
                throw failure;
            }
        };
        try (DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, executor)) {
            StarRocksConnectorException error = assertThrows(StarRocksConnectorException.class, source::hasMoreOutput);
            assertSame(failure, error.getCause());
        }
        assertEquals(1, files.closes.get());
    }

    @Test
    public void testCancelQueuedTaskBeforeReaderStarts() throws Exception {
        Files files = new Files(1);
        AtomicReference<Runnable> queued = new AtomicReference<>();
        CountDownLatch scheduled = new CountDownLatch(1);
        DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, task -> {
            queued.set(task);
            scheduled.countDown();
        });
        CompletableFuture<Boolean> consumer = CompletableFuture.supplyAsync(source::hasMoreOutput);
        assertTrue(scheduled.await(5, TimeUnit.SECONDS));
        source.close();
        assertFalse(consumer.get(5, TimeUnit.SECONDS));
        queued.get().run();
        assertEquals(0, files.reads.get());
        assertEquals(1, files.closes.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCancelBlockedReaderClosesWithoutWaitingForIo(boolean reject) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        Files files = new Files(1) {
            @Override
            public boolean hasMoreOutput() {
                entered.countDown();
                try {
                    new CountDownLatch(1).await();
                    return false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
        DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, reject ? task -> {
            throw new RejectedExecutionException();
        } : executor);
        CompletableFuture<Boolean> consumer = CompletableFuture.supplyAsync(source::hasMoreOutput);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        source.close();
        assertFalse(consumer.get(5, TimeUnit.SECONDS));
        assertTrue(files.closed.await(5, TimeUnit.SECONDS));
        assertEquals(1, files.closes.get());
    }

    @Test
    public void testRejectedPrefetchFallsBackAndRestoresContext() {
        Files files = new Files(3) {
            @Override
            public RemoteFileInfo getOutput() {
                assertSame(context, ConnectContext.get());
                return super.getOutput();
            }
        };
        Tracers owner = Tracers.get();
        try (DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, task -> {
            throw new RejectedExecutionException();
        }, 1, 100000)) {
            assertEquals(3, source.getAllOutputs().size());
        }
        assertSame(owner, Tracers.get());
        assertSame(context, ConnectContext.get());
        assertTrue(Tracers.printVars().contains("prefetchFallback"));
    }

    @Test
    public void testSlowConsumerDoesNotOccupySharedWorker() throws Exception {
        try (DeltaLakePrefetchSource slow = new DeltaLakePrefetchSource(new Files(10000), executor, 2, 100000);
                DeltaLakePrefetchSource fast = new DeltaLakePrefetchSource(new Files(9), executor, 2, 100000)) {
            slow.getOutput();
            executor.submit(() -> { }).get();
            assertEquals(9, fast.getAllOutputs().size());
        }
    }
    @Test
    public void testWorkerContextAndTraceAreRestored() throws Exception {
        Tracers previous = executor.submit(Tracers::get).get();
        Files files = new Files(1) {
            @Override
            public RemoteFileInfo getOutput() {
                assertSame(context, ConnectContext.get());
                Tracers.record(Tracers.Module.EXTERNAL, "delta-test-worker", "read");
                return super.getOutput();
            }
        };
        try (DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, executor)) {
            assertEquals(1, source.getAllOutputs().size());
        }
        assertTrue(executor.submit(() -> ConnectContext.get() == null).get());
        assertSame(previous, executor.submit(Tracers::get).get());
        assertTrue(Tracers.printVars().contains("delta-test-worker"));
    }


    @Test
    public void testConsumerInterruptionCancelsQueuedRead() throws Exception {
        Files files = new Files(1);
        CountDownLatch scheduled = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        DeltaLakePrefetchSource source = new DeltaLakePrefetchSource(files, task -> scheduled.countDown());
        Thread consumer = new Thread(() -> {
            try {
                source.hasMoreOutput();
            } catch (Throwable e) {
                failure.set(e);
                assertTrue(Thread.currentThread().isInterrupted());
            }
        });
        consumer.start();
        try {
            assertTrue(scheduled.await(5, TimeUnit.SECONDS));
            consumer.interrupt();
            consumer.join(5000);
            assertFalse(consumer.isAlive());
            assertTrue(failure.get() instanceof StarRocksConnectorException);
            assertEquals(0, files.reads.get());
            assertEquals(1, files.closes.get());
        } finally {
            source.close();
            consumer.interrupt();
        }
    }

    @Test
    public void testMetadataOptInReadsRealDeltaLog(@TempDir Path tablePath) throws Exception {
        Path logDir = java.nio.file.Files.createDirectories(tablePath.resolve("_delta_log"));
        String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\","
                + "\"nullable\":true,\"metadata\":{}}]}";
        StringBuilder log = new StringBuilder("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
        log.append(GsonUtils.GSON.toJson(Map.of("metaData", Map.of(
                "id", "prefetch-test", "format", Map.of("provider", "parquet", "options", Map.of()),
                "schemaString", schema, "partitionColumns", List.of(), "configuration", Map.of())))).append('\n');
        for (int i = 0; i < 20; i++) {
            log.append(GsonUtils.GSON.toJson(Map.of("add", Map.of("path", "part-" + i + ".parquet",
                    "partitionValues", Map.of(), "size", 1000, "modificationTime", 0, "dataChange", true,
                    "stats", "{\"numRecords\":100}")))).append('\n');
        }
        java.nio.file.Files.writeString(logDir.resolve("00000000000000000000.json"), log);
        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(), new DeltaLakeCatalogProperties(Map.of()),
                CacheBuilder.newBuilder().build(), CacheBuilder.newBuilder().build());
        SnapshotImpl snapshot = (SnapshotImpl) io.delta.kernel.Table.forPath(engine, tablePath.toUri().toString())
                .getLatestSnapshot(engine);
        DeltaLakeTable table = new DeltaLakeTable(1, "delta", "db", "tbl",
                List.of(new Column("id", IntegerType.INT)), List.of(), snapshot, engine,
                new MetastoreTable("db", "tbl", tablePath.toUri().toString(), 0));
        DeltaLakeMetadata metadata = new DeltaLakeMetadata(new HdfsEnvironment(Map.of()), "delta", null, null,
                new ConnectorProperties(ConnectorType.DELTALAKE), executor);
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().build();
        try (RemoteFileInfoSource direct = metadata.getRemoteFilesAsync(table, params)) {
            assertFalse(direct instanceof DeltaLakePrefetchSource);
            assertEquals(20, direct.getAllOutputs().size());
        }
        context.getSessionVariable().setEnableDeltaLakeScanPrefetch(true);
        try (RemoteFileInfoSource prefetched = metadata.getRemoteFilesAsync(table, params)) {
            assertTrue(prefetched instanceof DeltaLakePrefetchSource);
            assertEquals(20, prefetched.getAllOutputs().size());
        }
    }

}
