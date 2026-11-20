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

package com.starrocks.connector.iceberg.io;

import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IcebergPlanningIoStatsTest {

    @Test
    public void testRecordAndFormat() {
        IcebergPlanningIoStats stats = new IcebergPlanningIoStats();
        Assertions.assertEquals("Requests: 0 (HEAD: 0, GET: 0), IOTime: 0ms, Retries: 0", stats.toString());
        Assertions.assertNull(stats.formatSlowest());
        stats.recordGet(TimeUnit.MILLISECONDS.toNanos(30), "s3://bucket/db/t1/metadata/m1.avro");
        stats.recordHead(TimeUnit.MILLISECONDS.toNanos(2000), "s3://bucket/db/t1/metadata/snap-1.avro");
        stats.recordGet(TimeUnit.MILLISECONDS.toNanos(10), "s3://bucket/db/t1/metadata/m2.avro");
        stats.recordRetry();
        Assertions.assertEquals(3, stats.getRequests());
        Assertions.assertEquals(2040, stats.getIoTimeMs());
        Assertions.assertEquals(2000, stats.getMaxRequestMs());
        Assertions.assertEquals("HEAD snap-1.avro", stats.getSlowestRequest());
        Assertions.assertEquals("Requests: 3 (HEAD: 1, GET: 2), IOTime: 2s40ms, Retries: 1, " +
                "Slowest: HEAD snap-1.avro 2s0ms", stats.toString());
        Assertions.assertEquals("3 (HEAD: 1, GET: 2)", stats.formatRequests());
        Assertions.assertEquals("2s40ms", stats.formatIoTime());
        Assertions.assertEquals("HEAD snap-1.avro 2s0ms", stats.formatSlowest());
        Assertions.assertEquals("3 (HEAD: 1, GET: 2), 2s40ms", stats.formatPhase());
    }

    @Test
    public void testScopeRestoresPrevious() {
        IcebergPlanningIoStats outer = new IcebergPlanningIoStats();
        IcebergPlanningIoStats inner = new IcebergPlanningIoStats();
        try (IcebergPlanningIoStats.Scope ignored = outer.enter()) {
            try (IcebergPlanningIoStats.Scope ignored2 = inner.enter()) {
                Assertions.assertSame(inner, IcebergPlanningIoStats.current());
            }
            Assertions.assertSame(outer, IcebergPlanningIoStats.current());
        }
        Assertions.assertNull(IcebergPlanningIoStats.current());
    }

    @Test
    public void testEnterIfNoneKeepsCurrent() {
        IcebergPlanningIoStats planning = new IcebergPlanningIoStats();
        IcebergPlanningIoStats table = new IcebergPlanningIoStats();
        // a table-level entry point reached from inside a planning: the planning keeps the I/O
        try (IcebergPlanningIoStats.Scope ignored = planning.enter()) {
            try (IcebergPlanningIoStats.Scope ignored2 = IcebergPlanningIoStats.enterIfNone(table)) {
                Assertions.assertSame(planning, IcebergPlanningIoStats.current());
            }
            Assertions.assertSame(planning, IcebergPlanningIoStats.current());
        }
        // reached on its own (the optimizer): the table takes it
        try (IcebergPlanningIoStats.Scope ignored = IcebergPlanningIoStats.enterIfNone(table)) {
            Assertions.assertSame(table, IcebergPlanningIoStats.current());
        }
        Assertions.assertNull(IcebergPlanningIoStats.current());
    }

    @Test
    public void testAdd() {
        IcebergPlanningIoStats scan = new IcebergPlanningIoStats();
        scan.recordGet(TimeUnit.MILLISECONDS.toNanos(20), "s3://b/t1/metadata/m1.avro");
        IcebergPlanningIoStats table = new IcebergPlanningIoStats();
        table.recordGet(TimeUnit.MILLISECONDS.toNanos(5), "s3://b/t1/metadata/00001.metadata.json");
        table.recordHead(TimeUnit.MILLISECONDS.toNanos(900), "s3://b/t1/metadata/snap-1.avro");
        table.recordRetry();

        scan.add(table);
        Assertions.assertEquals("3 (HEAD: 1, GET: 2)", scan.formatRequests());
        Assertions.assertEquals(925, scan.getIoTimeMs());
        Assertions.assertEquals(1, scan.getRetries());
        Assertions.assertEquals("HEAD snap-1.avro 900ms", scan.formatSlowest());

        // a faster slowest request does not replace the current one
        IcebergPlanningIoStats fast = new IcebergPlanningIoStats();
        fast.recordGet(TimeUnit.MILLISECONDS.toNanos(1), "s3://b/t1/metadata/m2.avro");
        scan.add(fast);
        Assertions.assertEquals("HEAD snap-1.avro 900ms", scan.formatSlowest());
        // empty stats add nothing
        scan.add(new IcebergPlanningIoStats());
        Assertions.assertEquals("4 (HEAD: 1, GET: 3)", scan.formatRequests());
    }

    @Test
    public void testWrappedExecutorCarriesStats() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            IcebergPlanningIoStats stats = new IcebergPlanningIoStats();
            ExecutorService planning = stats.wrap(pool);
            // ParallelIterable submits through CompletableFuture.supplyAsync
            IcebergPlanningIoStats seen = CompletableFuture.supplyAsync(IcebergPlanningIoStats::current, planning)
                    .get(10, TimeUnit.SECONDS);
            Assertions.assertSame(stats, seen);
            // the pool thread is left clean for other queries
            Assertions.assertNull(pool.submit(IcebergPlanningIoStats::current).get(10, TimeUnit.SECONDS));
            Assertions.assertThrows(UnsupportedOperationException.class, planning::shutdown);
            Assertions.assertFalse(pool.isShutdown());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testInterceptorCountsRetries() {
        IcebergPlanningIoInterceptor interceptor = new IcebergPlanningIoInterceptor();
        IcebergPlanningIoStats stats = new IcebergPlanningIoStats();
        InterceptorContext s3 = InterceptorContext.builder()
                .request(GetObjectRequest.builder().bucket("b").key("k").build()).build();
        InterceptorContext glue = InterceptorContext.builder()
                .request(GetTableRequest.builder().databaseName("db1").name("t1").build()).build();

        // no planning on this thread: nothing counted, nothing breaks
        interceptor.beforeTransmission(s3, new ExecutionAttributes());

        try (IcebergPlanningIoStats.Scope ignored = stats.enter()) {
            // one S3 execution with three attempts: two retries
            ExecutionAttributes execution = new ExecutionAttributes();
            interceptor.beforeTransmission(s3, execution);
            interceptor.beforeTransmission(s3, execution);
            interceptor.beforeTransmission(s3, execution);
            // another S3 execution, first attempt succeeds
            interceptor.beforeTransmission(s3, new ExecutionAttributes());
            // a catalog call retrying in the same scope is not storage I/O
            ExecutionAttributes catalogCall = new ExecutionAttributes();
            interceptor.beforeTransmission(glue, catalogCall);
            interceptor.beforeTransmission(glue, catalogCall);
        }
        Assertions.assertEquals(2, stats.getRetries());
    }

    @Test
    public void testTimedStreamRecordsOneGetOnClose() throws IOException {
        IcebergPlanningIoStats stats = new IcebergPlanningIoStats();
        byte[] data = "manifest".getBytes();
        SeekableInputStream stream = PlanningIoTimedInputStream.wrap(new BytesStream(data), stats, "s3://b/m1.avro", 0);
        Assertions.assertEquals(data.length, stream.readAllBytes().length);
        Assertions.assertEquals(0, stats.getGetRequests());
        stream.close();
        stream.close();
        Assertions.assertEquals(1, stats.getGetRequests());
        Assertions.assertEquals("GET m1.avro", stats.getSlowestRequest());
    }

    private static final class BytesStream extends SeekableInputStream {
        private final byte[] data;
        private InputStream in;
        private long pos;

        private BytesStream(byte[] data) {
            this.data = data;
            this.in = new ByteArrayInputStream(data);
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            in = new ByteArrayInputStream(data, (int) newPos, data.length - (int) newPos);
            pos = newPos;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b >= 0) {
                pos++;
            }
            return b;
        }
    }
}
