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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.FailMsg;
import com.starrocks.load.loadv2.LoadsHistoryWriter.FlushResult;
import com.starrocks.load.loadv2.LoadsHistoryWriter.Row;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TManualLoadTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class LoadsHistoryWriterTest {

    private final List<String> executed = Lists.newArrayList();

    private final LoadsHistoryWriter.Sink recording = (sql, rows) -> executed.add(sql);

    @AfterEach
    public void tearDown() {
        LoadsHistoryWriter.activate(null);
    }

    private static TLoadInfo row(long id) {
        TLoadInfo info = new TLoadInfo();
        info.setJob_id(id);
        info.setLabel("label_" + id);
        info.setDb("test_db");
        info.setTable("t1");
        info.setUser("root");
        info.setState("FINISHED");
        info.setProgress("ETL:100%; LOAD:100%");
        info.setType("BROKER");
        info.setPriority("NORMAL");
        info.setNum_scan_rows(10);
        info.setNum_sink_rows(9);
        info.setRuntime_details("{\"load_id\":\"abc\"}");
        info.setProperties("{\"timeout\":\"3600\"}");
        info.setCreate_time_ms(1_700_000_000_000L);
        info.setLoad_finish_time_ms(1_700_000_060_000L + id);
        info.setTxn_id(100 + id);
        return info;
    }

    private static long idOf(Row row) {
        return Long.parseLong(row.values.substring(1, row.values.indexOf(',')));
    }

    private static List<Long> ids(List<Row> rows) {
        return rows.stream().map(LoadsHistoryWriterTest::idOf).collect(Collectors.toList());
    }

    private static int count(String text, String part) {
        int count = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
            count++;
        }
        return count;
    }

    @Test
    public void testBatchIsOneMultiRowInsert() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        writer.enqueue(row(1));
        writer.enqueue(row(2));
        writer.enqueue(row(3));
        Assertions.assertEquals(3, writer.queueSize());
        Assertions.assertTrue(writer.queuedBytes() > 0);

        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(500, 20));
        Assertions.assertEquals(1, executed.size());
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertEquals(0, writer.queuedBytes());
        String sql = executed.get(0);
        Assertions.assertTrue(sql.startsWith("INSERT INTO _statistics_.loads_history (`id`, `label`, `profile_id`, "), sql);
        Assertions.assertTrue(sql.startsWith(LoadsHistoryWriter.insertPrefix()), sql);
        Assertions.assertTrue(sql.contains("`cpu_ns`, `peak_mem_bytes`) VALUES (1, 'label_1', NULL, 'test_db', 't1', 'root', "),
                sql);
        Assertions.assertTrue(sql.contains("), (2, 'label_2', "), sql);
        Assertions.assertTrue(sql.contains("), (3, 'label_3', "), sql);
        Assertions.assertTrue(writer.lastFlushLatencyMs() >= 0);
        Assertions.assertEquals(0L, writer.lastBackstopRequestMs());
    }

    @Test
    public void testFlushWhenBatchSizeReached() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        for (int i = 1; i <= 7; i++) {
            writer.enqueue(row(i));
        }
        long start = System.currentTimeMillis();
        // A full batch is written without waiting for the interval and reports a backlog.
        Assertions.assertEquals(FlushResult.BACKLOG, writer.runOnce(5, 60_000));
        Assertions.assertTrue(System.currentTimeMillis() - start < 30_000);
        Assertions.assertEquals(1, executed.size());
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(Lists.newArrayList(6L, 7L), ids(writer.takeBatch(10, 0)));
    }

    @Test
    public void testFlushWhenIntervalElapsed() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        writer.enqueue(row(1));
        long start = System.currentTimeMillis();
        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(500, 300));
        Assertions.assertTrue(System.currentTimeMillis() - start >= 250);
        Assertions.assertEquals(1, executed.size());
        Assertions.assertEquals(0, writer.queueSize());

        // Nothing pending: the wait returns with nothing to write.
        Assertions.assertEquals(FlushResult.NOTHING, writer.runOnce(500, 10));
        Assertions.assertEquals(1, executed.size());
    }

    @Test
    public void testDropOldestWhenFull() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(3, recording);
        for (int i = 1; i <= 5; i++) {
            writer.enqueue(row(i));
        }
        Assertions.assertEquals(3, writer.queueSize());
        Assertions.assertEquals(2, writer.droppedTotal());
        // Dropped rows are left to the backstop.
        Assertions.assertTrue(writer.lastBackstopRequestMs() > 0);
        Assertions.assertEquals(Lists.newArrayList(3L, 4L, 5L), ids(writer.takeBatch(10, 0)));
    }

    @Test
    public void testByteBoundDropsOldestRows() throws Exception {
        int rowBytes = LoadsHistoryWriter.renderRow(row(1)).length();
        // Room for two rows and a bit, never for three.
        long maxBytes = rowBytes * 2L + rowBytes / 2;
        LoadsHistoryWriter writer = new LoadsHistoryWriter(() -> 1000, () -> maxBytes, recording);
        writer.enqueue(row(1));
        writer.enqueue(row(2));
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(2L * rowBytes, writer.queuedBytes());
        Assertions.assertEquals(0, writer.droppedTotal());

        writer.enqueue(row(3));
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(1, writer.droppedTotal());
        Assertions.assertTrue(writer.queuedBytes() <= maxBytes);
        Assertions.assertEquals(Lists.newArrayList(2L, 3L), ids(writer.takeBatch(10, 0)));
        Assertions.assertEquals(0, writer.queuedBytes());
    }

    @Test
    public void testShrinkingCapacityDropsOldestRows() throws Exception {
        AtomicInteger capacity = new AtomicInteger(10);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(capacity::get, () -> Long.MAX_VALUE, recording);
        for (int i = 1; i <= 6; i++) {
            writer.enqueue(row(i));
        }
        Assertions.assertEquals(6, writer.queueSize());
        Assertions.assertEquals(0, writer.droppedTotal());

        // The new limit applies to the next enqueue: rows beyond it go, oldest first.
        capacity.set(3);
        writer.enqueue(row(7));
        Assertions.assertEquals(3, writer.queueSize());
        Assertions.assertEquals(4, writer.droppedTotal());
        Assertions.assertEquals(Lists.newArrayList(5L, 6L, 7L), ids(writer.takeBatch(10, 0)));
    }

    @Test
    public void testFailedFlushKeepsRowsForRetry() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            if (calls.incrementAndGet() == 1) {
                throw new RuntimeException("backend down");
            }
            executed.add(sql);
        });
        writer.enqueue(row(1));
        writer.enqueue(row(2));

        Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(500, 10));
        Assertions.assertEquals(1, writer.consecutiveFailures());
        Assertions.assertEquals(2, writer.queueSize());
        // Rows kept for retry are not lost, so nothing asks for the backstop.
        Assertions.assertEquals(0L, writer.lastBackstopRequestMs());

        writer.enqueue(row(3));
        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(500, 10));
        Assertions.assertEquals(0, writer.consecutiveFailures());
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertEquals(1, executed.size());
        // Retried rows keep their order ahead of rows enqueued after the failure.
        String sql = executed.get(0);
        Assertions.assertTrue(sql.indexOf("'label_1'") < sql.indexOf("'label_2'"), sql);
        Assertions.assertTrue(sql.indexOf("'label_2'") < sql.indexOf("'label_3'"), sql);
    }

    @Test
    public void testFailedFlushRequeueRespectsCapacity() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(2, (sql, rows) -> {
            throw new RuntimeException("backend down");
        });
        writer.enqueue(row(1));
        writer.enqueue(row(2));
        List<Row> batch = writer.takeBatch(10, 0);
        writer.enqueue(row(3));
        writer.enqueue(row(4));
        Assertions.assertFalse(writer.flush(batch));
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(2, writer.droppedTotal());
        Assertions.assertEquals(Lists.newArrayList(3L, 4L), ids(writer.takeBatch(10, 0)));
    }

    @Test
    public void testRetryCapSplitsBatchAndDropsPoisonRow() throws Exception {
        // The table rejects whatever statement carries row 4.
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            if (sql.contains("'label_4'")) {
                throw new RuntimeException("rejected");
            }
            executed.add(sql);
        });
        for (int i = 1; i <= 4; i++) {
            writer.enqueue(row(i));
        }

        for (int i = 0; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
            Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(10, 0));
        }
        // The first half goes through, but the other half is still queued: not drained.
        Assertions.assertEquals(FlushResult.BACKLOG, writer.runOnce(10, 0));
        Assertions.assertEquals(2, writer.queueSize());

        int attempts = 4;
        while (writer.queueSize() > 0 && attempts < 50) {
            writer.runOnce(10, 0);
            attempts++;
        }
        // 3 failed attempts on [1..4], then [1,2] written, 3 failed on [3,4], [3] written,
        // 3 failed on [4], which is dropped.
        Assertions.assertEquals(11, attempts);
        Assertions.assertEquals(2, executed.size());
        Assertions.assertEquals(1, count(executed.get(0), "'label_1'"));
        Assertions.assertEquals(1, count(executed.get(0), "'label_2'"));
        Assertions.assertEquals(1, count(executed.get(1), "'label_3'"));
        Assertions.assertEquals(1, writer.droppedTotal());

        // A later batch starts with a full limit again.
        writer.enqueue(row(5));
        writer.enqueue(row(6));
        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(10, 0));
        Assertions.assertEquals(0, writer.consecutiveFailures());
        Assertions.assertEquals(3, executed.size());
        Assertions.assertTrue(executed.get(2).contains("'label_5'") && executed.get(2).contains("'label_6'"));
    }

    @Test
    public void testSingleFailingRowIsDroppedAfterRetryCap() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            throw new RuntimeException("rejected");
        });
        writer.enqueue(row(1));
        for (int i = 1; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
            Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(10, 0));
            Assertions.assertEquals(1, writer.queueSize());
        }
        Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(10, 0));
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertEquals(1, writer.droppedTotal());
        Assertions.assertTrue(writer.lastBackstopRequestMs() > 0);
    }

    @Test
    public void testFailedFlushesBackOffAndKeepRowsWhileTableIsUnreachable() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<Long> sleeps = Lists.newArrayList();
        AtomicBoolean reachable = new AtomicBoolean(false);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(() -> 100, () -> Long.MAX_VALUE, (sql, rows) -> {
            throw new RuntimeException("rejected");
        }, reachable::get);
        writer.setClock(now::get, millis -> {
            sleeps.add(millis);
            now.addAndGet(millis);
        });
        writer.enqueue(row(1));

        // The queue holds a full batch, so nothing but the backoff paces the attempts: the flush
        // interval doubled per failure, capped.
        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(1, 1000));
        }
        Assertions.assertEquals(List.of(1000L, 2000L, 4000L, 8000L, 16000L, 32000L, 60000L, 60000L, 60000L), sleeps);
        // No node is alive: the failures say nothing about the row, which is neither counted nor dropped.
        Assertions.assertEquals(10, writer.consecutiveFailures());
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertEquals(0, writer.droppedTotal());
        Assertions.assertEquals(0L, writer.lastBackstopRequestMs());

        // With the table reachable the same failures count, and the row is dropped at the cap
        // and left to the backstop.
        reachable.set(true);
        for (int i = 0; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
            Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(1, 1000));
        }
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertEquals(1, writer.droppedTotal());
        Assertions.assertEquals(now.get(), writer.lastBackstopRequestMs());
    }

    @Test
    public void testBackoffResetsAfterASuccessfulFlush() throws Exception {
        Assertions.assertEquals(0L, LoadsHistoryWriter.backoffMs(0, 5));
        Assertions.assertEquals(0L, LoadsHistoryWriter.backoffMs(10_000, 0));
        Assertions.assertEquals(10_000L, LoadsHistoryWriter.backoffMs(10_000, 1));
        Assertions.assertEquals(40_000L, LoadsHistoryWriter.backoffMs(10_000, 3));
        Assertions.assertEquals(LoadsHistoryWriter.MAX_BACKOFF_MS, LoadsHistoryWriter.backoffMs(10_000, 4));
        Assertions.assertEquals(LoadsHistoryWriter.MAX_BACKOFF_MS, LoadsHistoryWriter.backoffMs(1000, 1000));

        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<Long> sleeps = Lists.newArrayList();
        AtomicInteger calls = new AtomicInteger();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            if (calls.incrementAndGet() <= 2) {
                throw new RuntimeException("backend down");
            }
            executed.add(sql);
        });
        writer.setClock(now::get, millis -> {
            sleeps.add(millis);
            now.addAndGet(millis);
        });
        writer.enqueue(row(1));
        writer.enqueue(row(2));
        Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(1, 1000));
        Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(1, 1000));
        Assertions.assertEquals(FlushResult.BACKLOG, writer.runOnce(1, 1000));
        Assertions.assertEquals(List.of(1000L, 2000L), sleeps);
        // The next attempt after a success does not wait.
        Assertions.assertEquals(FlushResult.BACKLOG, writer.runOnce(1, 1000));
        Assertions.assertEquals(List.of(1000L, 2000L), sleeps);
        Assertions.assertEquals(2, executed.size());
    }

    @Test
    public void testLateRowIsQueuedAndKeptFromTheBackstop() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        writer.setClock(now::get, millis -> { });

        // A load that just finished is queued; the backstop does not consider it yet anyway.
        TLoadInfo fresh = row(1);
        fresh.setLoad_finish_time_ms(now.get() - 10_000L);
        writer.enqueue(fresh);
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertTrue(writer.beginBackstop().excludedIds.isEmpty());
        writer.endBackstop();

        // A load that finished long enough ago for the backstop to copy it while its row waits
        // is queued too, and the backstop is told to leave it out until the row is written.
        TLoadInfo late = row(2);
        late.setLoad_finish_time_ms(now.get() - LoadsHistoryWriter.FRESH_ROW_MS);
        writer.enqueue(late);
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(0L, writer.lastBackstopRequestMs());
        Assertions.assertEquals(List.of(2L), writer.beginBackstop().excludedIds);

        // While a backstop statement runs it cannot be told any more: a late row that arrives
        // now is left to the next backstop run. A fresh one is queued as always.
        TLoadInfo duringBackstop = row(3);
        duringBackstop.setLoad_finish_time_ms(now.get() - 5 * 60_000L);
        writer.enqueue(duringBackstop);
        TLoadInfo freshDuringBackstop = row(4);
        freshDuringBackstop.setLoad_finish_time_ms(now.get());
        writer.enqueue(freshDuringBackstop);
        Assertions.assertEquals(3, writer.queueSize());
        Assertions.assertEquals(now.get(), writer.lastBackstopRequestMs());
        Assertions.assertEquals(now.get(), writer.oldestBackstopRequestMs());
        writer.endBackstop();

        // Once the rows are written the backstop may see the load again (and finds it in the table).
        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(10, 0));
        Assertions.assertEquals(1, executed.size());
        Assertions.assertEquals(1, count(executed.get(0), "'label_2'"));
        Assertions.assertTrue(writer.beginBackstop().excludedIds.isEmpty());
        writer.endBackstop();
        Assertions.assertEquals(now.get(), writer.lastTakeMs());
    }

    @Test
    public void testDroppedLateRowIsGivenBackToTheBackstop() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(1, recording);
        writer.setClock(now::get, millis -> { });
        TLoadInfo late = row(1);
        late.setLoad_finish_time_ms(now.get() - 2 * LoadsHistoryWriter.FRESH_ROW_MS);
        writer.enqueue(late);
        Assertions.assertEquals(List.of(1L), writer.beginBackstop().excludedIds);
        writer.endBackstop();
        // The queue is full: the late row makes room, and its load is the backstop's again.
        TLoadInfo fresh = row(2);
        fresh.setLoad_finish_time_ms(now.get());
        writer.enqueue(fresh);
        Assertions.assertEquals(1, writer.droppedTotal());
        Assertions.assertTrue(writer.beginBackstop().excludedIds.isEmpty());
        writer.endBackstop();
        Assertions.assertEquals(now.get(), writer.oldestBackstopRequestMs());
    }

    @Test
    public void testClusterSideFailureDoesNotCountAgainstTheRows() throws Exception {
        Assertions.assertTrue(LoadsHistoryWriter.isClusterSideFailure(new RuntimeException("wrapped",
                new RuntimeException("No alive backend or compute node in warehouse default_warehouse"))));
        Assertions.assertTrue(LoadsHistoryWriter.isClusterSideFailure(new RuntimeException("Load timeout")));
        Assertions.assertFalse(LoadsHistoryWriter.isClusterSideFailure(new RuntimeException("Insert has filtered data")));
        Assertions.assertFalse(LoadsHistoryWriter.isClusterSideFailure(new RuntimeException()));

        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            throw new RuntimeException("no queryable replica found for tablet 10001");
        });
        writer.setClock(() -> 1_800_000_000_000L, millis -> { });
        writer.enqueue(row(1));
        for (int i = 0; i < 2 * LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
            Assertions.assertEquals(FlushResult.FAILED, writer.runOnce(10, 0));
        }
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertEquals(0, writer.droppedTotal());
    }

    @Test
    public void testOldestBackstopRequestIsKeptUntilCovered() {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(1, recording);
        writer.setClock(now::get, millis -> { });
        long first = now.get();
        // Each new row pushes the one before it out of the one-row queue.
        for (int i = 1; i <= 4; i++) {
            TLoadInfo info = row(i);
            info.setLoad_finish_time_ms(now.get());
            writer.enqueue(info);
            now.addAndGet(30_000L);
        }
        long last = now.get() - 30_000L;
        Assertions.assertEquals(3, writer.droppedTotal());
        Assertions.assertEquals(first + 30_000L, writer.oldestBackstopRequestMs());
        Assertions.assertEquals(last, writer.lastBackstopRequestMs());

        // A run that covered the first drops leaves the later ones pending.
        writer.backstopCovered(first + 45_000L);
        Assertions.assertEquals(first + 45_001L, writer.oldestBackstopRequestMs());
        // A run that covered everything clears the request.
        writer.backstopCovered(last);
        Assertions.assertEquals(0L, writer.oldestBackstopRequestMs());
        Assertions.assertEquals(last, writer.lastBackstopRequestMs());
    }

    @Test
    public void testHeldRowIsWrittenOnceItsLoadIsVisible() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        writer.setClock(now::get, millis -> { });
        TLoadInfo info = row(7);
        info.setLoad_finish_time_ms(now.get());
        Assertions.assertTrue(writer.hold(7L, () -> info));

        // Not visible yet: nothing is queued however often the holds are looked at, and the
        // backstop leaves the load out although it finished long ago by now.
        now.addAndGet(10 * 60_000L);
        writer.releaseHolds();
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertEquals(1, writer.heldRows());
        Assertions.assertEquals(List.of(7L), writer.beginBackstop().excludedIds);
        writer.endBackstop();

        // Visible: the row is queued with the visible time and stays excluded until it is written.
        long visibleMs = now.get() - 1000L;
        info.setVisible_time_ms(visibleMs);
        writer.releaseHolds();
        writer.releaseHolds();
        Assertions.assertEquals(0, writer.heldRows());
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertEquals(List.of(7L), writer.beginBackstop().excludedIds);
        writer.endBackstop();
        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(10, 0));
        Assertions.assertEquals(1, executed.size());
        Assertions.assertEquals(1, count(executed.get(0), "'label_7'"));
        Assertions.assertTrue(executed.get(0).contains("'" + LoadsHistoryWriter.formatDatetime(visibleMs) + "'"));
        Assertions.assertTrue(writer.beginBackstop().excludedIds.isEmpty());
        writer.endBackstop();
        Assertions.assertEquals(0L, writer.lastBackstopRequestMs());
    }

    @Test
    public void testHeldRowIsWrittenAsItIsWhenItsLoadNeverBecomesVisible() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        writer.setClock(now::get, millis -> { });
        TLoadInfo info = row(8);
        info.setLoad_finish_time_ms(now.get());
        Assertions.assertTrue(writer.hold(8L, () -> info));

        long maxHoldMs = LoadsHistorySyncer.backstopLookbackMinutes() * 60_000L / 2;
        now.addAndGet(maxHoldMs - 1);
        writer.releaseHolds();
        Assertions.assertEquals(0, writer.queueSize());
        now.addAndGet(1);
        writer.releaseHolds();
        writer.releaseHolds();
        Assertions.assertEquals(0, writer.heldRows());
        // Exactly one row, without a visible time; the backstop still leaves the load out.
        Assertions.assertEquals(List.of(8L), writer.beginBackstop().excludedIds);
        writer.endBackstop();
        Assertions.assertEquals(FlushResult.DRAINED, writer.runOnce(10, 0));
        Assertions.assertEquals(1, executed.size());
        Assertions.assertEquals(1, count(executed.get(0), "'label_8'"));
        Assertions.assertEquals(FlushResult.NOTHING, writer.runOnce(10, 0));
    }

    @Test
    public void testHoldIsRefusedWhenTooManyLoadsAreKeptFromTheBackstop() {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        for (long id = 0; id < LoadsHistoryWriter.MAX_BACKSTOP_EXCLUSIONS; id++) {
            Assertions.assertTrue(writer.hold(id, () -> null));
        }
        Assertions.assertFalse(writer.hold(-1L, () -> null));
        // A row whose supplier yields nothing is dropped from the holds and left to the backstop.
        writer.releaseHolds();
        Assertions.assertEquals(0, writer.heldRows());
        Assertions.assertTrue(writer.lastBackstopRequestMs() > 0);
        // Leaving the leader role drops what is held.
        Assertions.assertTrue(writer.hold(1L, () -> null));
        writer.clear();
        Assertions.assertEquals(0, writer.heldRows());
        Assertions.assertEquals(0L, writer.oldestBackstopRequestMs());
    }

    @Test
    public void testDatetimeIgnoresTheSessionTimeZone() {
        String globalZone = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable().getTimeZone();
        if (globalZone == null) {
            globalZone = TimeUtils.DEFAULT_TIME_ZONE;
        }
        long epochMs = 1_700_000_060_000L;
        String inGlobalZone = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.of(globalZone, TimeUtils.TIME_ZONE_ALIAS_MAP)).format(Instant.ofEpochMilli(epochMs));
        TLoadInfo info = row(1);
        info.setLoad_finish_time_ms(epochMs);
        info.setVisible_time_ms(epochMs);
        String rendered = LoadsHistoryWriter.renderRow(info);
        Assertions.assertEquals(2, count(rendered, "'" + inGlobalZone + "'"), rendered);

        // The thread of a user session whose time_zone differs renders the very same row.
        ConnectContext previous = ConnectContext.get();
        ConnectContext session = new ConnectContext();
        // Any zone whose wall clock differs from the global one at that moment.
        String sessionZone = List.of("UTC", "Asia/Tokyo").stream()
                .filter(zone -> !DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of(zone))
                        .format(Instant.ofEpochMilli(epochMs)).equals(inGlobalZone))
                .findFirst().get();
        session.getSessionVariable().setTimeZone(sessionZone);
        session.setThreadLocalInfo();
        try {
            Assertions.assertFalse(inGlobalZone.startsWith(TimeUtils.longToTimeString(epochMs)));
            Assertions.assertEquals(inGlobalZone, LoadsHistoryWriter.formatDatetime(epochMs));
            Assertions.assertEquals(rendered, LoadsHistoryWriter.renderRow(info));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testSkipsRowsWithoutFinishTimeAndWritesToHistoryTable() {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, recording);
        TLoadInfo running = row(1);
        running.unsetLoad_finish_time_ms();
        writer.enqueue(running);
        TLoadInfo self = row(2);
        self.setDb(LoadsHistorySyncer.LOADS_HISTORY_DB_NAME);
        self.setTable(LoadsHistorySyncer.LOADS_HISTORY_TABLE_NAME);
        writer.enqueue(self);
        TLoadInfo sibling = row(3);
        sibling.setDb("_STATISTICS_");
        sibling.setTable("load_jobs");
        writer.enqueue(sibling);
        writer.enqueue(null);
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertTrue(LoadsHistoryWriter.isInternalLoadTable(sibling));

        // The same table in another database is an ordinary load.
        TLoadInfo other = row(4);
        other.setTable(LoadsHistorySyncer.LOADS_HISTORY_TABLE_NAME);
        writer.enqueue(other);
        Assertions.assertEquals(1, writer.queueSize());
    }

    @Test
    public void testRenderEscapesAndNulls() {
        TLoadInfo info = row(7);
        info.setLabel("it's a \\ label\nwith \"quotes\"");
        info.setError_msg("type:LOAD_RUN_FAIL; msg:bad 'row'");
        info.setRuntime_details("{\"msg\":\"a\\\"b\",\"path\":\"c:\\\\d\"}");
        info.setTxn_id(-1);
        info.setLoad_id("1-2");
        info.setTable_id(42);
        info.setVisible_time_ms(1_700_000_070_000L);
        info.setBe_ids("1,2");

        String rendered = LoadsHistoryWriter.renderRow(info);
        Assertions.assertTrue(rendered.startsWith("(7, 'it\\'s a \\\\ label\nwith \"quotes\"', NULL, 'test_db', 't1', 'root', "),
                rendered);
        Assertions.assertTrue(rendered.contains("'type:LOAD_RUN_FAIL; msg:bad \\'row\\''"), rendered);
        Assertions.assertTrue(rendered.contains("parse_json('{\"msg\":\"a\\\\\"b\",\"path\":\"c:\\\\\\\\d\"}')"), rendered);
        Assertions.assertTrue(rendered.contains("parse_json('{\"timeout\":\"3600\"}')"), rendered);
        Assertions.assertTrue(rendered.contains("'" + LoadsHistoryWriter.formatDatetime(1_700_000_000_000L) + "', NULL, NULL, '"
                + LoadsHistoryWriter.formatDatetime(1_700_000_060_007L) + "', "), rendered);
        Assertions.assertTrue(rendered.contains("'" + LoadsHistoryWriter.formatDatetime(1_700_000_070_000L) + "'"), rendered);
        // txn_id -1 is unknown, load_id/table_id/be_ids are set, the rest of the job columns are NULL.
        Assertions.assertTrue(rendered.endsWith(", 7, NULL, '1-2', 42, NULL, NULL, NULL, '"
                + LoadsHistoryWriter.formatDatetime(1_700_000_070_000L) + "', NULL, NULL, '1,2', NULL, NULL, NULL)"), rendered);
        // A row whose values hold no separators renders exactly one value per archive column.
        Assertions.assertEquals(LoadsHistorySyncer.HISTORY_COLUMNS.size(),
                LoadsHistoryWriter.renderRow(row(1)).split(", ").length);
    }

    @Test
    public void testFitVarchar() {
        String ascii = "a".repeat(3000);
        Assertions.assertEquals(2048, LoadsHistoryWriter.fitVarchar(ascii).length());
        // U+4E2D takes three bytes in UTF-8.
        String wide = new String(Character.toChars(0x4e2d)).repeat(1000);
        Assertions.assertEquals(682, LoadsHistoryWriter.fitVarchar(wide).length());
        // U+1F600 takes four bytes and two chars; the cut never splits a pair.
        String emoji = new String(Character.toChars(0x1F600)).repeat(600);
        String fitted = LoadsHistoryWriter.fitVarchar(emoji);
        Assertions.assertEquals(1024, fitted.length());
        Assertions.assertFalse(Character.isHighSurrogate(fitted.charAt(fitted.length() - 1)));
        Assertions.assertEquals("abc", LoadsHistoryWriter.fitVarchar("abc"));
        String exact = ascii.substring(0, 2048);
        Assertions.assertSame(exact, LoadsHistoryWriter.fitVarchar(exact));
        Assertions.assertEquals("'\\\\\\'\\0'", LoadsHistoryWriter.quoteRaw("\\'\0"));
    }

    @Test
    public void testLoadJobFinishEnqueuesOneRow(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        LoadsHistoryWriter writer = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(writer);

        LoadJob job = new BrokerLoadJob();
        job.updateState(JobState.FINISHED);
        Assertions.assertEquals(JobState.FINISHED, job.getState());
        List<Row> rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertTrue(rows.get(0).values.contains("'FINISHED'"), rows.get(0).values);

        // A follower, or a node whose writer stopped, records nothing.
        LoadsHistoryWriter.deactivate(writer);
        new BrokerLoadJob().updateState(JobState.FINISHED);
        Assertions.assertEquals(0, writer.queueSize());
    }

    @Test
    public void testLoadJobUnknownEnqueuesOneRow(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        LoadsHistoryWriter writer = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(writer);

        LoadJob job = new BrokerLoadJob();
        long createMs = System.currentTimeMillis();
        Deencapsulation.setField(job, "createTimestamp", createMs);
        job.updateState(JobState.UNKNOWN);
        Assertions.assertEquals(JobState.UNKNOWN, job.getState());
        List<Row> rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertTrue(rows.get(0).values.contains("'UNKNOWN'"), rows.get(0).values);
        // An unknown job ends at its creation time, which is what partitions its history row.
        Assertions.assertEquals(createMs, job.getFinishTimestamp());
    }

    @Test
    public void testLoadJobCancelEnqueuesOneRow(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog)
            throws Exception {
        new FakeEditLog();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        LoadsHistoryWriter writer = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(writer);

        LoadJob job = new BrokerLoadJob();
        job.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.USER_CANCEL, "cancelled by user"), false);
        Assertions.assertEquals(JobState.CANCELLED, job.getState());
        List<Row> rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertTrue(rows.get(0).values.contains("'CANCELLED'"), rows.get(0).values);
        Assertions.assertTrue(rows.get(0).values.contains("cancelled by user"), rows.get(0).values);
    }

    @Test
    public void testStreamLoadTaskFinalStatesEnqueueOneRow() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(writer);

        StreamLoadTask finished = new StreamLoadTask(1L, new Database(), new OlapTable(), "finished_label", "", "",
                100000L, System.currentTimeMillis(), false, WarehouseManager.DEFAULT_RESOURCE);
        // A backend sends its counters with the commit request. The request handler applies
        // them to the task only after the commit call returned, which is after this callback, so
        // the row must take them from the transaction.
        TManualLoadTxnCommitAttachment counters = new TManualLoadTxnCommitAttachment();
        counters.setLoadedRows(90);
        counters.setFilteredRows(7);
        counters.setUnselectedRows(3);
        counters.setLoadedBytes(4096);
        counters.setErrorLogUrl("http://be:8040/api/_load_error_log?file=error_log");
        TransactionState txnState = new TransactionState();
        txnState.setFinishTime(System.currentTimeMillis());
        txnState.setTxnCommitAttachment(new ManualLoadTxnCommitAttachment(counters));
        finished.afterVisible(txnState);
        List<Row> rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("finished_label", rows.get(0).label);
        Assertions.assertTrue(rows.get(0).values.contains("'FINISHED'"), rows.get(0).values);
        // scan_bytes, filtered_rows, unselected_rows, sink_rows
        Assertions.assertTrue(rows.get(0).values.contains(", 4096, 7, 3, 90, "), rows.get(0).values);
        TLoadInfo finishedInfo = finished.toThrift().get(0);
        Assertions.assertEquals("http://be:8040/api/_load_error_log?file=error_log", finishedInfo.getUrl());
        // The handler's later call with the same attachment changes nothing.
        finished.setLoadState(txnState.getTxnCommitAttachment(), "");
        Assertions.assertEquals(90, finished.toThrift().get(0).getNum_sink_rows());
        Assertions.assertEquals(0, writer.queueSize());

        StreamLoadTask aborted = new StreamLoadTask(2L, new Database(), new OlapTable(), "aborted_label", "", "",
                100000L, System.currentTimeMillis(), true, WarehouseManager.DEFAULT_RESOURCE);
        aborted.afterAborted(txnState, "aborted");
        // A second abort of a task already in a final state does not record another row.
        aborted.afterAborted(txnState, "aborted again");
        rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertTrue(rows.get(0).values.contains("'CANCELLED'"), rows.get(0).values);
        // The abort request carries the counters the same way.
        Assertions.assertTrue(rows.get(0).values.contains(", 4096, 7, 3, 90, "), rows.get(0).values);
        Assertions.assertTrue(rows.get(0).values.contains("'ROUTINE_LOAD'"), rows.get(0).values);

        StreamLoadTask restarted = new StreamLoadTask(3L, new Database(), new OlapTable(), "restart_label", "", "",
                100000L, System.currentTimeMillis(), false, WarehouseManager.DEFAULT_RESOURCE);
        restarted.cancelAfterRestart();
        rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertTrue(rows.get(0).values.contains("'CANCELLED'"), rows.get(0).values);

        // Cancelled before a transaction was begun: the task ends now and is recorded once.
        StreamLoadTask noTxn = new StreamLoadTask(4L, new Database(), new OlapTable(), "no_txn_label", "", "",
                100000L, System.currentTimeMillis(), false, WarehouseManager.DEFAULT_RESOURCE);
        long before = System.currentTimeMillis();
        Assertions.assertNull(noTxn.cancelTask("manual"));
        Assertions.assertTrue(noTxn.getFinishTimestampMs() >= before);
        Assertions.assertNotNull(noTxn.cancelTask("again"));
        rows = writer.takeBatch(10, 0);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("no_txn_label", rows.get(0).label);
        Assertions.assertTrue(rows.get(0).values.contains("'CANCELLED'"), rows.get(0).values);
    }
}
