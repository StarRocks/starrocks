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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class LoadsHistorySyncerTest {

    private static List<String> viewColumnNames() {
        return LoadsSystemTable.columns().stream()
                .map(column -> column.getName().toLowerCase())
                .collect(Collectors.toList());
    }

    private static List<String> historyColumnNames() {
        return LoadsHistorySyncer.HISTORY_COLUMNS.stream().map(column -> column.first).collect(Collectors.toList());
    }

    private static OlapTable table(long id, List<Column> columns) {
        return new OlapTable(id, "loads_history", columns, KeysType.DUP_KEYS, new SinglePartitionInfo(),
                new HashDistributionInfo(3, Lists.newArrayList(columns.get(1))));
    }

    @Test
    public void testHistoryColumnsMirrorLoadsView() {
        Assertions.assertEquals(viewColumnNames(), historyColumnNames());
        Assertions.assertTrue(historyColumnNames().containsAll(
                List.of("txn_id", "load_id", "table_id", "parent_id", "statement", "error_code", "visible_time",
                        "sink_bytes", "client_ip", "be_ids", "commit_throttle_ms", "cpu_ns", "peak_mem_bytes")));
    }

    @Test
    public void testCreateTableDeclaresEveryColumn() {
        String createSql = LoadsHistorySyncer.createKeeper().getCreateTableSql();
        for (Pair<String, String> column : LoadsHistorySyncer.HISTORY_COLUMNS) {
            Assertions.assertTrue(createSql.contains("`" + column.first + "` " + column.second), column.first);
        }
        Assertions.assertTrue(createSql.contains("`load_finish_time` datetime not null"));
        Assertions.assertTrue(createSql.contains("`runtime_details` json"));
        Assertions.assertTrue(createSql.contains("`label` varchar(2048)"));
        Assertions.assertTrue(createSql.contains("PARTITION BY date_trunc('DAY', load_finish_time)"));
    }

    @Test
    public void testSyncSqlNamesColumnsOnBothSides() {
        String sql = LoadsHistorySyncer.SQLBuilder.buildSyncSql();
        String columnList = LoadsHistorySyncer.HISTORY_COLUMNS.stream()
                .map(column -> "`" + column.first + "`")
                .collect(Collectors.joining(", "));
        Assertions.assertTrue(sql.startsWith("INSERT INTO _statistics_.loads_history (" + columnList + ") "
                + "SELECT " + columnList + " FROM information_schema.loads l WHERE l.load_finish_time IS NOT NULL"), sql);
        Assertions.assertFalse(sql.contains("SELECT *"));
        // The writer's INSERT names the same columns in the same order.
        Assertions.assertEquals("INSERT INTO _statistics_.loads_history (" + columnList + ") VALUES ",
                LoadsHistoryWriter.insertPrefix());
    }

    @Test
    public void testKeeperAddsOnlyMissingColumns() {
        Column id = new Column("id", IntegerType.BIGINT);
        Column label = new Column("label", TypeFactory.createVarcharType(2048));
        Column txnId = new Column("txn_id", IntegerType.BIGINT);
        OlapTable oldTable = table(1L, Lists.newArrayList(id, label, txnId));

        List<String> missing = TableKeeper.missingColumnDefinitions(oldTable, LoadsHistorySyncer.HISTORY_COLUMNS);
        Assertions.assertEquals(LoadsHistorySyncer.HISTORY_COLUMNS.size() - 3, missing.size());
        Assertions.assertTrue(missing.stream().noneMatch(def -> def.startsWith("`id`")
                || def.startsWith("`label`") || def.startsWith("`txn_id`")));
        Assertions.assertTrue(missing.contains("`load_id` varchar(2048)"));
        Assertions.assertTrue(missing.contains("`visible_time` datetime"));
        Assertions.assertEquals("`peak_mem_bytes` bigint", missing.get(missing.size() - 1));

        String alterSql = TableKeeper.alterTableAddColumns("_statistics_", "loads_history", missing);
        Assertions.assertTrue(alterSql.startsWith("ALTER TABLE _statistics_.loads_history ADD COLUMN ("), alterSql);
        Assertions.assertTrue(alterSql.endsWith("`peak_mem_bytes` bigint)"), alterSql);

        // Column order in the existing table does not matter once every column is present.
        List<Column> shuffled = Lists.newArrayList();
        for (Pair<String, String> column : Lists.reverse(LoadsHistorySyncer.HISTORY_COLUMNS)) {
            shuffled.add(new Column(column.first, IntegerType.BIGINT));
        }
        Assertions.assertTrue(
                TableKeeper.missingColumnDefinitions(table(2L, shuffled), LoadsHistorySyncer.HISTORY_COLUMNS).isEmpty());
    }

    @Test
    public void testBackstopSqlSkipsRowsAlreadyWritten() {
        String sql = LoadsHistorySyncer.SQLBuilder.buildSyncSql();
        Assertions.assertTrue(sql.contains(" FROM information_schema.loads l WHERE "), sql);
        // The settle delay stays; a fixed look-back window replaces the MAX() watermark so that
        // a row missed once can still be picked up, and rows already written are excluded by id.
        Assertions.assertTrue(sql.contains("l.load_finish_time IS NOT NULL"), sql);
        Assertions.assertTrue(sql.contains("l.load_finish_time < NOW() - INTERVAL 60 SECOND"), sql);
        Assertions.assertTrue(sql.contains("l.load_finish_time > NOW() - INTERVAL "
                + Config.loads_history_backstop_lookback_minutes + " MINUTE"), sql);
        Assertions.assertFalse(sql.contains("MAX("), sql);
        Assertions.assertTrue(sql.contains(
                "AND NOT (l.db_name = '_statistics_' AND l.table_name IN ('loads_history', 'load_jobs'))"), sql);
        Assertions.assertTrue(sql.endsWith(
                "AND NOT EXISTS (SELECT 1 FROM _statistics_.loads_history h WHERE h.id = l.id);"), sql);

        // The look-back always reaches past the settle window.
        int saved = Config.loads_history_backstop_lookback_minutes;
        try {
            Config.loads_history_backstop_lookback_minutes = 0;
            Assertions.assertTrue(LoadsHistorySyncer.SQLBuilder.buildSyncSql().contains("NOW() - INTERVAL 2 MINUTE"));
        } finally {
            Config.loads_history_backstop_lookback_minutes = saved;
        }
    }

    private static TLoadInfo finishedRow(long id) {
        TLoadInfo info = new TLoadInfo();
        info.setJob_id(id);
        info.setLabel("label_" + id);
        info.setDb("db");
        info.setTable("t");
        info.setState("FINISHED");
        info.setLoad_finish_time_ms(1_700_000_000_000L + id);
        return info;
    }

    // A writer and a syncer on one clock that only moves when the test moves it.
    private static LoadsHistorySyncer syncerOn(AtomicLong now, LoadsHistoryWriter writer, List<String> events) {
        writer.setClock(now::get, now::addAndGet);
        LoadsHistorySyncer syncer = new LoadsHistorySyncer(writer, () -> events.add("BACKSTOP"), () -> true);
        syncer.setClock(now::get);
        return syncer;
    }

    private static long backstops(List<String> events) {
        return events.stream().filter(event -> event.startsWith("BACKSTOP")).count();
    }

    // Like syncerOn, with what each backstop run was told: "BACKSTOP <excluded ids> <settle seconds>s <look-back minutes>m".
    private static LoadsHistorySyncer scopedSyncerOn(AtomicLong now, LoadsHistoryWriter writer, List<String> events) {
        writer.setClock(now::get, now::addAndGet);
        LoadsHistorySyncer syncer = new LoadsHistorySyncer(writer,
                (excludedIds, settleSeconds, lookbackMinutes) ->
                        events.add("BACKSTOP " + excludedIds + " " + settleSeconds + "s " + lookbackMinutes + "m"),
                () -> true);
        syncer.setClock(now::get);
        return syncer;
    }

    @Test
    public void testBacklogDoesNotKeepTheBackstopOff() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> events.add("FLUSH " + sql));
        LoadsHistorySyncer syncer = scopedSyncerOn(now, writer, events);
        String lookback = Config.loads_history_backstop_lookback_minutes + "m";

        // Startup: the queued row is written first, then the backstop runs; the row appears once.
        writer.enqueue(finishedRow(1));
        syncer.runCycle(10);
        Assertions.assertEquals(2, events.size());
        Assertions.assertTrue(events.get(0).startsWith("FLUSH "), events.get(0));
        Assertions.assertEquals("BACKSTOP [] 60s " + lookback, events.get(1));
        Assertions.assertEquals(1, events.get(0).split("'label_1'", -1).length - 1);
        Assertions.assertEquals(0, writer.queueSize());

        // The backstop is wanted once more, for loads that finished shortly before this FE took
        // over. A queue that is not drained (a full batch each cycle) must not postpone it: it
        // runs, and is told to leave out the load whose row still waits.
        now.addAndGet(LoadsHistoryWriter.SETTLE_MS + LoadsHistorySyncer.COVER_MARGIN_MS);
        int savedBatch = Config.loads_history_flush_batch_rows;
        try {
            Config.loads_history_flush_batch_rows = 1;
            events.clear();
            writer.enqueue(finishedRow(2));
            writer.enqueue(finishedRow(3));
            syncer.runCycle(10);
            Assertions.assertEquals(2, events.size(), events.toString());
            Assertions.assertTrue(events.get(0).startsWith("FLUSH ") && events.get(0).contains("'label_2'"));
            Assertions.assertEquals("BACKSTOP [3] 60s " + lookback, events.get(1));
            syncer.runCycle(10);
            syncer.runCycle(10);
            Assertions.assertEquals(3, events.size(), events.toString());
            Assertions.assertEquals(1, events.stream().filter(e -> e.contains("'label_2'")).count());
            Assertions.assertEquals(1, events.stream().filter(e -> e.contains("'label_3'")).count());
        } finally {
            Config.loads_history_flush_batch_rows = savedBatch;
        }
    }

    @Test
    public void testBackstopStaysBehindTheOldestQueuedRow() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> events.add("FLUSH"));
        LoadsHistorySyncer syncer = scopedSyncerOn(now, writer, events);
        int savedBatch = Config.loads_history_flush_batch_rows;
        try {
            Config.loads_history_flush_batch_rows = 1;
            // Two rows that arrived right after their loads finished wait for 100 seconds, and
            // only one of them is written in this cycle. The backstop considers loads that
            // finished a minute ago, so it has to stop before the load of the row still queued.
            for (long id = 1; id <= 2; id++) {
                TLoadInfo info = finishedRow(id);
                info.setLoad_finish_time_ms(now.get() - 10_000L);
                writer.enqueue(info);
            }
            now.addAndGet(100_000L);
            syncer.runCycle(0);
            long settleSeconds = (110_000L + LoadsHistorySyncer.COVER_MARGIN_MS) / 1000L;
            Assertions.assertEquals(List.of("FLUSH", "BACKSTOP [] " + settleSeconds + "s "
                    + Config.loads_history_backstop_lookback_minutes + "m"), events);
        } finally {
            Config.loads_history_flush_batch_rows = savedBatch;
        }
    }

    @Test
    public void testBackstopReachesBackToADroppedRowOlderThanTheLookBack() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        AtomicBoolean failing = new AtomicBoolean(true);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            if (failing.get()) {
                throw new RuntimeException("rejected");
            }
            events.add("FLUSH");
        });
        LoadsHistorySyncer syncer = scopedSyncerOn(now, writer, events);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            Config.loads_history_sync_interval_second = 0;
            // The row of a load that finished three hours ago is dropped after three failed writes.
            TLoadInfo lost = finishedRow(1);
            long finishedAt = now.get() - 180 * 60_000L;
            lost.setLoad_finish_time_ms(finishedAt);
            writer.enqueue(lost);
            for (int i = 0; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
                syncer.runCycle(0);
            }
            Assertions.assertEquals(1, writer.droppedTotal());
            Assertions.assertTrue(events.isEmpty(), events.toString());
            failing.set(false);
            // The run after the drop reaches back to that load, not only the configured look-back.
            now.addAndGet(70_000L);
            syncer.runCycle(0);
            Assertions.assertEquals(1, events.size(), events.toString());
            long lookbackMinutes = (now.get() - finishedAt) / 60_000L + 2;
            Assertions.assertTrue(lookbackMinutes > Config.loads_history_backstop_lookback_minutes);
            Assertions.assertEquals("BACKSTOP [] 60s " + lookbackMinutes + "m", events.get(0));
            // Covered: the next run is back to the configured look-back.
            now.addAndGet(70_000L);
            TLoadInfo dropped = finishedRow(2);
            dropped.setLoad_finish_time_ms(now.get());
            failing.set(true);
            writer.enqueue(dropped);
            for (int i = 0; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
                syncer.runCycle(0);
            }
            failing.set(false);
            now.addAndGet(70_000L);
            syncer.runCycle(0);
            Assertions.assertEquals("BACKSTOP [] 60s " + Config.loads_history_backstop_lookback_minutes + "m",
                    events.get(events.size() - 1));
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testStartupBackstopCoversLoadsThatHadNotSettled() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> events.add("FLUSH"));
        LoadsHistorySyncer syncer = syncerOn(now, writer, events);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            // An interval far below the settle window must not make the second run come too early.
            Config.loads_history_sync_interval_second = 10;
            syncer.runCycle(10);
            Assertions.assertEquals(1, backstops(events));
            // Loads that finished within the last minute before this FE took over were skipped
            // by that run. Runs an interval later would skip the youngest of them as well.
            for (int i = 0; i < 6; i++) {
                now.addAndGet(10_000L);
                syncer.runCycle(10);
                Assertions.assertEquals(1, backstops(events), "after " + (i + 1) * 10 + "s");
            }
            // Once the takeover lies past the settle window one more run sees them all.
            now.addAndGet(10_000L);
            syncer.runCycle(10);
            Assertions.assertEquals(2, backstops(events));
            // Nothing else was left to the backstop: it does not run again.
            for (int i = 0; i < 5; i++) {
                now.addAndGet(120_000L);
                syncer.runCycle(10);
            }
            Assertions.assertEquals(2, backstops(events));
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testDroppedRowKeepsTheBackstopPendingPastTheSettleWindow() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        AtomicBoolean failing = new AtomicBoolean(false);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            if (failing.get()) {
                throw new RuntimeException("backend down");
            }
            events.add("FLUSH");
        });
        LoadsHistorySyncer syncer = syncerOn(now, writer, events);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            Config.loads_history_sync_interval_second = 10;
            // Get the two startup runs out of the way.
            syncer.runCycle(0);
            now.addAndGet(70_000L);
            syncer.runCycle(0);
            Assertions.assertEquals(2, backstops(events));

            // The table rejects the row of a load that just finished until the row is dropped.
            failing.set(true);
            TLoadInfo lost = finishedRow(1);
            lost.setLoad_finish_time_ms(now.get());
            writer.enqueue(lost);
            for (int i = 0; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
                syncer.runCycle(0);
            }
            Assertions.assertEquals(0, writer.queueSize());
            Assertions.assertEquals(1, writer.droppedTotal());
            long droppedAt = now.get();
            Assertions.assertEquals(droppedAt, writer.lastBackstopRequestMs());
            failing.set(false);

            // The queue is empty and the interval has passed, but the backstop would skip a load
            // that finished less than a minute ago. It must not run now and consider the loss handled.
            now.addAndGet(20_000L);
            syncer.runCycle(0);
            now.addAndGet(20_000L);
            syncer.runCycle(0);
            Assertions.assertEquals(2, backstops(events));

            // Past the settle window it runs, once.
            now.addAndGet(30_000L);
            syncer.runCycle(0);
            Assertions.assertEquals(3, backstops(events));
            now.addAndGet(120_000L);
            syncer.runCycle(0);
            Assertions.assertEquals(3, backstops(events));
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testSteadyRequestsDoNotPostponeTheBackstop() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        // A queue of one row: every second row of a cycle pushes the first one out.
        LoadsHistoryWriter writer = new LoadsHistoryWriter(1, (sql, rows) -> events.add("FLUSH"));
        LoadsHistorySyncer syncer = syncerOn(now, writer, events);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            Config.loads_history_sync_interval_second = 10;
            // A row is left to the backstop every 30 seconds, more often than the settle window
            // is long. The backstop must not wait for that to stop.
            List<Long> requestedAt = Lists.newArrayList();
            List<Long> ranAt = Lists.newArrayList();
            long id = 0;
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 2; j++) {
                    TLoadInfo info = finishedRow(++id);
                    info.setLoad_finish_time_ms(now.get());
                    writer.enqueue(info);
                }
                requestedAt.add(now.get());
                long before = backstops(events);
                syncer.runCycle(0);
                if (backstops(events) > before) {
                    ranAt.add(now.get());
                }
                now.addAndGet(30_000L);
            }
            Assertions.assertEquals(20, writer.droppedTotal());
            long settleMs = LoadsHistoryWriter.SETTLE_MS + LoadsHistorySyncer.COVER_MARGIN_MS;
            // Every row that was left to the backstop early enough got a run that could see its
            // load, at most two cycles after the load had settled.
            for (long requested : requestedAt) {
                if (requested + settleMs + 60_000L > now.get()) {
                    continue;
                }
                Assertions.assertTrue(ranAt.stream().anyMatch(
                        ran -> ran >= requested + settleMs && ran <= requested + settleMs + 60_000L),
                        "no backstop run for the row dropped at " + requested + ": " + ranAt);
            }

            // Once nothing is left to it any more it runs until the last row is covered, and stops.
            for (int i = 0; i < 6; i++) {
                syncer.runCycle(0);
                now.addAndGet(30_000L);
            }
            Assertions.assertEquals(0L, writer.oldestBackstopRequestMs());
            long settled = backstops(events);
            for (int i = 0; i < 6; i++) {
                syncer.runCycle(0);
                now.addAndGet(30_000L);
            }
            Assertions.assertEquals(settled, backstops(events));
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testBackstopLeavesOutHeldAndLateRows() throws Exception {
        Assertions.assertTrue(LoadsHistorySyncer.SQLBuilder.buildSyncSql(List.of(5L, 9L), 75, 90).contains(
                "l.load_finish_time < NOW() - INTERVAL 75 SECOND AND l.load_finish_time > NOW() - INTERVAL 90 MINUTE"));
        Assertions.assertTrue(LoadsHistorySyncer.SQLBuilder.buildSyncSql(List.of(5L, 9L)).endsWith(
                "AND l.id NOT IN (5, 9) AND NOT EXISTS (SELECT 1 FROM _statistics_.loads_history h WHERE h.id = l.id);"));
        Assertions.assertFalse(LoadsHistorySyncer.SQLBuilder.buildSyncSql(List.of()).contains("NOT IN (5"));

        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        List<String> events = Lists.newArrayList();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> events.add("FLUSH " + sql));
        writer.setClock(now::get, now::addAndGet);
        LoadsHistorySyncer syncer = new LoadsHistorySyncer(writer,
                (excludedIds, settleSeconds, lookbackMinutes) -> events.add("BACKSTOP " + excludedIds), () -> true);
        syncer.setClock(now::get);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            Config.loads_history_sync_interval_second = 0;
            // An INSERT that is FINISHED while its transaction is only committed: the writer
            // holds its row, and the backstop runs of the takeover leave the load out although
            // it finished long enough ago to be copied.
            TLoadInfo pending = finishedRow(42);
            pending.setLoad_finish_time_ms(now.get() - 5 * 60_000L);
            Assertions.assertTrue(writer.hold(42L, () -> pending));
            syncer.runCycle(0);
            now.addAndGet(70_000L);
            syncer.runCycle(0);
            Assertions.assertEquals(List.of("BACKSTOP [42]", "BACKSTOP [42]"), events);

            // The transaction becomes visible: the next cycle writes the row, once, with the
            // visible time, and no backstop run could have copied the load before.
            long visibleMs = now.get();
            pending.setVisible_time_ms(visibleMs);
            now.addAndGet(1000L);
            syncer.runCycle(0);
            Assertions.assertEquals(3, events.size(), events.toString());
            Assertions.assertTrue(events.get(2).startsWith("FLUSH "), events.get(2));
            Assertions.assertEquals(1, events.get(2).split("'label_42'", -1).length - 1);
            Assertions.assertTrue(events.get(2).contains("'" + LoadsHistoryWriter.formatDatetime(visibleMs) + "'"));
            Assertions.assertEquals(0, writer.heldRows());
            Assertions.assertTrue(writer.beginBackstop().excludedIds.isEmpty());
            writer.endBackstop();
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testFailedBackstopStaysPending() throws Exception {
        AtomicLong now = new AtomicLong(1_800_000_000_000L);
        AtomicInteger attempts = new AtomicInteger();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> { });
        writer.setClock(now::get, now::addAndGet);
        LoadsHistorySyncer syncer = new LoadsHistorySyncer(writer, () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("no backend");
            }
        }, () -> true);
        syncer.setClock(now::get);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            // A non-positive interval still leaves a second between attempts.
            Config.loads_history_sync_interval_second = 0;
            syncer.runCycle(0);
            syncer.runCycle(0);
            Assertions.assertEquals(1, attempts.get());
            now.addAndGet(1000L);
            syncer.runCycle(0);
            Assertions.assertEquals(2, attempts.get());
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testSplitBatchRowsStayOutOfTheBackstop() throws Exception {
        List<String> events = Lists.newArrayList();
        // The table rejects whatever statement carries row 4, so [1..4] fails three times and
        // is then written in halves; after the first half succeeds rows 3 and 4 still wait.
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            if (sql.contains("'label_4'")) {
                throw new RuntimeException("rejected");
            }
            events.add("FLUSH " + sql);
        });
        LoadsHistorySyncer syncer = scopedSyncerOn(new AtomicLong(1_800_000_000_000L), writer, events);
        int savedInterval = Config.loads_history_sync_interval_second;
        try {
            Config.loads_history_sync_interval_second = 0;
            for (int i = 1; i <= 4; i++) {
                writer.enqueue(finishedRow(i));
            }
            // A failed flush keeps the backstop off; the cycle that writes the first half lets
            // it run, without the loads whose rows remain.
            for (int i = 0; i < LoadsHistoryWriter.MAX_BATCH_ATTEMPTS; i++) {
                syncer.runCycle(0);
            }
            Assertions.assertTrue(events.isEmpty(), events.toString());
            syncer.runCycle(0);
            Assertions.assertEquals(2, events.size(), events.toString());
            Assertions.assertTrue(events.get(0).startsWith("FLUSH ") && events.get(0).contains("'label_2'"));
            Assertions.assertTrue(events.get(1).startsWith("BACKSTOP [3, 4] "), events.get(1));
            Assertions.assertEquals(2, writer.queueSize());

            // Row 4 is dropped in the end, and still every other row was written once.
            for (int i = 0; i < 20 && writer.queueSize() > 0; i++) {
                syncer.runCycle(0);
            }
            Assertions.assertEquals(0, writer.queueSize());
            Assertions.assertEquals(1, writer.droppedTotal());
            for (int i = 1; i <= 3; i++) {
                String label = "'label_" + i + "'";
                Assertions.assertEquals(1, events.stream().filter(e -> e.contains(label)).count(), label);
            }
            Assertions.assertTrue(writer.beginBackstop().excludedIds.isEmpty());
            writer.endBackstop();
        } finally {
            Config.loads_history_sync_interval_second = savedInterval;
        }
    }

    @Test
    public void testFailedFlushSkipsTheBackstop() throws Exception {
        List<String> events = Lists.newArrayList();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(100, (sql, rows) -> {
            throw new RuntimeException("backend down");
        });
        LoadsHistorySyncer syncer = new LoadsHistorySyncer(writer, () -> events.add("BACKSTOP"), () -> true);
        writer.enqueue(finishedRow(1));
        syncer.runCycle(10);
        Assertions.assertTrue(events.isEmpty());
        Assertions.assertEquals(1, writer.queueSize());
    }

    @Test
    public void testWriterIsInactiveUntilTheDaemonRuns() {
        LoadsHistorySyncer syncer = new LoadsHistorySyncer();
        Assertions.assertEquals(0L, syncer.getInterval());
        Assertions.assertEquals(0L, LoadsHistoryWriter.queuedRows());
        Assertions.assertEquals(0, syncer.getWriter().queueSize());
        LoadsHistoryWriter.activate(syncer.getWriter());
        try {
            Assertions.assertEquals(0L, LoadsHistoryWriter.queuedRows());
        } finally {
            LoadsHistoryWriter.deactivate(syncer.getWriter());
        }
    }
}
