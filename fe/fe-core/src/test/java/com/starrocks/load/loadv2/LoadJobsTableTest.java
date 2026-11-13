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
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.FailMsg;
import com.starrocks.load.routineload.ErrorReason;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class LoadJobsTableTest {

    private static final long CREATE_MS = 1_700_000_000_000L;
    // The UTC day of CREATE_MS, whatever time zone the test runs in.
    private static final String CREATE_DAY = "2023-11-14";

    private final List<String> executed = Lists.newArrayList();
    private final LoadsHistoryWriter.Sink recording = (sql, rows) -> executed.add(sql);

    @AfterEach
    public void tearDown() {
        LoadJobsTable.activate(null);
        LoadsHistoryWriter.activate(null);
    }

    private static TLoadInfo row(long id, String type, String state) {
        TLoadInfo info = new TLoadInfo();
        info.setJob_id(id);
        info.setLabel("label_" + id);
        info.setDb("test_db");
        info.setTable("t1");
        info.setUser("root");
        info.setState(state);
        info.setType(type);
        info.setCreate_time_ms(CREATE_MS);
        return info;
    }

    /** Assert that the queued rows carry these states, in this order. */
    private static void assertStates(List<LoadsHistoryWriter.Row> rows, String... states) {
        Assertions.assertEquals(states.length, rows.size(), rows.toString());
        for (int i = 0; i < states.length; i++) {
            Assertions.assertTrue(rows.get(i).values.contains("'" + states[i] + "'"), rows.get(i).values);
        }
    }

    private LoadsHistoryWriter activateJobsWriter() {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, recording);
        LoadJobsTable.activate(writer);
        return writer;
    }

    @Test
    public void testCreateTableIsPrimaryKeyOnCreateDayEntryId() {
        String ddl = LoadJobsTable.CREATE_TABLE;
        Assertions.assertTrue(ddl.startsWith("CREATE TABLE IF NOT EXISTS _statistics_.load_jobs ("
                + "`create_day` date not null, `entry` varchar(32) not null, `id` bigint not null, "
                + "`level` varchar(16) not null, `label` varchar(2048), `profile_id` varchar(2048), "), ddl);
        Assertions.assertTrue(ddl.contains("`load_finish_time` datetime, "), ddl);
        Assertions.assertTrue(ddl.contains("`create_time` datetime, "), ddl);
        Assertions.assertTrue(ddl.contains(") PRIMARY KEY (`create_day`, `entry`, `id`) "
                + "PARTITION BY date_trunc('DAY', create_day) DISTRIBUTED BY HASH(`id`) BUCKETS 3 PROPERTIES( "
                + "'replication_num' = '1')"), ddl);
        // The table is expired by the syncer, row by row where needed, not by a partition TTL
        // that would drop the row of a load that is still running.
        Assertions.assertFalse(ddl.contains("partition_live_number"), ddl);

        // Every loads column is present once, plus create_day, entry and level.
        Assertions.assertEquals(LoadsHistorySyncer.HISTORY_COLUMNS.size() + 3, LoadJobsTable.COLUMNS.size());
        List<String> names = LoadJobsTable.COLUMNS.stream().map(c -> c.first).collect(Collectors.toList());
        Assertions.assertEquals(names.size(), names.stream().distinct().count());
        for (var column : LoadsHistorySyncer.HISTORY_COLUMNS) {
            Assertions.assertTrue(names.contains(column.first), column.first);
        }

        TableKeeper keeper = LoadJobsTable.createKeeper();
        Assertions.assertEquals("_statistics_", keeper.getDatabaseName());
        Assertions.assertEquals("load_jobs", keeper.getTableName());
        Assertions.assertEquals(ddl, keeper.getCreateTableSql());
    }

    @Test
    public void testInsertNamesEveryColumnInTableOrder() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, recording);
        writer.enqueue(row(1, "BROKER", "LOADING"));
        writer.enqueue(row(2, "INSERT", "FINISHED"));
        Assertions.assertEquals(LoadsHistoryWriter.FlushResult.DRAINED, writer.runOnce(500, 10));
        String sql = executed.get(0);
        Assertions.assertTrue(sql.startsWith("INSERT INTO _statistics_.load_jobs (`create_day`, `entry`, `id`, `level`, "
                + "`label`, `profile_id`, "), sql);
        Assertions.assertTrue(sql.contains("`cpu_ns`, `peak_mem_bytes`) VALUES ('" + CREATE_DAY
                + "', 'BROKER', 1, 'JOB', 'label_1', NULL, 'test_db', 't1', 'root', "), sql);
        Assertions.assertTrue(sql.contains("), ('" + CREATE_DAY + "', 'INSERT', 2, 'JOB', 'label_2', "), sql);
        // create_time itself is an ordinary column, rendered like the other datetime columns.
        Assertions.assertTrue(sql.contains("'" + LoadsHistoryWriter.formatDatetime(CREATE_MS) + "'"), sql);
        Assertions.assertTrue(sql.startsWith(LoadJobsTable.INSERT_PREFIX), sql);
        // One value per column, so the full-row replace never leaves a column at its old value.
        Assertions.assertEquals(LoadJobsTable.COLUMNS.size(), LoadJobsTable.renderRow(row(1, "BROKER", "LOADING"))
                .split(", ").length);
        Assertions.assertEquals(LoadJobsTable.columnList().split(", ").length, LoadJobsTable.COLUMNS.size());
    }

    @Test
    public void testEntryAndLevelPerEntryType() {
        Assertions.assertTrue(LoadJobsTable.renderRow(row(1, "BROKER", "LOADING")).startsWith(
                "('" + CREATE_DAY + "', 'BROKER', 1, 'JOB', "));
        Assertions.assertTrue(LoadJobsTable.renderRow(row(2, "SPARK", "ETL")).startsWith(
                "('" + CREATE_DAY + "', 'SPARK', 2, 'JOB', "));
        Assertions.assertTrue(LoadJobsTable.renderRow(row(3, "INSERT", "LOADING")).startsWith(
                "('" + CREATE_DAY + "', 'INSERT', 3, 'JOB', "));
        Assertions.assertTrue(LoadJobsTable.renderRow(row(4, "STREAM_LOAD", "PREPARED")).startsWith(
                "('" + CREATE_DAY + "', 'STREAM_LOAD', 4, 'JOB', "));
        Assertions.assertTrue(LoadJobsTable.renderRow(row(5, "PARALLEL_STREAM_LOAD", "LOADING")).startsWith(
                "('" + CREATE_DAY + "', 'STREAM_LOAD', 5, 'JOB', "));

        TLoadInfo routineTask = row(6, "ROUTINE_LOAD", "COMMITED");
        routineTask.setParent_id(60);
        Assertions.assertTrue(LoadJobsTable.renderRow(routineTask).startsWith(
                "('" + CREATE_DAY + "', 'ROUTINE_LOAD', 6, 'TASK', "));

        TLoadInfo routineJob = row(60, "ROUTINE_LOAD", "PAUSED");
        routineJob.setError_msg("ErrorReason{errCode = 4, msg='too many errors'}");
        String rendered = LoadJobsTable.renderRow(routineJob);
        Assertions.assertTrue(rendered.startsWith("('" + CREATE_DAY + "', 'ROUTINE_LOAD', 60, 'JOB', "), rendered);
        // JOB rows carry no counters: they render as NULL rather than 0.
        Assertions.assertTrue(rendered.contains("'PAUSED', '', 'ROUTINE_LOAD', '', NULL, NULL, NULL, NULL, NULL, NULL, "),
                rendered);
        Assertions.assertTrue(rendered.contains("'ErrorReason{errCode = 4, msg=\\'too many errors\\'}'"), rendered);

        TLoadInfo mergeCommit = row(7, "MERGE_COMMIT", "LOADING");
        Assertions.assertTrue(LoadJobsTable.renderRow(mergeCommit).startsWith(
                "('" + CREATE_DAY + "', 'MERGE_COMMIT', 7, 'TASK', "));

        TLoadInfo unknownType = row(8, "", "UNKNOWN");
        Assertions.assertTrue(LoadJobsTable.renderRow(unknownType).startsWith(
                "('" + CREATE_DAY + "', 'UNKNOWN', 8, 'JOB', "));

        // Rows with counters keep them.
        TLoadInfo counted = row(9, "BROKER", "FINISHED");
        counted.setNum_scan_rows(10);
        counted.setNum_scan_bytes(100);
        counted.setNum_filtered_rows(1);
        counted.setNum_unselected_rows(2);
        counted.setNum_sink_rows(7);
        Assertions.assertTrue(LoadJobsTable.renderRow(counted).contains("'FINISHED', '', 'BROKER', '', 10, 100, 1, 2, 7, "));
    }

    @Test
    public void testTargetAcceptsRunningRowsAndSkipsInternalTables() {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, recording);
        writer.enqueue(row(1, "BROKER", "LOADING"));
        Assertions.assertEquals(1, writer.queueSize());

        TLoadInfo noCreateTime = row(2, "BROKER", "LOADING");
        noCreateTime.unsetCreate_time_ms();
        writer.enqueue(noCreateTime);
        TLoadInfo selfWrite = row(3, "INSERT", "LOADING");
        selfWrite.setDb(LoadJobsTable.DB_NAME);
        selfWrite.setTable(LoadJobsTable.TABLE_NAME);
        writer.enqueue(selfWrite);
        TLoadInfo historyWrite = row(4, "INSERT", "LOADING");
        historyWrite.setDb(LoadsHistorySyncer.LOADS_HISTORY_DB_NAME);
        historyWrite.setTable(LoadsHistorySyncer.LOADS_HISTORY_TABLE_NAME);
        writer.enqueue(historyWrite);
        writer.enqueue(null);
        Assertions.assertEquals(1, writer.queueSize());

        // The history writer keeps taking only finished rows, and also skips writes to load_jobs.
        LoadsHistoryWriter history = new LoadsHistoryWriter(100, recording);
        history.enqueue(row(1, "BROKER", "LOADING"));
        TLoadInfo finishedSelfWrite = row(5, "INSERT", "FINISHED");
        finishedSelfWrite.setLoad_finish_time_ms(CREATE_MS + 1);
        finishedSelfWrite.setDb(LoadJobsTable.DB_NAME);
        finishedSelfWrite.setTable(LoadJobsTable.TABLE_NAME);
        history.enqueue(finishedSelfWrite);
        Assertions.assertEquals(0, history.queueSize());
        TLoadInfo finished = row(6, "BROKER", "FINISHED");
        finished.setLoad_finish_time_ms(CREATE_MS + 1);
        history.enqueue(finished);
        Assertions.assertEquals(1, history.queueSize());
    }

    @Test
    public void testQueuedRowsOfOneKeyAreCoalesced() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, recording);
        writer.enqueue(row(1, "BROKER", "PENDING"));
        writer.enqueue(row(2, "BROKER", "PENDING"));
        writer.enqueue(row(1, "BROKER", "LOADING"));
        TLoadInfo finished = row(1, "BROKER", "FINISHED");
        finished.setError_msg("a longer row than the one it replaces");
        writer.enqueue(finished);

        // One row per key waits, in the place of the first one, and the bounds count what is queued.
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(2, writer.coalescedTotal());
        Assertions.assertEquals(0, writer.droppedTotal());
        Assertions.assertEquals(LoadJobsTable.renderRow(finished).length()
                + LoadJobsTable.renderRow(row(2, "BROKER", "PENDING")).length(), writer.queuedBytes());

        Assertions.assertEquals(LoadsHistoryWriter.FlushResult.DRAINED, writer.runOnce(500, 10));
        Assertions.assertEquals(1, executed.size());
        String sql = executed.get(0);
        Assertions.assertTrue(sql.startsWith("INSERT INTO _statistics_.load_jobs ("), sql);
        // The statement carries each key once, with its latest state.
        Assertions.assertEquals(1, count(sql, "'BROKER', 1, 'JOB'"), sql);
        Assertions.assertEquals(1, count(sql, "'BROKER', 2, 'JOB'"), sql);
        Assertions.assertTrue(sql.contains("'FINISHED'"), sql);
        Assertions.assertFalse(sql.contains("'LOADING'"), sql);
        Assertions.assertTrue(sql.indexOf("'BROKER', 1, 'JOB'") < sql.indexOf("'BROKER', 2, 'JOB'"), sql);
        Assertions.assertEquals(0, writer.queuedBytes());

        // Once written, the key is free again: the next state is a new row.
        writer.enqueue(row(1, "BROKER", "CANCELLED"));
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertEquals(2, writer.coalescedTotal());

        // A different entry or creation day is a different key; the history table has none.
        Assertions.assertNotEquals(LoadJobsTable.keyOf(row(1, "BROKER", "LOADING")),
                LoadJobsTable.keyOf(row(1, "INSERT", "LOADING")));
        TLoadInfo later = row(1, "BROKER", "LOADING");
        later.setCreate_time_ms(CREATE_MS + 86_400_000L);
        Assertions.assertNotEquals(LoadJobsTable.keyOf(row(1, "BROKER", "LOADING")), LoadJobsTable.keyOf(later));
        Assertions.assertEquals(CREATE_DAY + "|BROKER|1", LoadJobsTable.keyOf(row(1, "BROKER", "FINISHED")));
        Assertions.assertNull(LoadsHistoryWriter.HISTORY_TARGET.keyOf(row(1, "BROKER", "FINISHED")));
    }

    @Test
    public void testFailedBatchDoesNotOverwriteANewerRowOfItsKey() throws Exception {
        List<String> written = Lists.newArrayList();
        LoadsHistoryWriter[] self = new LoadsHistoryWriter[1];
        // 0: the write succeeds, 1: it fails, 2: it fails after load 1 finished meanwhile.
        int[] mode = {2};
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> {
            if (mode[0] == 2) {
                self[0].enqueue(row(1, "BROKER", "FINISHED"));
            }
            if (mode[0] != 0) {
                throw new RuntimeException("backend down");
            }
            written.add(sql);
        });
        self[0] = writer;
        writer.setClock(System::currentTimeMillis, millis -> { });
        writer.enqueue(row(1, "BROKER", "LOADING"));
        writer.enqueue(row(2, "BROKER", "LOADING"));

        Assertions.assertEquals(LoadsHistoryWriter.FlushResult.FAILED, writer.runOnce(500, 10));
        // The failed batch goes back in front, except the row a newer one of the same key
        // replaced: writing LOADING after FINISHED would undo the final state.
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(1, writer.coalescedTotal());
        Assertions.assertEquals(0, writer.droppedTotal());
        mode[0] = 0;
        Assertions.assertEquals(LoadsHistoryWriter.FlushResult.DRAINED, writer.runOnce(500, 10));
        Assertions.assertEquals(1, written.size());
        String sql = written.get(0);
        Assertions.assertEquals(1, count(sql, "'BROKER', 1, 'JOB'"), sql);
        Assertions.assertTrue(sql.contains("'FINISHED'"), sql);
        Assertions.assertTrue(sql.indexOf("'BROKER', 2, 'JOB'") < sql.indexOf("'BROKER', 1, 'JOB'"), sql);
        Assertions.assertEquals(0, writer.queuedBytes());

        // Without a newer row the failed batch is retried as it was, in its order, and the row
        // that went back can still be replaced while it waits.
        writer.enqueue(row(3, "BROKER", "PENDING"));
        writer.enqueue(row(4, "BROKER", "PENDING"));
        mode[0] = 1;
        Assertions.assertEquals(LoadsHistoryWriter.FlushResult.FAILED, writer.runOnce(500, 10));
        Assertions.assertEquals(2, writer.queueSize());
        writer.enqueue(row(3, "BROKER", "LOADING"));
        Assertions.assertEquals(2, writer.queueSize());
        mode[0] = 0;
        Assertions.assertEquals(LoadsHistoryWriter.FlushResult.DRAINED, writer.runOnce(500, 10));
        Assertions.assertEquals(2, written.size());
        sql = written.get(1);
        Assertions.assertTrue(sql.indexOf("'BROKER', 3, 'JOB'") < sql.indexOf("'BROKER', 4, 'JOB'"), sql);
        Assertions.assertTrue(sql.contains("'LOADING'"), sql);
        Assertions.assertEquals(1, count(sql, "'PENDING'"), sql);
        Assertions.assertEquals(0, writer.queueSize());
        Assertions.assertEquals(0, writer.queuedBytes());
    }

    @Test
    public void testKeyAndRowDoNotDependOnTheSessionTimeZoneOfTheThread() throws Exception {
        TLoadInfo info = row(1, "BROKER", "PENDING");
        info.setLoad_start_time_ms(CREATE_MS + 5000);
        String key = LoadJobsTable.keyOf(info);
        String rendered = LoadJobsTable.renderRow(info);

        // The statement that creates a load runs on the thread of a user session, later state
        // changes on daemon threads; whatever time_zone the session has, the key must not move.
        for (String zone : List.of("UTC", "Asia/Tokyo", "America/Los_Angeles")) {
            String[] fromSession = new String[2];
            Thread session = new Thread(() -> {
                ConnectContext context = new ConnectContext();
                context.getSessionVariable().setTimeZone(zone);
                context.setThreadLocalInfo();
                try {
                    fromSession[0] = LoadJobsTable.keyOf(info);
                    fromSession[1] = LoadJobsTable.renderRow(info);
                } finally {
                    ConnectContext.remove();
                }
            });
            session.start();
            session.join();
            Assertions.assertEquals(key, fromSession[0], zone);
            Assertions.assertEquals(rendered, fromSession[1], zone);
        }

        // So a row offered from a session thread and one from a daemon thread are one row.
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, recording);
        Thread session = new Thread(() -> {
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().setTimeZone("UTC");
            context.setThreadLocalInfo();
            try {
                writer.enqueue(row(1, "BROKER", "PENDING"));
            } finally {
                ConnectContext.remove();
            }
        });
        session.start();
        session.join();
        writer.enqueue(row(1, "BROKER", "LOADING"));
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertTrue(writer.takeBatch(10, 0).get(0).values.startsWith("('" + CREATE_DAY + "', 'BROKER', 1, "));
    }

    private static int count(String text, String part) {
        int n = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
            n++;
        }
        return n;
    }

    @Test
    public void testNonFinalRowsSql() {
        // Routine load JOB rows have no timeout; every other row uses its own timeout, else the
        // default of its entry point. The time is a literal, not NOW(), which follows the session.
        String select = "SELECT `id`, `state`, IF((`entry` = 'ROUTINE_LOAD' AND `level` = 'JOB') "
                + "OR seconds_add(`create_time`, COALESCE(get_json_int(`properties`, '$.timeout'), "
                + "CASE `entry` WHEN 'STREAM_LOAD' THEN 600 WHEN 'MERGE_COMMIT' THEN 600 WHEN 'ROUTINE_LOAD' THEN 60 "
                + "ELSE 259200 END)) < '2026-11-09 10:00:00', 1, 0) FROM _statistics_.load_jobs "
                + "WHERE `state` NOT IN ('CANCELLED', 'FINISHED', 'STOPPED', 'UNKNOWN')";
        Assertions.assertEquals(select + " LIMIT 100000",
                LoadJobsTable.buildNonFinalRowsSql(null, "2026-11-09 10:00:00", 259200, 600, 60));
        // With a lower bound directly on the partition column only the recent partitions are read.
        Assertions.assertEquals(select + " AND `create_day` >= '2026-11-05' LIMIT 100000",
                LoadJobsTable.buildNonFinalRowsSql("2026-11-05", "2026-11-09 10:00:00", 259200, 600, 60));

        List<LoadJobsTable.NonFinalRow> rows = LoadJobsTable.toNonFinalRows(Lists.newArrayList(
                Lists.newArrayList("7", "LOADING", "1"),
                Lists.newArrayList("8", "RUNNING", "0"),
                Lists.newArrayList("not a number", "LOADING", "1"),
                Lists.newArrayList("9", null, "1"),
                Lists.newArrayList("10")));
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(7L, rows.get(0).id);
        Assertions.assertEquals("LOADING", rows.get(0).state);
        Assertions.assertTrue(rows.get(0).pastTimeout);
        Assertions.assertEquals(8L, rows.get(1).id);
        Assertions.assertFalse(rows.get(1).pastTimeout);
    }

    @Test
    public void testMarkUnknownSql() {
        String base = "UPDATE _statistics_.load_jobs SET `state` = 'UNKNOWN', `load_finish_time` = '2026-11-09 10:00:00' "
                + "WHERE `state` NOT IN ('CANCELLED', 'FINISHED', 'STOPPED', 'UNKNOWN')";
        Assertions.assertTrue(LoadJobsTable.buildMarkUnknownSql(Lists.newArrayList(), 10000, null,
                "2026-11-09 10:00:00").isEmpty());
        Assertions.assertEquals(Lists.newArrayList(base + " AND `id` IN (1, 2, 3)"),
                LoadJobsTable.buildMarkUnknownSql(Lists.newArrayList(3L, 1L, 2L, 1L), 10000, null, "2026-11-09 10:00:00"));
        // More ids than one statement carries, and the bound the ids were read with.
        String bounded = base + " AND `create_day` >= '2026-11-05'";
        Assertions.assertEquals(Lists.newArrayList(
                        bounded + " AND `id` IN (1, 2)",
                        bounded + " AND `id` IN (3, 4)",
                        bounded + " AND `id` IN (5)"),
                LoadJobsTable.buildMarkUnknownSql(Lists.newArrayList(5L, 4L, 3L, 2L, 1L), 2, "2026-11-05",
                        "2026-11-09 10:00:00"));
    }

    @Test
    public void testExpiryKeepsThePartitionsOfLoadsThatAreStillAlive() {
        List<LoadJobsTable.PartitionRange> partitions = Lists.newArrayList(
                new LoadJobsTable.PartitionRange("p20260701", "2026-07-01 00:00:00", "2026-07-02 00:00:00"),
                new LoadJobsTable.PartitionRange("p20261008", "2026-10-08 00:00:00", "2026-10-09 00:00:00"),
                new LoadJobsTable.PartitionRange("p20261009", "2026-10-09 00:00:00", "2026-10-10 00:00:00"),
                new LoadJobsTable.PartitionRange("p20261010", "2026-10-10 00:00:00", "2026-10-11 00:00:00"),
                new LoadJobsTable.PartitionRange("p20261109", "2026-11-09 00:00:00", "2026-11-10 00:00:00"));
        // A routine load job created on 2026-07-01 still runs, or ended less than the retention ago.
        List<String> liveDays = Lists.newArrayList("2026-07-01 00:00:00");
        String kept = "(`state` NOT IN ('CANCELLED', 'FINISHED', 'STOPPED', 'UNKNOWN') "
                + "OR COALESCE(`load_finish_time`, `create_time`) >= '2026-10-10 18:00:00')";
        // The partition bounds are UTC days, the finish times are in the zone rows are written in.
        Assertions.assertEquals("SELECT DISTINCT date_format(`create_day`, '%Y-%m-%d %H:%i:%s') "
                        + "FROM _statistics_.load_jobs WHERE `create_day` < '2026-10-10 10:00:00' AND " + kept,
                LoadJobsTable.buildLiveDaysSql("2026-10-10 10:00:00", "2026-10-10 18:00:00"));

        Assertions.assertEquals(Lists.newArrayList(
                        // Its partition stays, without the loads of that day whose retention has passed.
                        "DELETE FROM _statistics_.load_jobs WHERE `create_day` >= '2026-07-01 00:00:00' "
                                + "AND `create_day` < '2026-07-02 00:00:00' AND NOT " + kept,
                        // Partitions that ended before the cutoff and hold no row to keep are dropped.
                        "ALTER TABLE _statistics_.load_jobs DROP PARTITION IF EXISTS `p20261008` FORCE",
                        "ALTER TABLE _statistics_.load_jobs DROP PARTITION IF EXISTS `p20261009` FORCE"),
                // The partition of the cutoff day itself and the newer ones are not touched.
                LoadJobsTable.planExpiry(partitions, liveDays, "2026-10-10 10:00:00", "2026-10-10 18:00:00"));

        // Once the retention has passed since the job ended, the next pass drops its partition as well.
        Assertions.assertEquals("ALTER TABLE _statistics_.load_jobs DROP PARTITION IF EXISTS `p20260701` FORCE",
                LoadJobsTable.planExpiry(partitions, Lists.newArrayList(), "2026-10-10 10:00:00",
                        "2026-10-10 18:00:00").get(0));
    }

    @Test
    public void testKeyIsTheUtcDayOfCreationAndDoesNotFollowTheTimeZone() {
        // 2023-11-14 22:13:20 UTC is already the 15th in Asia/Shanghai, the default time_zone:
        // the key takes the day in UTC, so changing time_zone while a load runs cannot move it.
        Assertions.assertEquals("2023-11-14", LoadJobsTable.keyDay(CREATE_MS));
        Assertions.assertEquals("2023-11-14 22:13:20", LoadJobsTable.utcDatetime(CREATE_MS));
        Assertions.assertEquals("2023-11-15", LoadJobsTable.keyDay(CREATE_MS + 2 * 3600_000L));
        TLoadInfo info = row(1, "BROKER", "LOADING");
        TimeZone before = TimeZone.getDefault();
        try {
            for (String zone : List.of("UTC", "Asia/Tokyo", "America/Los_Angeles")) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                Assertions.assertEquals("2023-11-14|BROKER|1", LoadJobsTable.keyOf(info), zone);
            }
        } finally {
            TimeZone.setDefault(before);
        }
    }

    @Test
    public void testReplacingAQueuedRowKeepsTheQueueWithinItsBounds() {
        TLoadInfo small1 = row(1, "BROKER", "PENDING");
        TLoadInfo small2 = row(2, "BROKER", "PENDING");
        TLoadInfo small3 = row(3, "BROKER", "PENDING");
        long rowBytes = LoadJobsTable.renderRow(small1).length();
        AtomicLong maxBytes = new AtomicLong(3 * rowBytes + 10);
        AtomicInteger maxRows = new AtomicInteger(100);
        LoadsHistoryWriter writer =
                new LoadsHistoryWriter(LoadJobsTable.TARGET, maxRows::get, maxBytes::get, recording);
        writer.enqueue(small1);
        writer.enqueue(small2);
        writer.enqueue(small3);
        Assertions.assertEquals(3, writer.queueSize());

        // The newer row of load 3 is larger than the bound allows next to the other two: the
        // oldest row goes, as it would for a new row.
        TLoadInfo large = row(3, "BROKER", "CANCELLED");
        large.setError_msg("x".repeat(200));
        writer.enqueue(large);
        Assertions.assertEquals(2, writer.queueSize());
        Assertions.assertEquals(1, writer.droppedTotal());
        Assertions.assertTrue(writer.queuedBytes() <= maxBytes.get(), String.valueOf(writer.queuedBytes()));

        // A lowered row limit takes effect on a replacement as well.
        maxRows.set(1);
        writer.enqueue(row(3, "BROKER", "CANCELLED"));
        Assertions.assertEquals(1, writer.queueSize());
        Assertions.assertEquals(2, writer.droppedTotal());
        Assertions.assertEquals(LoadJobsTable.renderRow(row(3, "BROKER", "CANCELLED")).length(), writer.queuedBytes());
    }

    @Test
    public void testARetriedJobKeepsItsFirstCreationTimeAsKey() {
        BrokerLoadJob job = new BrokerLoadJob();
        Deencapsulation.setField(job, "createTimestamp", CREATE_MS);
        TLoadInfo before = new TLoadInfo();
        before.setCreate_time_ms(CREATE_MS);
        Assertions.assertSame(before, Deencapsulation.invoke(job, "keyedByFirstCreateTime", before));

        // A retry starts the job over with a new creation time; its row stays under the first one.
        job.reset();
        long retried = Deencapsulation.getField(job, "createTimestamp");
        Assertions.assertNotEquals(CREATE_MS, retried);
        TLoadInfo attempt = new TLoadInfo();
        attempt.setJob_id(1);
        attempt.setType("BROKER");
        attempt.setCreate_time_ms(retried);
        TLoadInfo keyed = Deencapsulation.invoke(job, "keyedByFirstCreateTime", attempt);
        Assertions.assertEquals(CREATE_MS, keyed.getCreate_time_ms());
        Assertions.assertEquals(retried, attempt.getCreate_time_ms());
        Assertions.assertEquals(CREATE_DAY + "|BROKER|1", LoadJobsTable.keyOf(keyed));
        // And under a second retry.
        job.reset();
        TLoadInfo again = new TLoadInfo(attempt);
        again.setCreate_time_ms(Deencapsulation.getField(job, "createTimestamp"));
        Assertions.assertEquals(CREATE_MS,
                ((TLoadInfo) Deencapsulation.invoke(job, "keyedByFirstCreateTime", again)).getCreate_time_ms());
    }

    @Test
    public void testTheCheckpointThreadRecordsNothing() throws Exception {
        LoadsHistoryWriter jobs = activateJobsWriter();
        LoadsHistoryWriter history = new LoadsHistoryWriter(100, recording);
        LoadsHistoryWriter.activate(history);
        TLoadInfo finished = row(1, "BROKER", "FINISHED");
        finished.setLoad_finish_time_ms(System.currentTimeMillis());
        Field checkpointThreadId = GlobalStateMgr.class.getDeclaredField("checkpointThreadId");
        checkpointThreadId.setAccessible(true);
        long savedId = checkpointThreadId.getLong(null);
        // A leader that writes its own image replays loads into a copy of the metadata on this
        // thread; nothing it does there is a state change.
        checkpointThreadId.setLong(null, Thread.currentThread().getId());
        try {
            LoadJobsTable.offer(() -> finished);
            LoadJobsTable.offerAll(() -> Lists.newArrayList(finished));
            LoadsHistoryWriter.offer(() -> finished);
            Assertions.assertFalse(LoadsHistoryWriter.holdUntilVisible(1L, () -> finished));
        } finally {
            checkpointThreadId.setLong(null, savedId);
        }
        Assertions.assertEquals(0, jobs.queueSize());
        Assertions.assertEquals(0, history.queueSize());
        LoadJobsTable.offer(() -> finished);
        Assertions.assertEquals(1, jobs.queueSize());
    }

    @Test
    public void testExistingIdsSql() {
        Assertions.assertTrue(LoadJobsTable.buildExistingIdsSql(Lists.newArrayList(), 10).isEmpty());
        Assertions.assertEquals(Lists.newArrayList(
                        "SELECT `id` FROM _statistics_.load_jobs WHERE `id` IN (1, 2)",
                        "SELECT `id` FROM _statistics_.load_jobs WHERE `id` IN (3)"),
                LoadJobsTable.buildExistingIdsSql(Lists.newArrayList(3L, 1L, 2L, 1L), 2));
    }

    @Test
    public void testLoadJobTransitionsEnqueueRows(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        LoadsHistoryWriter writer = activateJobsWriter();
        LoadsHistoryWriter history = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(history);

        LoadJob job = new BrokerLoadJob();
        Deencapsulation.setField(job, "createTimestamp", CREATE_MS);
        // The queue is taken after every transition: rows of one load coalesce while they wait.
        List<LoadsHistoryWriter.Row> rows = Lists.newArrayList();
        job.updateState(JobState.LOADING);
        rows.addAll(writer.takeBatch(10, 0));
        job.updateState(JobState.COMMITTED);
        rows.addAll(writer.takeBatch(10, 0));
        job.updateState(JobState.FINISHED);
        rows.addAll(writer.takeBatch(10, 0));
        // A LOADING job that has not started shows as QUEUEING and COMMITTED as PREPARED, as in the loads view.
        assertStates(rows, "QUEUEING", "PREPARED", "FINISHED");
        String keyPrefix = "('" + CREATE_DAY + "', 'BROKER', " + job.getId() + ", 'JOB', ";
        Assertions.assertTrue(rows.stream().allMatch(r -> r.values.startsWith(keyPrefix)), rows.toString());
        // The history table still only receives the final row.
        assertStates(history.takeBatch(10, 0), "FINISHED");

        LoadJobsTable.deactivate(writer);
        new BrokerLoadJob().updateState(JobState.LOADING);
        Assertions.assertEquals(0, writer.queueSize());
    }

    @Test
    public void testLoadJobCancelEnqueuesRow(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog)
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
        LoadsHistoryWriter writer = activateJobsWriter();
        LoadsHistoryWriter history = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(history);

        LoadJob job = new BrokerLoadJob();
        Deencapsulation.setField(job, "createTimestamp", CREATE_MS);
        job.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.USER_CANCEL, "cancelled by user"), false);
        // One row per table for the one transition.
        List<LoadsHistoryWriter.Row> rows = writer.takeBatch(10, 0);
        assertStates(rows, "CANCELLED");
        assertStates(history.takeBatch(10, 0), "CANCELLED");
        Assertions.assertTrue(rows.get(0).values.startsWith("('" + CREATE_DAY + "', 'BROKER', " + job.getId()
                + ", 'JOB', "), rows.get(0).values);
        Assertions.assertTrue(rows.get(0).values.contains("cancelled by user"), rows.get(0).values);
    }

    @Test
    public void testStreamLoadTaskCreationAndTransitionsEnqueueRows() throws Exception {
        LoadsHistoryWriter writer = activateJobsWriter();

        StreamLoadTask task = new StreamLoadTask(1L, new Database(), new OlapTable(), "label", "", "",
                100000L, CREATE_MS, false, WarehouseManager.DEFAULT_RESOURCE);
        new StreamLoadMgr().addLoadTask(task, false);
        TransactionState txnState = new TransactionState();
        txnState.setFinishTime(System.currentTimeMillis());
        LoadsHistoryWriter history = new LoadsHistoryWriter(10, recording);
        LoadsHistoryWriter.activate(history);
        List<LoadsHistoryWriter.Row> rows = Lists.newArrayList(writer.takeBatch(10, 0));
        task.afterPrepared(txnState);
        rows.addAll(writer.takeBatch(10, 0));
        task.afterCommitted(txnState);
        rows.addAll(writer.takeBatch(10, 0));
        task.afterVisible(txnState);
        rows.addAll(writer.takeBatch(10, 0));
        assertStates(rows, "BEGIN", "PREPARED", "COMMITED", "FINISHED");
        assertStates(history.takeBatch(10, 0), "FINISHED");

        // Left to wait, the transitions of one task are one queued row with the last state.
        StreamLoadTask quick = new StreamLoadTask(3L, new Database(), new OlapTable(), "quick", "", "",
                100000L, CREATE_MS, false, WarehouseManager.DEFAULT_RESOURCE);
        new StreamLoadMgr().addLoadTask(quick, false);
        quick.afterPrepared(txnState);
        quick.afterCommitted(txnState);
        quick.afterVisible(txnState);
        assertStates(writer.takeBatch(10, 0), "FINISHED");
        assertStates(history.takeBatch(10, 0), "FINISHED");
        String keyPrefix = "('" + CREATE_DAY + "', 'STREAM_LOAD', 1, 'JOB', 'label', ";
        Assertions.assertTrue(rows.stream().allMatch(r -> r.values.startsWith(keyPrefix)), rows.toString());

        // A replayed task is not a creation on this node.
        StreamLoadTask replayed = new StreamLoadTask(2L, new Database(), new OlapTable(), "replayed", "", "",
                100000L, CREATE_MS, false, WarehouseManager.DEFAULT_RESOURCE);
        new StreamLoadMgr().replayCreateLoadTask(replayed);
        Assertions.assertEquals(0, writer.queueSize());
    }

    @Test
    public void testRoutineLoadJobTransitionsEnqueueJobRows(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked EditLog editLog) throws Exception {
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
        LoadsHistoryWriter writer = activateJobsWriter();

        RoutineLoadJob job = new KafkaRoutineLoadJob(111L, "job_name", 11L, 22L, "brokerList", "topic");
        Deencapsulation.setField(job, "createTimestamp", CREATE_MS);
        // The queue is taken after every transition: rows of one job coalesce while they wait.
        List<LoadsHistoryWriter.Row> rows = Lists.newArrayList();
        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        rows.addAll(writer.takeBatch(10, 0));
        job.updateState(RoutineLoadJob.JobState.PAUSED,
                new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, "too many errors"));
        rows.addAll(writer.takeBatch(10, 0));

        // The JOB row: the job's identity and state, no counters, no parent.
        TLoadInfo paused = job.toLoadJobRow();
        Assertions.assertEquals(111L, paused.getJob_id());
        Assertions.assertEquals("job_name", paused.getLabel());
        Assertions.assertEquals("PAUSED", paused.getState());
        Assertions.assertEquals("ROUTINE_LOAD", paused.getType());
        Assertions.assertEquals(CREATE_MS, paused.getCreate_time_ms());
        Assertions.assertEquals(22L, paused.getTable_id());
        Assertions.assertFalse(paused.isSetParent_id());
        Assertions.assertFalse(paused.isSetNum_scan_rows());
        Assertions.assertFalse(paused.isSetLoad_finish_time_ms());
        Assertions.assertTrue(paused.isSetLoad_start_time_ms());
        Assertions.assertTrue(paused.getError_msg().contains("too many errors"), paused.getError_msg());
        Assertions.assertEquals("ROUTINE_LOAD", LoadJobsTable.entryOf(paused));
        Assertions.assertEquals("JOB", LoadJobsTable.levelOf(paused));

        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        rows.addAll(writer.takeBatch(10, 0));
        job.updateState(RoutineLoadJob.JobState.STOPPED, null);
        rows.addAll(writer.takeBatch(10, 0));
        TLoadInfo stopped = job.toLoadJobRow();
        Assertions.assertFalse(stopped.isSetError_msg());
        Assertions.assertTrue(stopped.isSetLoad_finish_time_ms());
        // start_time follows the last RUNNING transition.
        Assertions.assertTrue(stopped.getLoad_start_time_ms() >= paused.getLoad_start_time_ms());

        assertStates(rows, "RUNNING", "PAUSED", "RUNNING", "STOPPED");
        String keyPrefix = "('" + CREATE_DAY + "', 'ROUTINE_LOAD', 111, 'JOB', 'job_name', ";
        Assertions.assertTrue(rows.stream().allMatch(r -> r.values.startsWith(keyPrefix)), rows.toString());
        Assertions.assertTrue(rows.get(1).values.contains("too many errors"), rows.get(1).values);
        Assertions.assertFalse(rows.get(2).values.contains("too many errors"), rows.get(2).values);

        // Replayed state changes write nothing.
        job.replayUpdateState(RoutineLoadJob.JobState.CANCELLED, null);
        Assertions.assertEquals(0, writer.queueSize());
    }
}
