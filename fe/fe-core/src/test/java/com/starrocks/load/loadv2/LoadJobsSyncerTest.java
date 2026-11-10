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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.loadv2.LoadJobsTable.PartitionRange;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TResultBatch;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class LoadJobsSyncerTest {

    private static final long CREATE_MS = 1_700_000_000_000L;

    private final List<String> events = Lists.newArrayList();
    private final AtomicLong now = new AtomicLong(1_800_000_000_000L);
    // What the fake table and the fake managers hold.
    private final List<List<String>> nonFinalRows = Lists.newArrayList();
    private final List<String> liveDays = Lists.newArrayList();
    private final List<PartitionRange> partitions = Lists.newArrayList();
    private final Map<Long, TLoadInfo> held = Maps.newHashMap();
    private final List<Set<Long>> lookedUp = Lists.newArrayList();
    // The ids the fake table has a row for, and every load the fake managers hold.
    private final Set<Long> existingIds = Sets.newHashSet();
    private final Map<Long, TLoadInfo> allHeld = Maps.newLinkedHashMap();

    private int savedInterval;
    private int savedBatchRows;
    private int savedRetainedDays;

    private final LoadJobsSyncer.TableAccess access = new LoadJobsSyncer.TableAccess() {
        @Override
        public List<List<String>> query(String sql) {
            events.add("QUERY " + sql);
            if (sql.startsWith("SELECT DISTINCT")) {
                return liveDays.stream().map(day -> Lists.newArrayList(day)).collect(Collectors.toList());
            }
            if (sql.startsWith("SELECT `id` FROM")) {
                return existingIds.stream().map(id -> Lists.newArrayList(String.valueOf(id)))
                        .collect(Collectors.toList());
            }
            return nonFinalRows;
        }

        @Override
        public void execute(String sql) {
            events.add("DML " + sql);
        }

        @Override
        public void executeDdl(String sql) {
            events.add("DDL " + sql);
        }

        @Override
        public List<PartitionRange> partitions() {
            return partitions;
        }
    };

    @BeforeEach
    public void setUp() {
        savedInterval = Config.loads_history_sync_interval_second;
        savedBatchRows = Config.loads_history_flush_batch_rows;
        savedRetainedDays = Config.loads_history_retained_days;
        Config.loads_history_sync_interval_second = 60;
    }

    @AfterEach
    public void tearDown() {
        LoadJobsTable.activate(null);
        Config.loads_history_sync_interval_second = savedInterval;
        Config.loads_history_flush_batch_rows = savedBatchRows;
        Config.loads_history_retained_days = savedRetainedDays;
    }

    private static TLoadInfo row(long id, String type, String state) {
        TLoadInfo info = new TLoadInfo();
        info.setJob_id(id);
        info.setLabel("label_" + id);
        info.setDb("d");
        info.setTable("t");
        info.setType(type);
        info.setState(state);
        info.setCreate_time_ms(CREATE_MS);
        return info;
    }

    private static List<String> tableRow(long id, String state, boolean pastTimeout) {
        return Lists.newArrayList(String.valueOf(id), state, pastTimeout ? "1" : "0");
    }

    private Map<Long, TLoadInfo> lookup(Collection<Long> ids) {
        lookedUp.add(Sets.newHashSet(ids));
        return held.entrySet().stream().filter(e -> ids.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private LoadJobsSyncer newSyncer(LoadsHistoryWriter writer) {
        writer.setClock(now::get, millis -> { });
        LoadJobsSyncer syncer = new LoadJobsSyncer(writer, this::lookup, () -> allHeld, access, () -> true);
        syncer.setClock(now::get);
        return syncer;
    }

    private List<String> events(String kind) {
        return events.stream().filter(e -> e.startsWith(kind + " ")).collect(Collectors.toList());
    }

    @Test
    public void testReconcileRepairsFromTheManagersBeforeMarkingUnknown() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100,
                (sql, rows) -> events.add("FLUSH " + rows));
        LoadJobsSyncer syncer = newSyncer(writer);

        // 1: finished on this FE, but the table never got the final row (lost with the queue of
        //    the previous leader, dropped from a full queue, ...).
        nonFinalRows.add(tableRow(1, "LOADING", true));
        held.put(1L, row(1, "BROKER", "FINISHED"));
        // 2: no manager knows it any more and it is past its timeout.
        nonFinalRows.add(tableRow(2, "LOADING", true));
        // 3: unknown to the managers, but it may still be running elsewhere.
        nonFinalRows.add(tableRow(3, "LOADING", false));
        // 4: still running here and the table agrees.
        nonFinalRows.add(tableRow(4, "LOADING", true));
        held.put(4L, row(4, "BROKER", "LOADING"));
        // 5: a routine load job (no timeout) that was stopped; the STOPPED row was lost.
        nonFinalRows.add(tableRow(5, "RUNNING", true));
        held.put(5L, row(5, "ROUTINE_LOAD", "STOPPED"));
        // 6: a routine load job that the manager has already removed.
        nonFinalRows.add(tableRow(6, "PAUSED", true));

        syncer.runCycle(10);

        Assertions.assertEquals(1, lookedUp.size());
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L, 5L, 6L), lookedUp.get(0));
        // Only the loads nobody tracks are marked; the time is that of the pass, as a literal.
        String nowText = LoadsHistoryWriter.formatDatetime(now.get());
        Assertions.assertEquals(Lists.newArrayList("DML UPDATE _statistics_.load_jobs SET `state` = 'UNKNOWN', "
                + "`load_finish_time` = '" + nowText + "' WHERE `state` NOT IN ('CANCELLED', 'FINISHED', 'STOPPED', "
                + "'UNKNOWN') AND `id` IN (2, 6)"), events("DML"));
        // The loads a manager holds in another state than the table are written again as they are now.
        List<LoadsHistoryWriter.Row> queued = writer.takeBatch(10, 0);
        Assertions.assertEquals(2, queued.size(), queued.toString());
        Assertions.assertTrue(queued.get(0).values.contains("'BROKER', 1, 'JOB'")
                && queued.get(0).values.contains("'FINISHED'"), queued.get(0).values);
        Assertions.assertTrue(queued.get(1).values.contains("'ROUTINE_LOAD', 5, 'JOB'")
                && queued.get(1).values.contains("'STOPPED'"), queued.get(1).values);
    }

    @Test
    public void testReconcileWaitsWhileRowsAreStillOnTheirWay() throws Exception {
        AtomicInteger failures = new AtomicInteger(1);
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> {
            if (failures.getAndDecrement() > 0) {
                throw new RuntimeException("backend down");
            }
            events.add("FLUSH " + rows);
        });
        LoadJobsSyncer syncer = newSyncer(writer);

        // The table still shows load 1 as LOADING; its final row is queued, and no manager holds
        // the load any more. Judged now, the row would be marked UNKNOWN.
        nonFinalRows.add(tableRow(1, "LOADING", true));
        writer.enqueue(row(1, "BROKER", "FINISHED"));
        writer.enqueue(row(2, "BROKER", "LOADING"));

        // The flush failed: the queue holds what the table lacks, so nothing is reconciled.
        syncer.runCycle(10);
        Assertions.assertTrue(events.isEmpty(), events.toString());
        Assertions.assertEquals(2, writer.queueSize());

        // A flush that left rows behind does not count either.
        Config.loads_history_flush_batch_rows = 1;
        syncer.runCycle(10);
        Assertions.assertEquals(Lists.newArrayList("FLUSH 1"), events);
        Assertions.assertEquals(1, writer.queueSize());

        // Once the queue is drained the pass runs.
        Config.loads_history_flush_batch_rows = 500;
        nonFinalRows.clear();
        syncer.runCycle(10);
        Assertions.assertEquals(3, events.size(), events.toString());
        Assertions.assertEquals("FLUSH 1", events.get(1));
        Assertions.assertTrue(events.get(2).startsWith("QUERY SELECT `id`, `state`, "), events.get(2));
        Assertions.assertTrue(events("DML").isEmpty(), events.toString());
    }

    @Test
    public void testReconcileRunsAnywayWhenTheQueueNeverDrains() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100,
                (sql, rows) -> events.add("FLUSH " + rows));
        LoadJobsSyncer syncer = newSyncer(writer);
        Config.loads_history_flush_batch_rows = 1;
        for (long id = 1; id <= 4; id++) {
            writer.enqueue(row(id, "BROKER", "LOADING"));
        }

        syncer.runCycle(10);
        now.addAndGet(60_000L);
        syncer.runCycle(10);
        Assertions.assertEquals(Lists.newArrayList("FLUSH 1", "FLUSH 1"), events);

        // Ten intervals after the session started the pass no longer waits for an empty queue.
        now.addAndGet(LoadJobsSyncer.OVERDUE_INTERVALS * 60_000L);
        syncer.runCycle(10);
        Assertions.assertEquals(1, events("QUERY").size(), events.toString());
        Assertions.assertEquals(1, writer.queueSize());
    }

    @Test
    public void testOrdinaryPassesReadRecentPartitionsAndAFullPassRunsEveryHour() throws Exception {
        LoadJobsSyncer syncer = newSyncer(new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> { }));
        nonFinalRows.add(tableRow(2, "LOADING", true));

        // The first pass of a leader session reads every partition.
        syncer.runCycle(10);
        Assertions.assertEquals(1, events("QUERY").size());
        Assertions.assertFalse(events("QUERY").get(0).contains("`create_day` >="), events.toString());
        Assertions.assertTrue(events("DML").get(0).endsWith("'UNKNOWN') AND `id` IN (2)"), events.toString());

        // Within the interval the cycle only flushes.
        now.addAndGet(30_000L);
        syncer.runCycle(10);
        Assertions.assertEquals(1, events("QUERY").size());

        // The next pass reads, and updates, only the partitions a running load can be in:
        // those of the longest default timeout plus a day.
        now.addAndGet(30_000L);
        syncer.runCycle(10);
        long spanMs = Math.max(Config.max_load_timeout_second, Math.max(Config.stream_load_default_timeout_second,
                Config.routine_load_task_timeout_second)) * 1000L + 86_400_000L;
        String bound = " AND `create_day` >= '" + LoadJobsTable.keyDay(now.get() - spanMs) + "'";
        Assertions.assertEquals(2, events("QUERY").size());
        Assertions.assertTrue(events("QUERY").get(1).endsWith(bound + " LIMIT 100000"), events.toString());
        Assertions.assertTrue(events("DML").get(1).endsWith(bound + " AND `id` IN (2)"), events.toString());

        // An hour after the last full pass every partition is read again.
        now.addAndGet(LoadJobsSyncer.FULL_PASS_INTERVAL_MS);
        syncer.runCycle(10);
        Assertions.assertEquals(3, events("QUERY").size());
        Assertions.assertFalse(events("QUERY").get(2).contains("`create_day` >="), events.toString());
    }

    @Test
    public void testFailedReconcileIsRetriedNextCycle() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> { });
        writer.setClock(now::get, millis -> { });
        LoadJobsSyncer syncer = new LoadJobsSyncer(writer, this::lookup, new LoadJobsSyncer.TableAccess() {
            @Override
            public List<List<String>> query(String sql) {
                if (attempts.incrementAndGet() == 1) {
                    throw new RuntimeException("backend down");
                }
                return Lists.newArrayList();
            }

            @Override
            public void execute(String sql) {
            }

            @Override
            public void executeDdl(String sql) {
            }

            @Override
            public List<PartitionRange> partitions() {
                return Lists.newArrayList();
            }
        }, () -> true);
        syncer.setClock(now::get);

        syncer.runCycle(10);
        now.addAndGet(60_000L);
        syncer.runCycle(10);
        Assertions.assertEquals(2, attempts.get());
    }

    @Test
    public void testFullPassExpiresFinishedLoadsButKeepsLiveOnes() throws Exception {
        LoadJobsSyncer syncer = newSyncer(new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> { }));
        Config.loads_history_retained_days = 30;
        String finishCutoff = LoadsHistoryWriter.formatDatetime(now.get() - 30 * 86_400_000L);
        String cutoff = LoadJobsTable.utcDatetime(now.get() - 30 * 86_400_000L);
        String cutoffDay = cutoff.substring(0, 10);
        // Two partitions well before the cutoff and the one the cutoff falls into.
        partitions.add(new PartitionRange("p_job", "2020-01-01 00:00:00", "2020-01-02 00:00:00"));
        partitions.add(new PartitionRange("p_old", "2020-01-02 00:00:00", "2020-01-03 00:00:00"));
        partitions.add(new PartitionRange("p_cutoff", cutoffDay + " 00:00:00", "9999-12-31 00:00:00"));
        // A routine load job created on 2020-01-01 is still running, or ended within the retention.
        liveDays.add("2020-01-01 00:00:00");

        syncer.runCycle(10);

        Assertions.assertEquals("QUERY " + LoadJobsTable.buildLiveDaysSql(cutoff, finishCutoff), events("QUERY").get(1));
        // Only the rows whose load finished longer ago than the retention go, whenever it was created.
        Assertions.assertEquals(Lists.newArrayList("DML DELETE FROM _statistics_.load_jobs WHERE `create_day` >= "
                + "'2020-01-01 00:00:00' AND `create_day` < '2020-01-02 00:00:00' AND NOT (`state` NOT IN ('CANCELLED', "
                + "'FINISHED', 'STOPPED', 'UNKNOWN') OR COALESCE(`load_finish_time`, `create_time`) >= '"
                + finishCutoff + "')"), events("DML"));
        Assertions.assertEquals(Lists.newArrayList(
                "DDL ALTER TABLE _statistics_.load_jobs DROP PARTITION IF EXISTS `p_old` FORCE"), events("DDL"));

        // Not again before the next full pass.
        events.clear();
        now.addAndGet(60_000L);
        syncer.runCycle(10);
        Assertions.assertEquals(1, events.size(), events.toString());

        // With nothing past the retention the table is not even asked for its live days.
        events.clear();
        partitions.remove(0);
        partitions.remove(0);
        now.addAndGet(LoadJobsSyncer.FULL_PASS_INTERVAL_MS);
        syncer.runCycle(10);
        Assertions.assertEquals(1, events.size(), events.toString());
    }

    @Test
    public void testLookupFindsSubTasksOfAMultiStatementStreamLoad() throws Exception {
        StreamLoadTask plain = new StreamLoadTask(5L, new Database(), new OlapTable(), "plain", "", "",
                100000L, CREATE_MS, false, WarehouseManager.DEFAULT_RESOURCE);
        StreamLoadMultiStmtTask parent = new StreamLoadMultiStmtTask(20L, new Database(), "multi", "", "",
                100000L, CREATE_MS, WarehouseManager.DEFAULT_RESOURCE);
        StreamLoadTask sub = new StreamLoadTask(21L, new Database(), new OlapTable(), "multi", "", "",
                100000L, CREATE_MS, false, WarehouseManager.DEFAULT_RESOURCE);
        Map<String, StreamLoadTask> subTasks = Deencapsulation.getField(parent, "taskMaps");
        subTasks.put("t1", sub);
        // Committed by the parent, not visible yet: not a final state of the row.
        sub.markCommittedByParent(false, CREATE_MS + 1000);
        Assertions.assertEquals(Lists.newArrayList(21L), parent.getSubTaskIds());

        List<AbstractStreamLoadTask> tasks = Lists.newArrayList(plain, parent);
        new MockUp<StreamLoadMgr>() {
            @Mock
            public List<AbstractStreamLoadTask> getAllTasks() {
                return tasks;
            }
        };

        // The row of a sub-task carries the sub-task's id, not the parent's.
        Map<Long, TLoadInfo> found = LoadJobsSyncer.lookupLoads(Sets.newHashSet(5L, 20L, 21L, 99L));
        Assertions.assertEquals(Sets.newHashSet(5L, 21L), found.keySet());
        Assertions.assertEquals("COMMITED", found.get(21L).getState());

        // So a sub-task past the timeout of its row is not marked UNKNOWN while its parent holds it.
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> { });
        writer.setClock(now::get, millis -> { });
        LoadJobsSyncer syncer = new LoadJobsSyncer(writer, LoadJobsSyncer::lookupLoads, access, () -> true);
        syncer.setClock(now::get);
        nonFinalRows.add(tableRow(21, "COMMITED", true));
        nonFinalRows.add(tableRow(99, "LOADING", true));
        syncer.runCycle(10);
        Assertions.assertEquals(1, events("DML").size(), events.toString());
        Assertions.assertTrue(events("DML").get(0).endsWith(" AND `id` IN (99)"), events.toString());
        Assertions.assertEquals(0, writer.queueSize());
    }

    @Test
    public void testLoadsWithoutARowGetOne() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> { });
        LoadJobsSyncer syncer = newSyncer(writer);
        Config.loads_history_retained_days = 30;
        // 1: has a row. 2: a short load whose only row was lost with the previous leader's queue.
        // 3: a routine load job that existed before the table did. 4: finished longer ago than
        // the retention, its row has expired. 5: a row that could not be read.
        existingIds.add(1L);
        allHeld.put(1L, row(1, "BROKER", "LOADING"));
        TLoadInfo finished = row(2, "STREAM_LOAD", "FINISHED");
        finished.setLoad_finish_time_ms(now.get() - 5_000L);
        allHeld.put(2L, finished);
        allHeld.put(3L, row(3, "ROUTINE_LOAD", "PAUSED"));
        TLoadInfo old = row(4, "BROKER", "CANCELLED");
        old.setLoad_finish_time_ms(now.get() - 31 * 86_400_000L);
        allHeld.put(4L, old);
        allHeld.put(5L, LoadJobsSyncer.UNREADABLE);

        // The first pass of a leader session looks for them.
        syncer.runCycle(10);
        Assertions.assertEquals("QUERY SELECT `id` FROM _statistics_.load_jobs WHERE `id` IN (1, 2, 3)",
                events("QUERY").get(1));
        List<LoadsHistoryWriter.Row> queued = writer.takeBatch(10, 0);
        Assertions.assertEquals(2, queued.size(), queued.toString());
        Assertions.assertTrue(queued.get(0).values.contains("'STREAM_LOAD', 2, 'JOB'")
                && queued.get(0).values.contains("'FINISHED'"), queued.get(0).values);
        Assertions.assertTrue(queued.get(1).values.contains("'ROUTINE_LOAD', 3, 'JOB'"), queued.get(1).values);

        // An ordinary pass does not, unless the writer dropped rows since.
        events.clear();
        existingIds.addAll(Sets.newHashSet(2L, 3L));
        now.addAndGet(60_000L);
        syncer.runCycle(10);
        Assertions.assertEquals(1, events("QUERY").size(), events.toString());

        LoadsHistoryWriter small = new LoadsHistoryWriter(LoadJobsTable.TARGET, 1, (sql, rows) -> { });
        LoadJobsSyncer afterDrop = newSyncer(small);
        afterDrop.runCycle(10);
        events.clear();
        // 6 is created and finished, and its row is pushed out of the full queue by 7.
        TLoadInfo dropped = row(6, "STREAM_LOAD", "FINISHED");
        dropped.setLoad_finish_time_ms(now.get());
        allHeld.put(6L, dropped);
        small.enqueue(dropped);
        small.enqueue(row(7, "BROKER", "LOADING"));
        Assertions.assertEquals(1, small.droppedTotal());
        existingIds.add(7L);
        now.addAndGet(60_000L);
        afterDrop.runCycle(10);
        Assertions.assertEquals(2, events("QUERY").size(), events.toString());
        queued = small.takeBatch(10, 0);
        Assertions.assertTrue(queued.stream().anyMatch(r -> r.values.contains("'STREAM_LOAD', 6, 'JOB'")), queued.toString());
    }

    @Test
    public void testALoadWhoseRowCannotBeReadIsNotMarkedUnknown() throws Exception {
        LoadsHistoryWriter writer = new LoadsHistoryWriter(LoadJobsTable.TARGET, 100, (sql, rows) -> { });
        LoadJobsSyncer syncer = newSyncer(writer);
        // Both are past their timeout; a manager holds 1, but reading its row failed.
        nonFinalRows.add(tableRow(1, "RUNNING", true));
        held.put(1L, LoadJobsSyncer.UNREADABLE);
        nonFinalRows.add(tableRow(2, "LOADING", true));

        syncer.runCycle(10);

        Assertions.assertEquals(1, events("DML").size(), events.toString());
        Assertions.assertTrue(events("DML").get(0).endsWith("AND `id` IN (2)"), events.toString());
        Assertions.assertEquals(0, writer.queueSize());
    }

    @Test
    public void testDecodeResultRows() {
        TResultBatch batch = new TResultBatch();
        batch.addToRows(ByteBuffer.wrap("{\"data\":[7,\"LOADING\",1]}".getBytes(StandardCharsets.UTF_8)));
        batch.addToRows(ByteBuffer.wrap("{\"data\":[8,null,0]}".getBytes(StandardCharsets.UTF_8)));
        List<List<String>> rows = LoadJobsSyncer.decode(Lists.newArrayList(batch));
        Assertions.assertEquals(Lists.newArrayList("7", "LOADING", "1"), rows.get(0));
        Assertions.assertEquals(Lists.newArrayList("8", null, "0"), rows.get(1));
        Assertions.assertTrue(LoadJobsSyncer.decode(null).isEmpty());
        List<LoadJobsTable.NonFinalRow> parsed = LoadJobsTable.toNonFinalRows(rows);
        Assertions.assertEquals(1, parsed.size());
        Assertions.assertEquals(7L, parsed.get(0).id);
    }

    @Test
    public void testWriterIsInactiveUntilTheDaemonRuns() {
        LoadJobsSyncer syncer = new LoadJobsSyncer();
        Assertions.assertEquals(0L, syncer.getInterval());
        Assertions.assertSame(LoadJobsTable.TARGET, syncer.getWriter().getTarget());
        Assertions.assertEquals(0L, LoadJobsTable.queuedRows());
        LoadJobsTable.activate(syncer.getWriter());
        try {
            Assertions.assertEquals(0L, LoadJobsTable.queuedRows());
        } finally {
            LoadJobsTable.deactivate(syncer.getWriter());
        }
    }
}
