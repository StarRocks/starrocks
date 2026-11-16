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

package com.starrocks.alter.reshard;

import com.starrocks.alter.AlterJobException;
import com.starrocks.alter.AlterJobExecutor;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.lock.LockTestUtils;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ALTER TABLE while a range table is held in TABLET_RESHARD by a split or merge job. The jobs are left
 * PENDING (or set to RUNNING by hand) with every leader daemon stopped, so the reshard daemon never
 * advances them and each test sees the table in TABLET_RESHARD for its whole duration.
 */
public class AlterTableDuringTabletReshardTest {
    private static final String DB = "reshard_alter";
    private static StarRocksAssert starRocksAssert;
    private Database db;
    private OlapTable table;
    private OlapTable other;
    private TabletReshardJobMgr mgr;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        List<LeaderDaemon> daemons = LeaderDaemon.getRunningInstances();
        daemons.forEach(LeaderDaemon::setStop);
        LeaderDaemon.awaitQuiesced(daemons, 30_000L);
        Config.enable_range_distribution = true;
        starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        mgr.getTabletReshardJobs().clear();
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        String partitions = " partition by range(p) (partition p1 values [('0'), ('10')), "
                + "partition p2 values [('10'), ('20')), partition p3 values [('20'), ('30'))) "
                + "order by(k) properties('replication_num'='1')";
        starRocksAssert.withTable("create table t (p int, k int, v int)" + partitions);
        starRocksAssert.withTable("create table t_other (p int, k int, v int)" + partitions);
        starRocksAssert.alterTable("alter table t add temporary partition tmp_p1 values [('0'), ('10'))");
        starRocksAssert.alterTable("alter table t add temporary partition tmp_p2 values [('10'), ('20'))");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        table = (OlapTable) db.getTable("t");
        other = (OlapTable) db.getTable("t_other");
    }

    @AfterEach
    public void afterEach() {
        mgr.getTabletReshardJobs().clear();
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    private long physicalPartitionId(String partitionName, boolean temp) {
        return table.getPartition(partitionName, temp).getDefaultPhysicalPartition().getId();
    }

    private TabletReshardJob job(boolean merge, long physicalPartitionId) {
        MaterializedIndex source = table.getPhysicalPartition(physicalPartitionId).getLatestBaseIndex();
        MaterializedIndex output = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                source.getMetaId(), MaterializedIndex.IndexState.NORMAL, source.getShardGroupId());
        Map<Long, ReshardingPhysicalPartition> partitions = new HashMap<>();
        partitions.put(physicalPartitionId, new ReshardingPhysicalPartition(physicalPartitionId,
                Map.of(source.getId(), new ReshardingMaterializedIndex(source.getId(), output, List.of()))));
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return merge ? new MergeTabletJob(jobId, db.getId(), table.getId(), partitions)
                : new SplitTabletJob(jobId, db.getId(), table.getId(), partitions);
    }

    /** Admits a PENDING split job on the given physical partition, which puts the table in TABLET_RESHARD. */
    private TabletReshardJob holdInReshard(long physicalPartitionId) throws Exception {
        TabletReshardJob job = job(false, physicalPartitionId);
        mgr.addTabletReshardJob(job);
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        return job;
    }

    private static void alter(String sql) throws Exception {
        starRocksAssert.alterTable(sql);
    }

    private void assertStillResharding(TabletReshardJob job) {
        assertSame(job, mgr.getTabletReshardJob(job.getJobId()));
        assertFalse(job.isDone());
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
    }

    @Test
    public void addPartitionIsAllowed() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        alter("alter table t add partition p4 values [('30'), ('40'))");
        assertNotNull(table.getPartition("p4"));
        alter("alter table t add temporary partition tmp_p3 values [('20'), ('30'))");
        assertNotNull(table.getPartition("tmp_p3", true));
        assertStillResharding(job);
    }

    @Test
    public void dropPartitionIsAllowed() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        alter("alter table t drop partition p3");
        assertNull(table.getPartition("p3"));
        alter("alter table t drop temporary partition tmp_p2");
        assertNull(table.getPartition("tmp_p2", true));
        assertStillResharding(job);
    }

    // A recoverable drop of the partition the job works on is refused, as it is for a partition with COMMITTED
    // transactions: the job's reserved version is not a registered transaction, and RECOVER would bring the
    // partition back below it. DROP ... FORCE cannot be recovered, so it stays allowed.
    @Test
    public void reshardedPartitionCanOnlyBeDroppedWithForce() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        Exception e = assertThrows(Exception.class, () -> alter("alter table t drop partition p1"));
        assertTrue(e.getMessage().contains("split or merge is in progress on the partition [p1]"), e.getMessage());
        assertTrue(e.getMessage().contains("FORCE"), e.getMessage());
        assertNotNull(table.getPartition("p1"));
        alter("alter table t drop partition p1 force");
        assertNull(table.getPartition("p1"));
        assertStillResharding(job);
    }

    @Test
    public void replacePartitionIsAllowedIncludingTheReshardedOne() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        long tmpP2 = table.getPartition("tmp_p2", true).getId();
        alter("alter table t replace partition (p2) with temporary partition (tmp_p2)");
        assertEquals(tmpP2, table.getPartition("p2").getId());
        long tmpP1 = table.getPartition("tmp_p1", true).getId();
        alter("alter table t replace partition (p1) with temporary partition (tmp_p1)");
        assertEquals(tmpP1, table.getPartition("p1").getId());
        assertStillResharding(job);
    }

    @Test
    public void replaceKeepsATemporaryReshardTargetResolvable() throws Exception {
        long target = physicalPartitionId("tmp_p2", true);
        TabletReshardJob job = holdInReshard(target);
        alter("alter table t replace partition (p2) with temporary partition (tmp_p2)");
        assertEquals(table.getPartition("p2").getId(), table.getPhysicalPartition(target).getParentId());
        assertStillResharding(job);
    }

    @Test
    public void truncatePartitionIsAllowed() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        long before = table.getPartition("p3").getId();
        alter("alter table t truncate partition p3");
        assertNotEquals(before, table.getPartition("p3").getId());
        assertStillResharding(job);
    }

    @Test
    public void renamesAreAllowed() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        alter("alter table t rename partition p3 p3_new");
        assertNotNull(table.getPartition("p3_new"));
        alter("alter table t rename column v to v2");
        assertNotNull(table.getColumn("v2"));
        // k is the sort key, so it is also the column the range distribution routes on.
        alter("alter table t rename column k to k2");
        assertNotNull(table.getColumn("k2"));
        alter("alter table t rename t_renamed");
        assertSame(table, db.getTable("t_renamed"));
        assertStillResharding(job);
    }

    @Test
    public void modifyPartitionAndCommentAreAllowed() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        alter("alter table t modify partition (p3) set ('datacache.enable' = 'false')");
        assertFalse(table.getPartitionInfo().getDataCacheInfo(table.getPartition("p3").getId()).isEnabled());
        alter("alter table t comment = 'changed during reshard'");
        assertEquals("changed during reshard", table.getComment());
        assertStillResharding(job);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void swapIsAllowedInBothDirections(boolean reshardingTableIsSubject) throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        alter(reshardingTableIsSubject ? "alter table t swap with t_other" : "alter table t_other swap with t");
        assertSame(other, db.getTable("t"));
        assertSame(table, db.getTable("t_other"));
        assertStillResharding(job);
    }

    @Test
    public void otherAlterOperationsStayBlockedWithAMessageThatNamesTheJobsView() throws Exception {
        TabletReshardJob job = holdInReshard(physicalPartitionId("p1", false));
        AlterTableStmt addColumn = parse("alter table t add column c int");
        assertRejectedForReshard(addColumn);
        assertNull(table.getColumn("c"));
        assertRejectedForReshard(parse("alter table t set ('enable_load_profile' = 'true')"));

        AlterTableStmt addPartition = parse("alter table t add partition p4 values [('30'), ('40'))");
        AlterTableStmt mixed = new AlterTableStmt(addPartition.getTableRef(),
                List.of(addPartition.getAlterClauseList().get(0), addColumn.getAlterClauseList().get(0)));
        assertRejectedForReshard(mixed);
        assertNull(table.getPartition("p4"));
        assertStillResharding(job);
    }

    private static AlterTableStmt parse(String sql) throws Exception {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setDatabase(DB);
        return (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, context);
    }

    // Goes through AlterJobExecutor directly so a statement built from separately analyzed clauses can be
    // executed; the analyzer would refuse to combine them in one SQL text.
    private static void assertRejectedForReshard(AlterTableStmt stmt) {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setDatabase(DB);
        AlterJobException e = assertThrows(AlterJobException.class, () -> new AlterJobExecutor().process(stmt, context));
        InvalidOlapTableStateException cause = assertInstanceOf(InvalidOlapTableStateException.class, e.getCause());
        assertTrue(cause.getMessage().contains("information_schema.tablet_reshard_jobs"), cause.getMessage());
    }

    @Test
    public void reshardStateMessageNamesTheTableAndTheJobsView() {
        String message = InvalidOlapTableStateException.of(OlapTable.OlapTableState.TABLET_RESHARD, "tbl").getMessage();
        assertTrue(message.contains("on the table tbl."), message);
        assertTrue(message.contains("split or merge"), message);
        assertTrue(message.contains("information_schema.tablet_reshard_jobs"), message);
    }

    /**
     * RENAME TABLE and SWAP take the table out of {@code Database.idToTable} and put it back while they hold
     * the DB WRITE lock. A RUNNING job that looked the table up without the lock would find nothing in that
     * gap and abort itself; it has to resolve the table only after the lock is taken.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void runningJobResolvesTheTableAfterTakingTheLock(boolean merge) throws Exception {
        TabletReshardJob job = job(merge, physicalPartitionId("p1", false));
        mgr.addTabletReshardJob(job);
        job.jobState = TabletReshardJob.JobState.RUNNING;

        AtomicReference<OlapTable> resolved = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread jobThread = new Thread(() -> {
            try (LockedObject<OlapTable> locked = job instanceof SplitTabletJob split
                    ? split.getLockedTable(LockType.READ) : ((MergeTabletJob) job).getLockedTable(LockType.READ)) {
                resolved.set(locked.get());
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            db.unRegisterTableUnlocked(table);
            jobThread.start();
            awaitParkedOrTerminated(jobThread);
            db.registerTableUnlocked(table);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
        jobThread.join(30_000L);

        assertNull(failure.get());
        assertSame(table, resolved.get());
        assertEquals(TabletReshardJob.JobState.RUNNING, job.getJobState());
    }

    @Test
    public void lockedObjectReleasesItsLocksWhenTheLookupFails() {
        assertThrows(IllegalStateException.class, () -> new LockedObject<OlapTable>(db.getId(),
                List.of(table.getId()), LockType.READ, () -> {
                    throw new IllegalStateException("table is gone");
                }));
        LockTestUtils.assertNoMetaLockHeld();
    }

    private static void awaitParkedOrTerminated(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.TERMINATED || state == Thread.State.WAITING
                    || state == Thread.State.TIMED_WAITING || state == Thread.State.BLOCKED) {
                return;
            }
            Thread.sleep(5);
        }
    }
}
