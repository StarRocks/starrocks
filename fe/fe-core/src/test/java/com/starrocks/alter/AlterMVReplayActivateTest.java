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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Replaying ALTER MATERIALIZED VIEW ... ACTIVE used to re-analyze the whole define query while holding the
 * MV write lock, resolving every base table through the connector. On a follower that is a connector round
 * trip under the lock per replayed entry; on the checkpoint thread it made the image depend on an external
 * catalog being reachable, and in lock_blocking_call_validation_mode=error the gate throws inside the
 * re-analysis, so the image recorded the MV inactive while the leader had it active.
 *
 * <p>The leader now journals the base tables it activated with, and replay adopts them. The part of the
 * rebuild that still needs the connector runs asynchronously, outside the lock.
 */
public class AlterMVReplayActivateTest extends MVTestBase {

    private static final String BASE_TABLE = "t_replay_activate_base";
    private static final String HIVE_MV_DEFINITION = "PARTITION BY (`l_shipdate`)\n"
            + "DISTRIBUTED BY RANDOM\n"
            + "REFRESH DEFERRED MANUAL\n"
            + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;";

    private LockProbeHiveMetadata probe;
    private ConnectorMetadata originalHiveMetadata;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert.withTable("CREATE TABLE " + BASE_TABLE + " (\n"
                + "  k1 date,\n"
                + "  k2 int,\n"
                + "  v1 int\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES ('replication_num' = '1');");
    }

    /**
     * Samples {@link LockHoldDepth#isUnderLock()} on getTable, OR-accumulated, for calls made on the thread
     * that installed it only: creating an MV hands its definition to the mv-plan-cache executor, which
     * resolves the same hive table at an arbitrary moment on a thread whose lock state is unrelated.
     * The asynchronous rebuild of a replayed activation can instead be parked on {@link #gate}.
     */
    private static class LockProbeHiveMetadata extends MockedHiveMetadata {
        private final Thread owner = Thread.currentThread();
        private final AtomicBoolean underLock = new AtomicBoolean(false);
        private final AtomicInteger calls = new AtomicInteger();
        // The same, for the calls made by the asynchronous rebuild of a replayed activation.
        private final AtomicBoolean rebuildUnderLock = new AtomicBoolean(false);
        private final AtomicInteger rebuildCalls = new AtomicInteger();
        private volatile CountDownLatch gate;
        private final CountDownLatch gateEntered = new CountDownLatch(1);

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            if (Thread.currentThread() == owner) {
                calls.incrementAndGet();
                if (LockHoldDepth.isUnderLock()) {
                    underLock.set(true);
                }
            } else if (isReplayRebuild()) {
                rebuildCalls.incrementAndGet();
                if (LockHoldDepth.isUnderLock()) {
                    rebuildUnderLock.set(true);
                }
                CountDownLatch current = gate;
                if (current != null) {
                    gateEntered.countDown();
                    try {
                        current.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            return super.getTable(context, dbName, tblName);
        }
    }

    // The mv-plan-cache executor also runs unrelated plan builds; park only the replayed activation's rebuild.
    private static boolean isReplayRebuild() {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (frame.getMethodName().contains("activateOnReplay")) {
                return true;
            }
        }
        return false;
    }

    private void installProbe() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        if (originalHiveMetadata == null) {
            originalHiveMetadata = metadataMgr.getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME)
                    .orElseThrow(() -> new IllegalStateException("hive0 catalog is not registered"));
        }
        probe = new LockProbeHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, probe);
    }

    @AfterEach
    public void removeProbe() {
        if (probe != null && probe.gate != null) {
            probe.gate.countDown();
        }
        if (originalHiveMetadata != null) {
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, originalHiveMetadata);
            originalHiveMetadata = null;
        }
    }

    private static AlterMaterializedViewStatusLog activeLog(MaterializedView mv, List<BaseTableInfo> infos) {
        AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(
                mv.getDbId(), mv.getId(), AlterMaterializedViewStatusClause.ACTIVE, "");
        log.setBaseTableInfos(infos);
        return log;
    }

    private static void awaitActive(MaterializedView mv) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(mv::isActive);
    }

    @Test
    public void testActivateJournalsTheBaseTableInfos() throws Exception {
        String mvName = "mv_replay_activate_journal";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " INACTIVE");

            EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
            EditLog spyEditLog = spy(originalEditLog);
            GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
            try {
                starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " ACTIVE");
                ArgumentCaptor<AlterMaterializedViewStatusLog> captor =
                        ArgumentCaptor.forClass(AlterMaterializedViewStatusLog.class);
                verify(spyEditLog).logAlterMvStatus(captor.capture(), any());
                Assertions.assertEquals(mv.getBaseTableInfos(), captor.getValue().getBaseTableInfos());
            } finally {
                GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
            }
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testBaseTableInfosSurviveSerializationAndOldEntriesCarryNone() {
        AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(
                1L, 2L, AlterMaterializedViewStatusClause.ACTIVE, "");
        List<BaseTableInfo> infos = Lists.newArrayList(new BaseTableInfo(1L, "db", "t", 3L),
                new BaseTableInfo("hive0", "partitioned_db", "lineitem_par", "lineitem_par:uuid"));
        log.setBaseTableInfos(infos);
        AlterMaterializedViewStatusLog read = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(log), AlterMaterializedViewStatusLog.class);
        Assertions.assertEquals(infos, read.getBaseTableInfos());

        // An entry written before the field existed.
        AlterMaterializedViewStatusLog old = GsonUtils.GSON.fromJson(
                "{\"dbId\":1,\"tableId\":2,\"status\":\"ACTIVE\",\"reason\":\"\"}",
                AlterMaterializedViewStatusLog.class);
        Assertions.assertNull(old.getBaseTableInfos());
    }

    @Test
    public void testReplayDoesNotResolveAnExternalBaseTableUnderTheLock() throws Exception {
        String mvName = "mv_replay_activate_hive";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" + HIVE_MV_DEFINITION);
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            List<BaseTableInfo> infos = Lists.newArrayList(mv.getBaseTableInfos());
            mv.setInactiveAndReason("test: forced inactive before the replay");

            installProbe();
            GlobalStateMgr.getCurrentState().getAlterJobMgr().replayAlterMaterializedViewStatus(activeLog(mv, infos));

            Assertions.assertFalse(probe.underLock.get(),
                    "replay resolved an external base table while holding the MV lock");
            awaitActive(mv);
            Assertions.assertEquals(infos, mv.getBaseTableInfos());
            // The resolution moved out of the lock rather than disappearing: the rebuild did reach the connector.
            Assertions.assertTrue(probe.rebuildCalls.get() > 0,
                    "the rebuild never resolved the external base table, so this test proves nothing");
            Assertions.assertFalse(probe.rebuildUnderLock.get(),
                    "the rebuild resolved an external base table while holding an FE metadata lock");
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testReplayStillRebuildsTheBaseTableRelationship() throws Exception {
        String mvName = "mv_replay_activate_relationship";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            Table base = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, BASE_TABLE);
            List<BaseTableInfo> infos = Lists.newArrayList(mv.getBaseTableInfos());
            mv.setInactiveAndReason("test: forced inactive before the replay");
            base.removeRelatedMaterializedView(mv.getMvId());

            GlobalStateMgr.getCurrentState().getAlterJobMgr().replayAlterMaterializedViewStatus(activeLog(mv, infos));

            awaitActive(mv);
            Assertions.assertTrue(base.getRelatedMaterializedViews().contains(mv.getMvId()),
                    "the replayed activation must link the base table back to the MV");
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testReplayWithAMissingBaseTableLeavesMvInactive() throws Exception {
        String mvName = "mv_replay_activate_missing";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            mv.setInactiveAndReason("test: forced inactive before the replay");
            List<BaseTableInfo> infos = Lists.newArrayList(
                    new BaseTableInfo(mv.getDbId(), DB_NAME, "t_replay_activate_no_such_table", Long.MAX_VALUE));

            GlobalStateMgr.getCurrentState().getAlterJobMgr().replayAlterMaterializedViewStatus(activeLog(mv, infos));

            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> mv.getInactiveReason() != null
                    && mv.getInactiveReason().contains("t_replay_activate_no_such_table"));
            Assertions.assertFalse(mv.isActive());
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /**
     * The rebuild runs outside the lock, so a later replayed status change can land while it is in flight.
     * Its verdict describes an older state and must not overwrite the newer one.
     */
    @Test
    public void testAStaleVerdictDoesNotOverrideALaterStatusChange() throws Exception {
        String mvName = "mv_replay_activate_stale";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" + HIVE_MV_DEFINITION);
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            List<BaseTableInfo> infos = Lists.newArrayList(mv.getBaseTableInfos());
            mv.setInactiveAndReason("test: forced inactive before the replay");

            installProbe();
            probe.gate = new CountDownLatch(1);
            CompletableFuture<?> rebuild = mv.activateOnReplay(infos);
            Assertions.assertTrue(probe.gateEntered.await(30, TimeUnit.SECONDS),
                    "the rebuild never reached the connector, so nothing was in flight");

            mv.setInactiveAndReason("test: a later replayed INACTIVE");
            probe.gate.countDown();
            rebuild.get(30, TimeUnit.SECONDS);

            Assertions.assertFalse(mv.isActive(), "the stale verdict re-activated the MV");
            Assertions.assertEquals("test: a later replayed INACTIVE", mv.getInactiveReason());
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /**
     * The rebuild runs outside the lock, so a DROP can be replayed while it is in flight. DROP cleans up the
     * base-table links, constraints and rewrite cache; whatever the rebuild publishes afterwards must not
     * resurrect them for an mv that no longer exists.
     */
    @Test
    public void testADropDuringTheRebuildLeavesNoLinkBehind() throws Exception {
        String mvName = "mv_replay_activate_dropped";
        String mixedBase = "t_replay_activate_mixed_base";
        starRocksAssert.withTable("CREATE TABLE " + mixedBase + " (l_orderkey int, v int) "
                + "DISTRIBUTED BY HASH(l_orderkey) PROPERTIES('replication_num'='1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT a.l_orderkey, b.v FROM hive0.partitioned_db.lineitem_par a "
                + "JOIN " + mixedBase + " b ON a.l_orderkey = b.l_orderkey;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            Table base = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, mixedBase);
            List<BaseTableInfo> infos = Lists.newArrayList(mv.getBaseTableInfos());
            mv.setInactiveAndReason("test: forced inactive before the replay");

            installProbe();
            probe.gate = new CountDownLatch(1);
            CompletableFuture<?> rebuild = mv.activateOnReplay(infos);
            Assertions.assertTrue(probe.gateEntered.await(30, TimeUnit.SECONDS),
                    "the rebuild never reached the connector, so nothing was in flight");

            starRocksAssert.dropMaterializedView(mvName);
            Assertions.assertFalse(base.getRelatedMaterializedViews().contains(mv.getMvId()));
            probe.gate.countDown();
            rebuild.get(30, TimeUnit.SECONDS);

            Assertions.assertFalse(base.getRelatedMaterializedViews().contains(mv.getMvId()),
                    "the rebuild re-linked a base table to a dropped MV");
            Assertions.assertFalse(mv.isActive(), "the rebuild re-activated a dropped MV");
        } finally {
            starRocksAssert.ddl("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            starRocksAssert.dropTable(mixedBase);
        }
    }

    /**
     * The checkpoint thread must not depend on an external catalog at all: it adopts the journaled state
     * synchronously, derives nothing, and the image it writes carries the MV as active with the leader's
     * base tables.
     */
    @Test
    public void testCheckpointReplayWritesTheLeadersStateIntoTheImage() throws Exception {
        String mvName = "mv_replay_activate_checkpoint";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" + HIVE_MV_DEFINITION);
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            List<BaseTableInfo> infos = Lists.newArrayList(mv.getBaseTableInfos());
            mv.setInactiveAndReason("test: forced inactive before the replay");

            installProbe();
            // Make exactly this thread the checkpoint thread, the way GlobalStateMgr#startCheckpoint does.
            Field checkpointThreadId = GlobalStateMgr.class.getDeclaredField("checkpointThreadId");
            checkpointThreadId.setAccessible(true);
            long previous = checkpointThreadId.getLong(null);
            checkpointThreadId.setLong(null, Thread.currentThread().getId());
            try {
                // Driven directly: on this thread getCurrentState() is the separate checkpoint catalog, which
                // this test does not populate. The checkpoint branch is entirely inside activateOnReplay.
                Assertions.assertTrue(GlobalStateMgr.isCheckpointThread());
                Assertions.assertTrue(mv.activateOnReplay(infos).isDone(),
                        "the checkpoint replay must not leave anything running behind it");
            } finally {
                checkpointThreadId.setLong(null, previous);
            }

            Assertions.assertTrue(mv.isActive(), "the checkpoint replay must adopt the leader's ACTIVE");
            Assertions.assertEquals(0, probe.calls.get() + probe.rebuildCalls.get(),
                    "the checkpoint replay reached the connector");

            Table reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(mv, Table.class), Table.class);
            Assertions.assertTrue(reloaded instanceof MaterializedView);
            Assertions.assertTrue(((MaterializedView) reloaded).isActive(), "the image lost the activation");
            Assertions.assertEquals(infos, ((MaterializedView) reloaded).getBaseTableInfos());
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }
}
