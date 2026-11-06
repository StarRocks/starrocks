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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.TableName;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The journal must only record activations that actually happened.
 *
 * <p>ALTER MATERIALIZED VIEW ... ACTIVE used to journal the transition first and rebuild the MV's
 * base-table relationship afterwards, inside the WAL applier. That violates WALApplier's "apply can
 * not fail" contract: {@code fixRelationship()} swallows its own failure and just leaves the MV
 * inactive, so the entry stayed durable while the leader's memory said inactive. A permanently
 * broken MV therefore grew one bogus entry per MVActiveChecker round, without bound.
 */
public class AlterMVActivateJournalTest extends MVTestBase {

    private static final String BASE_TABLE = "t_activate_journal_base";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE " + BASE_TABLE + " (\n"
                + "  k1 date,\n"
                + "  k2 int,\n"
                + "  v1 int\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES ('replication_num' = '1');");
    }

    /**
     * Point a partitioned MV's persisted partition expr at a base table that has no partition
     * columns at all, which is what an Iceberg base table looks like once its read spec becomes
     * unpartitioned. getBaseTablePartitionColumnMapImpl then skips every base table and throws
     * "Can not find partition column map" from inside the relationship rebuild.
     */
    private static void breakPartitionColumnMap(MaterializedView mv) {
        SlotRef partitionSlot = new SlotRef(new TableName(DB_NAME, BASE_TABLE), "k1");
        LinkedHashMap<Expr, SlotRef> partitionExprMaps = new LinkedHashMap<>();
        partitionExprMaps.put(partitionSlot, partitionSlot);
        mv.setPartitionExprMaps(partitionExprMaps);
    }

    @Test
    public void testActivateFailureWritesNoJournalEntry() throws Exception {
        String mvName = "mv_activate_journal_broken";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            breakPartitionColumnMap(mv);
            mv.setInactiveAndReason("test: forced inactive before the activation attempt");
            Assertions.assertFalse(mv.isActive());

            EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
            EditLog spyEditLog = spy(originalEditLog);
            GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
            try {
                Exception exception = Assertions.assertThrows(Exception.class,
                        () -> starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " ACTIVE"),
                        "activating an MV whose relationship cannot be rebuilt must fail loudly");
                Assertions.assertTrue(exception.getMessage() != null
                                && exception.getMessage().contains("Can not find partition column map"),
                        "the error must carry the reason the MV stayed inactive, got: " + exception.getMessage());

                Assertions.assertFalse(mv.isActive(), "a failed activation must leave the MV inactive");
                verify(spyEditLog, never()).logAlterMvStatus(any(AlterMaterializedViewStatusLog.class), any());
            } finally {
                GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
            }
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /**
     * The control: a genuine activation still journals exactly one entry. This also proves the spy
     * above can observe logAlterMvStatus at all, so the never() assertion is not vacuous.
     */
    @Test
    public void testActivateSuccessWritesExactlyOneJournalEntry() throws Exception {
        String mvName = "mv_activate_journal_ok";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " INACTIVE");
            Assertions.assertFalse(mv.isActive());

            EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
            EditLog spyEditLog = spy(originalEditLog);
            GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
            try {
                starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " ACTIVE");
                Assertions.assertTrue(mv.isActive(), "a healthy MV must still activate");
                verify(spyEditLog, times(1))
                        .logAlterMvStatus(any(AlterMaterializedViewStatusLog.class), any());
            } finally {
                GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
            }
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /**
     * A journal write that never commits means the activation did not happen. Because the rebuild now
     * runs before journaling, it has already called setActive() -- which republishes the MV to the
     * query-rewrite cache -- by the time the write fails. The MV must be put back, or this leader
     * would keep rewriting queries with an MV that is active in memory only and whose refresh task
     * was never resumed, so it would never be refreshed again either.
     */
    @Test
    public void testActivateLeavesMvInactiveWhenJournalWriteFails() throws Exception {
        String mvName = "mv_activate_journal_write_fails";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " INACTIVE");
            Assertions.assertFalse(mv.isActive());
            String reasonBefore = mv.getInactiveReason();

            EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
            EditLog spyEditLog = spy(originalEditLog);
            doThrow(new RuntimeException("EditLog write failed"))
                    .when(spyEditLog).logAlterMvStatus(any(AlterMaterializedViewStatusLog.class), any());
            GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
            try {
                Exception exception = Assertions.assertThrows(Exception.class,
                        () -> starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " ACTIVE"));
                Assertions.assertTrue(exception.getMessage() != null
                                && exception.getMessage().contains("EditLog write failed"),
                        "the journal failure must surface, got: " + exception.getMessage());

                Assertions.assertFalse(mv.isActive(),
                        "an activation whose journal write failed must not leave the MV active");
                Assertions.assertEquals(reasonBefore, mv.getInactiveReason(),
                        "the inactive reason the statement started from must be restored");
            } finally {
                GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
            }
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /**
     * Replay symmetry: a journaled ACTIVE that this FE cannot reproduce must leave the MV inactive
     * rather than active. On the leader the entry now only exists when the activation genuinely
     * succeeded, so a follower that cannot rebuild the relationship has diverged and must not
     * silently present the MV as usable.
     */
    @Test
    public void testReplayOfActivateThatCannotBeRebuiltLeavesMvInactive() throws Exception {
        String mvName = "mv_activate_journal_replay";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "REFRESH MANUAL\n"
                + "AS SELECT k1, sum(v1) AS s FROM " + BASE_TABLE + " GROUP BY k1;");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            breakPartitionColumnMap(mv);
            mv.setInactiveAndReason("test: forced inactive before the replay");
            Assertions.assertFalse(mv.isActive());

            AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(
                    mv.getDbId(), mv.getId(), AlterMaterializedViewStatusClause.ACTIVE, "");
            GlobalStateMgr.getCurrentState().getAlterJobMgr().replayAlterMaterializedViewStatus(log);

            Assertions.assertFalse(mv.isActive(),
                    "replaying an ACTIVE entry this FE cannot reproduce must not mark the MV active");
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }
}
