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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * auto_refresh_partitions_limit leaves the oldest partitions unrefreshed on purpose. PCT absorbs
 * that; incremental maintenance cannot, because the truncated bootstrap becomes its baseline while
 * the bookmark still advances to the base table head. So the combination is refused at admission.
 */
public class MaterializedViewIvmAutoRefreshLimitTest extends StarRocksTestBase {
    private static final String DB = "db_ivm_arpl";

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        ctx = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(ctx);
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        starRocksAssert.withTable("CREATE TABLE base_tbl (\n"
                + "  dt DATE NOT NULL, k INT NOT NULL, v BIGINT NOT NULL\n"
                + ") DUPLICATE KEY(dt, k)\n"
                + "PARTITION BY date_trunc('day', dt)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
    }

    /** The property rejects MANUAL outright, so every case here has to be interval-scheduled. */
    private static String ddl(String name, String properties) {
        return "CREATE MATERIALIZED VIEW " + name + "\n"
                + "PARTITION BY dt\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "REFRESH ASYNC EVERY(INTERVAL 1 DAY)\n"
                + "PROPERTIES (" + properties + ")\n"
                + "AS SELECT dt, k, SUM(v) AS total FROM base_tbl GROUP BY dt, k";
    }

    private static MaterializedView getMv(String name) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        return (MaterializedView) db.getTable(name);
    }

    private static void runAlter(String sql) throws Exception {
        AlterMaterializedViewStmt stmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
    }

    private static final String GATE_MARKER = "auto_refresh_partitions_limit";
    private static final String GATE_ADVICE = "partition_refresh_number";

    private static void assertRejectedByLimitGate(Executable action, String what) {
        Exception e = Assertions.assertThrows(Exception.class, action, what);
        String msg = String.valueOf(e.getMessage());
        Assertions.assertTrue(msg.contains(GATE_MARKER) && msg.contains("refresh_mode"),
                "must be rejected by the limit-vs-refresh_mode gate, got: " + msg);
        Assertions.assertTrue(msg.contains(GATE_ADVICE),
                "rejection must name the throttle that does not drop partitions, got: " + msg);
    }

    /**
     * Proves the shape really does materialize as incremental, so a rejection case built on the same
     * DDL is testing the gate rather than an unrelated refusal.
     */
    private static void assertShapeIsGenuinelyIncremental(String mode) throws Exception {
        String name = "mv_probe_" + mode;
        starRocksAssert.withMaterializedView(ddl(name, "'refresh_mode' = '" + mode + "'"));
        try {
            Assertions.assertTrue(getMv(name).getCurrentRefreshMode().isIncrementalOrAuto(),
                    "guard: without the limit this DDL must settle as " + mode
                            + ", else the rejection case is vacuous");
        } finally {
            starRocksAssert.dropMaterializedView(name);
        }
    }

    @Test
    public void testCreateRejectsAutoRefreshLimitOnIncrementalMv() throws Exception {
        assertShapeIsGenuinelyIncremental("incremental");
        assertRejectedByLimitGate(
                () -> starRocksAssert.withMaterializedView(ddl("mv_inc_limit",
                        "'refresh_mode' = 'incremental', 'auto_refresh_partitions_limit' = '2'")),
                "CREATE with both properties must be refused");
    }

    @Test
    public void testCreateRejectsAutoRefreshLimitOnAutoMv() throws Exception {
        assertShapeIsGenuinelyIncremental("auto");
        assertRejectedByLimitGate(
                () -> starRocksAssert.withMaterializedView(ddl("mv_auto_limit",
                        "'refresh_mode' = 'auto', 'auto_refresh_partitions_limit' = '2'")),
                "CREATE with both properties must be refused");
    }

    /** Non-regression: the property's whole point is the PCT rolling window, which stays legal. */
    @Test
    public void testCreateAllowsAutoRefreshLimitOnPctMv() throws Exception {
        starRocksAssert.withMaterializedView(ddl("mv_pct_limit",
                "'refresh_mode' = 'pct', 'auto_refresh_partitions_limit' = '2'"));
        try {
            MaterializedView mv = getMv("mv_pct_limit");
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
            Assertions.assertEquals(2, mv.getTableProperty().getAutoRefreshPartitionsLimit());
        } finally {
            starRocksAssert.dropMaterializedView("mv_pct_limit");
        }
    }

    /** Non-regression: the throttle the rejection points users at must itself stay legal on IVM. */
    @Test
    public void testCreateAllowsPartitionRefreshNumberOnIncrementalMv() throws Exception {
        starRocksAssert.withMaterializedView(ddl("mv_inc_batch",
                "'refresh_mode' = 'incremental', 'partition_refresh_number' = '2'"));
        try {
            MaterializedView mv = getMv("mv_inc_batch");
            Assertions.assertTrue(mv.getCurrentRefreshMode().isIncrementalOrAuto());
            Assertions.assertEquals(2, mv.getTableProperty().getPartitionRefreshNumber());
        } finally {
            starRocksAssert.dropMaterializedView("mv_inc_batch");
        }
    }

    @Test
    public void testAlterRejectsAutoRefreshLimitOnIncrementalMv() throws Exception {
        starRocksAssert.withMaterializedView(ddl("mv_inc_alter", "'refresh_mode' = 'incremental'"));
        try {
            MaterializedView mv = getMv("mv_inc_alter");
            Assertions.assertTrue(mv.getCurrentRefreshMode().isIncrementalOrAuto(),
                    "guard: must be a real IVM MV, else the case is vacuous");
            assertRejectedByLimitGate(
                    () -> runAlter("alter materialized view mv_inc_alter "
                            + "set ('auto_refresh_partitions_limit' = '2')"),
                    "ALTER SET on an IVM MV must be refused");
            Assertions.assertEquals(-1, mv.getTableProperty().getAutoRefreshPartitionsLimit(),
                    "a refused ALTER must leave the property untouched");
        } finally {
            starRocksAssert.dropMaterializedView("mv_inc_alter");
        }
    }

    /**
     * Rejecting -1 too would strand every view created before the gate, with no way to clear it.
     * The gate makes that starting state unreachable through DDL, so it is seeded directly --
     * otherwise the ALTER is a no-op and the case passes without exercising anything.
     */
    @Test
    public void testAlterAllowsClearingAutoRefreshLimitOnIncrementalMv() throws Exception {
        starRocksAssert.withMaterializedView(ddl("mv_inc_clear", "'refresh_mode' = 'incremental'"));
        try {
            MaterializedView mv = getMv("mv_inc_clear");
            mv.getTableProperty().setAutoRefreshPartitionsLimit(2);

            runAlter("alter materialized view mv_inc_clear set ('auto_refresh_partitions_limit' = '-1')");

            Assertions.assertEquals(-1, mv.getTableProperty().getAutoRefreshPartitionsLimit(),
                    "clearing the property must take effect, not silently no-op");
        } finally {
            starRocksAssert.dropMaterializedView("mv_inc_clear");
        }
    }

    /**
     * The third way into the combination -- putting refresh_mode on an MV that already carries the
     * limit -- is closed by the pre-existing cross-mode refusal, not by this gate. Pinned so that
     * relaxing that refusal cannot silently reopen the hole.
     */
    @Test
    public void testAlterRefreshModeOntoLimitedMvStaysRefused() throws Exception {
        starRocksAssert.withMaterializedView(ddl("mv_pct_promote",
                "'refresh_mode' = 'pct', 'auto_refresh_partitions_limit' = '2'"));
        try {
            Exception e = Assertions.assertThrows(Exception.class,
                    () -> runAlter("alter materialized view mv_pct_promote "
                            + "set ('refresh_mode' = 'incremental')"));
            Assertions.assertTrue(String.valueOf(e.getMessage()).contains("Altering refresh_mode"),
                    "expected the cross-mode refusal, got: " + e.getMessage());
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT,
                    getMv("mv_pct_promote").getCurrentRefreshMode());
        } finally {
            starRocksAssert.dropMaterializedView("mv_pct_promote");
        }
    }
}
