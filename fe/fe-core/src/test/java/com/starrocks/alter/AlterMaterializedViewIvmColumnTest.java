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
import com.starrocks.common.Config;
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

/**
 * An IVM MV's stored schema is position-aligned with its rewritten maintenance query, hidden
 * __ROW_ID__ / __AGG_STATE_* columns included. A column ALTER only maintains the visible half --
 * it never creates the companion __AGG_STATE_* column the rewrite derives for a new aggregate --
 * so IvmSchemaCompat.compare fails on every later refresh while is_active stays true. Reject the
 * DDL instead and point at the shadow-MV + SWAP rebuild.
 */
public class AlterMaterializedViewIvmColumnTest extends StarRocksTestBase {
    private static final String DB = "db_ivm_alter_col";

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
                + "  dt DATE NOT NULL, k INT NOT NULL, v BIGINT, w BIGINT\n"
                + ") DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
    }

    private static void createMv(String name, String refreshMode) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + "\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "PROPERTIES ('refresh_mode' = '" + refreshMode + "')\n"
                + "AS SELECT k, SUM(v) AS total FROM base_tbl GROUP BY k");
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

    private static final String IVM_GATE_MARKER = "incrementally maintained materialized view";

    private static void assertRejectedByIvmGate(String sql, MaterializedView mv) {
        int columnsBefore = mv.getColumns().size();
        Exception e = Assertions.assertThrows(Exception.class, () -> runAlter(sql));
        Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains(IVM_GATE_MARKER),
                "must be rejected by the IVM gate, got: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("SWAP"),
                "rejection must point at the SWAP rebuild, got: " + e.getMessage());
        Assertions.assertEquals(columnsBefore, mv.getColumns().size(),
                "a rejected ALTER must leave the schema untouched");
    }

    @Test
    public void testAddColumnRejectedOnIvmMv() throws Exception {
        createMv("mv_ivm_add", "INCREMENTAL");
        try {
            MaterializedView mv = getMv("mv_ivm_add");
            Assertions.assertNotNull(mv.getRowIdStrategy(),
                    "guard: this shape must materialize as a real IVM MV, else the test is vacuous");
            assertRejectedByIvmGate(
                    "alter materialized view mv_ivm_add add column w_total as sum(w)", mv);
        } finally {
            starRocksAssert.dropMaterializedView("mv_ivm_add");
        }
    }

    @Test
    public void testDropColumnRejectedOnIvmMv() throws Exception {
        createMv("mv_ivm_drop", "INCREMENTAL");
        try {
            MaterializedView mv = getMv("mv_ivm_drop");
            Assertions.assertNotNull(mv.getRowIdStrategy(),
                    "guard: this shape must materialize as a real IVM MV, else the test is vacuous");
            assertRejectedByIvmGate(
                    "alter materialized view mv_ivm_drop drop column total", mv);
        } finally {
            starRocksAssert.dropMaterializedView("mv_ivm_drop");
        }
    }

    /** force mode relaxes the fast schema evolution rules, so the gate has to sit ahead of them. */
    @Test
    public void testAddColumnRejectedUnderForceMode() throws Exception {
        createMv("mv_ivm_force", "INCREMENTAL");
        String originalMode = Config.mv_fast_schema_change_mode;
        try {
            Config.mv_fast_schema_change_mode = "force";
            assertRejectedByIvmGate(
                    "alter materialized view mv_ivm_force add column w_total as sum(w)",
                    getMv("mv_ivm_force"));
        } finally {
            Config.mv_fast_schema_change_mode = originalMode;
            starRocksAssert.dropMaterializedView("mv_ivm_force");
        }
    }

    /**
     * The reject is keyed on being an IVM MV, not on the ALTER shape. A PCT MV may still be refused
     * by the pre-existing FSE rules, so this only pins that the new IVM gate does not fire on it.
     */
    @Test
    public void testPctMvNotRejectedByIvmGate() throws Exception {
        createMv("mv_pct_add", "PCT");
        try {
            MaterializedView mv = getMv("mv_pct_add");
            Assertions.assertNull(mv.getRowIdStrategy(),
                    "guard: a PCT MV has no __ROW_ID__, else this case proves nothing");
            try {
                runAlter("alter materialized view mv_pct_add add column w_total as sum(w)");
            } catch (Exception e) {
                Assertions.assertFalse(e.getMessage() != null && e.getMessage().contains(IVM_GATE_MARKER),
                        "the IVM gate must not fire on a PCT MV, got: " + e.getMessage());
            }
        } finally {
            starRocksAssert.dropMaterializedView("mv_pct_add");
        }
    }
}
