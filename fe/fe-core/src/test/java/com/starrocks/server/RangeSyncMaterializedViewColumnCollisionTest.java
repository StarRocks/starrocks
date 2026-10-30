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

package com.starrocks.server;

import com.starrocks.alter.LakeRangeRollupJob;
import com.starrocks.alter.LakeRollupJob;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A generated MV name must not replace its expression with an unrelated real base column. */
public class RangeSyncMaterializedViewColumnCollisionTest {
    private static StarRocksAssert sr;
    private static ConnectContext context;
    private static GlobalStateMgr state;
    private static Database db;
    private static boolean savedRangeDistribution;

    @BeforeAll
    public static void setup() throws Exception {
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        savedRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        context = UtFrameUtils.createDefaultCtx();
        sr = new StarRocksAssert(context);
        sr.withDatabase("range_sync_column_collision").useDatabase("range_sync_column_collision");
        state = GlobalStateMgr.getCurrentState();
        db = state.getLocalMetastore().getDb("range_sync_column_collision");
    }

    @AfterEach
    public void removeTables() {
        state.getRollupHandler().clearJobs();
        for (com.starrocks.catalog.Table table : db.getTables()) {
            db.dropTable(table.getId());
        }
    }

    @AfterAll
    public static void teardown() {
        Config.enable_range_distribution = savedRangeDistribution;
    }

    private static OlapTable createTable(String columns, String keys, String distribution) throws Exception {
        sr.withTable("CREATE TABLE source (" + columns + ") " + keys + " " + distribution
                + " PROPERTIES('replication_num'='1')");
        return (OlapTable) db.getTable("source");
    }

    private static CreateSyncMVStmt analyze(String name, String query) throws Exception {
        return (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE MATERIALIZED VIEW " + name + " AS " + query, context);
    }

    private static LakeRangeRollupJob create(OlapTable table, CreateSyncMVStmt stmt) throws Exception {
        MaterializedViewHandler handler = state.getRollupHandler();
        handler.processCreateMaterializedView(stmt, db, table);
        LakeRangeRollupJob job = handler.getAlterJobsV2().values().stream()
                .filter(candidate -> candidate.getTableId() == table.getId())
                .map(candidate -> assertInstanceOf(LakeRangeRollupJob.class, candidate)).findFirst().orElseThrow();
        handler.clearJobs();
        return job;
    }

    private static void assertCollisionRejected(OlapTable table, String query, String name) throws Exception {
        CreateSyncMVStmt stmt = analyze("collision_mv", query);
        int maxUniqueId = table.getMaxColUniqueId();
        String schema = GsonUtils.GSON.toJson(table.getBaseSchema());
        int indexes = table.getIndexMetaIdToMeta().size();
        DdlException error = assertThrows(DdlException.class,
                () -> state.getRollupHandler().processCreateMaterializedView(stmt, db, table));
        assertTrue(error.getMessage().contains("conflicts with base column"), error.getMessage());
        assertTrue(error.getMessage().toLowerCase(java.util.Locale.ROOT).contains(name.toLowerCase(java.util.Locale.ROOT)),
                error.getMessage());
        assertEquals(maxUniqueId, table.getMaxColUniqueId(), "reject before converting any MV column");
        assertEquals(schema, GsonUtils.GSON.toJson(table.getBaseSchema()));
        assertEquals(indexes, table.getIndexMetaIdToMeta().size());
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertTrue(state.getRollupHandler().getAlterJobsV2().isEmpty());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "SELECT g, SUM(v) FROM source GROUP BY g | mv_sum_v",
            "SELECT g, COUNT(v) FROM source GROUP BY g | mv_count_v",
            "SELECT g + 10 AS shifted, v FROM source | mv_shifted",
            "SELECT g, SUM(v + 1) AS expr FROM source GROUP BY g | mv_expr",
            "SELECT g, SUM(v) FROM source GROUP BY g | MV_SUM_V",
            "SELECT g + 1 AS fresh_key, SUM(v) FROM source GROUP BY g + 1 | mv_sum_v"
    })
    public void testGeneratedNameCollisionRejectedBeforeMutation(String query, String column) throws Exception {
        OlapTable table = createTable("id INT, g BIGINT, v BIGINT, " + column + " BIGINT",
                "DUPLICATE KEY(id)", "ORDER BY(id)");
        assertTrue(MaterializedViewHandler.isRangeRollupRoutable(table));
        assertCollisionRejected(table, query, column);
    }

    @Test
    public void testAggregateTableCollisionRejected() throws Exception {
        OlapTable table = createTable("g BIGINT, v BIGINT SUM, mv_sum_v BIGINT SUM",
                "AGGREGATE KEY(g)", "ORDER BY(g)");
        assertCollisionRejected(table, "SELECT g, SUM(v) FROM source GROUP BY g", "mv_sum_v");
    }

    @Test
    public void testDirectProjectionOfRealMvPrefixedColumnAllowed() throws Exception {
        OlapTable table = createTable("id INT, g BIGINT, mv_real BIGINT", "DUPLICATE KEY(id)", "ORDER BY(id)");
        LakeRangeRollupJob job = create(table, analyze("direct_mv", "SELECT g, MV_REAL AS alias FROM source"));
        MaterializedIndexMeta meta = Deencapsulation.invoke(job, "restoredSyncIndexMeta", table);
        Column value = meta.getSchema().get(1);
        assertEquals("mv_real", value.getName().toLowerCase(java.util.Locale.ROOT));
        assertEquals(table.getBaseColumn("mv_real").getUniqueId(), value.getUniqueId());
        assertNull(value.getDefineExpr());
        assertEquals("`mv_real`", meta.getSyncMvDefinition().projectionSql(value).toLowerCase(java.util.Locale.ROOT));
    }

    @Test
    public void testNonCollidingExpressionRetainedWithMvPrefixedBaseColumn() throws Exception {
        OlapTable table = createTable("id INT, g BIGINT, v BIGINT, mv_real BIGINT",
                "DUPLICATE KEY(id)", "ORDER BY(id)");
        LakeRangeRollupJob job = create(table, analyze("sum_mv", "SELECT g, SUM(v) FROM source GROUP BY g"));
        MaterializedIndexMeta meta = Deencapsulation.invoke(job, "restoredSyncIndexMeta", table);
        Column value = meta.getSchema().get(1);
        assertNotNull(value.getDefineExpr());
        assertEquals("`v`", meta.getSyncMvDefinition().projectionSql(value));
        assertTrue(value.getUniqueId() > table.getBaseColumn("mv_real").getUniqueId());
    }

    @Test
    public void testSameExpressionInExistingDerivedIndexAllowed() throws Exception {
        OlapTable table = createTable("id INT, g BIGINT, v BIGINT", "DUPLICATE KEY(id)", "ORDER BY(id)");
        LakeRangeRollupJob first = create(table, analyze("first_mv", "SELECT g, SUM(v) FROM source GROUP BY g"));
        Deencapsulation.invoke(first, "beforeShadowBuild");
        Deencapsulation.invoke(first, "registerShadowIndexMeta", table);
        table.setState(OlapTable.OlapTableState.NORMAL);
        assertNotNull(table.getColumn("mv_sum_v"));
        assertNull(table.getBaseColumn("mv_sum_v"));
        LakeRangeRollupJob second = create(table, analyze("second_mv", "SELECT g, SUM(v) FROM source GROUP BY g"));
        MaterializedIndexMeta meta = Deencapsulation.invoke(second, "restoredSyncIndexMeta", table);
        assertNotNull(meta.getSchema().get(1).getDefineExpr());
        assertEquals("`v`", meta.getSyncMvDefinition().projectionSql(meta.getSchema().get(1)));
    }

    @Test
    public void testHashTableKeepsExistingCreationPath() throws Exception {
        OlapTable table = createTable("id INT, g BIGINT, v BIGINT, mv_sum_v BIGINT",
                "DUPLICATE KEY(id)", "DISTRIBUTED BY HASH(id) BUCKETS 1");
        assertFalse(table.isRangeDistribution());
        state.getRollupHandler().processCreateMaterializedView(
                analyze("hash_mv", "SELECT g, SUM(v) FROM source GROUP BY g"), db, table);
        assertEquals(1, state.getRollupHandler().getAlterJobsV2().size());
        assertInstanceOf(LakeRollupJob.class, state.getRollupHandler().getAlterJobsV2().values().iterator().next());
    }
}
