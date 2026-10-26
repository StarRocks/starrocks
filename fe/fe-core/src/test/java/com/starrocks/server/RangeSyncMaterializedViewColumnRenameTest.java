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

import com.starrocks.alter.AlterMVJobExecutor;
import com.starrocks.alter.LakeRangeRollupJob;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Renaming a source column must not leave an authoritative synchronous MV image unrecoverable. */
public class RangeSyncMaterializedViewColumnRenameTest {
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
        UtFrameUtils.setUpForPersistTest();
        savedRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        context = UtFrameUtils.createDefaultCtx();
        sr = new StarRocksAssert(context);
        sr.withDatabase("range_sync_column_rename").useDatabase("range_sync_column_rename");
        state = GlobalStateMgr.getCurrentState();
        db = state.getLocalMetastore().getDb("range_sync_column_rename");
    }

    @AfterEach
    public void removeTables() {
        state.getSchemaChangeHandler().clearJobs();
        for (com.starrocks.catalog.Table table : db.getTables()) {
            db.dropTable(table.getId());
        }
    }

    @AfterAll
    public static void teardown() {
        Config.enable_range_distribution = savedRangeDistribution;
        UtFrameUtils.tearDownForPersisTest();
    }

    private static OlapTable fixture(String name, String select) throws Exception {
        sr.withTable("CREATE TABLE " + name + " (id INT, g INT, v BIGINT, filter_col INT, unused INT) "
                + "DUPLICATE KEY(id) ORDER BY(id) PROPERTIES('replication_num'='1')");
        return installMv(name, "mv_" + name, select, " WHERE filter_col > 0");
    }

    private static OlapTable installMv(String name, String mvName, String select, String predicate) throws Exception {
        OlapTable table = (OlapTable) db.getTable(name);
        assertTrue(table.isRangeDistribution());
        CreateSyncMVStmt create = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE MATERIALIZED VIEW " + mvName + " AS SELECT " + select + " FROM " + name
                        + predicate + ((select.contains("SUM(") || select.contains("AVG_UNION(")) ? " GROUP BY g" : ""), context);
        MaterializedViewHandler handler = state.getRollupHandler();
        handler.processCreateMaterializedView(create, db, table);
        LakeRangeRollupJob job = handler.getAlterJobsV2().values().stream()
                .filter(candidate -> candidate.getTableId() == table.getId())
                .map(candidate -> assertInstanceOf(LakeRangeRollupJob.class, candidate)).findFirst().orElseThrow();
        handler.clearJobs();
        Deencapsulation.invoke(job, "beforeShadowBuild");
        Deencapsulation.invoke(job, "registerShadowIndexMeta", table);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            MaterializedIndex index = new MaterializedIndex(state.getNextId(), job.getShadowIndexMetaId(),
                    MaterializedIndex.IndexState.NORMAL,
                    partition.getLatestIndex(table.getBaseIndexMetaId()).getShardGroupId());
            partition.createRollupIndex(index);
        }
        table.rebuildFullSchema();
        table.setState(OlapTable.OlapTableState.NORMAL);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
        return table;
    }

    private static OlapTable roundTrip(OlapTable table) {
        OlapTable recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        db.dropTable(table.getId());
        assertTrue(db.registerTableUnlocked(recovered));
        return recovered;
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "projection | g, v | g",
            "mixed_case | g, v | G",
            "value_column | g, v | v",
            "alias | g AS renamed_g, v | g",
            "computed | g + 10 AS shifted_g, v | g"
    })
    public void testReferencedProjectionRenameRejectedBeforeImage(String name, String select, String column) throws Exception {
        OlapTable table = fixture(name, select);
        MaterializedIndexMeta mv = table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_" + name));
        assertFalse(mv.getSchema().stream().anyMatch(Column::isAggregated));
        Column sourceColumn = table.getBaseColumn(column);
        assertNotNull(sourceColumn);
        String definition = GsonUtils.GSON.toJson(mv.getSyncMvDefinition());
        Exception failure = assertThrows(Exception.class,
                () -> sr.alterTable("ALTER TABLE " + name + " RENAME COLUMN " + column + " TO renamed_column"));
        assertTrue(failure.getMessage().contains("rename"), failure.getMessage());
        assertTrue(failure.getMessage().contains("mv_" + name), failure.getMessage());
        assertSame(sourceColumn, table.getBaseColumn(column));
        assertNull(table.getBaseColumn("renamed_column"));
        OlapTable recovered = roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
        assertEquals(definition, GsonUtils.GSON.toJson(
                recovered.getIndexMetaByMetaId(recovered.getIndexMetaIdByName("mv_" + name)).getSyncMvDefinition()));
    }

    @Test
    public void testUnrelatedColumnRenamePreservesImageRecovery() throws Exception {
        OlapTable table = fixture("unrelated_source", "g, v");
        sr.alterTable("ALTER TABLE unrelated_source RENAME COLUMN unused TO unused2");
        assertNotNull(table.getBaseColumn("unused2"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testWhereColumnRenameRemainsRejected() throws Exception {
        OlapTable table = fixture("where_source", "g, v");
        Exception failure = assertThrows(Exception.class,
                () -> sr.alterTable("ALTER TABLE where_source RENAME COLUMN filter_col TO filter2"));
        assertTrue(failure.getMessage().contains("where expr"), failure.getMessage());
        assertNotNull(table.getBaseColumn("filter_col"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testAggregatedMvGroupingColumnRenameRemainsRejected() throws Exception {
        OlapTable table = fixture("aggregate_source", "g, SUM(v)");
        Exception failure = assertThrows(Exception.class,
                () -> sr.alterTable("ALTER TABLE aggregate_source RENAME COLUMN g TO g2"));
        assertTrue(failure.getMessage().contains("rollup"), failure.getMessage());
        assertNotNull(table.getBaseColumn("g"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testRenameWithoutMvRemainsAllowed() throws Exception {
        sr.withTable("CREATE TABLE no_mv (id INT, g INT) DUPLICATE KEY(id) ORDER BY(id) "
                + "PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTable("no_mv");
        assertTrue(table.isRangeDistribution());
        sr.alterTable("ALTER TABLE no_mv RENAME COLUMN g TO g2");
        assertNotNull(table.getBaseColumn("g2"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testLegacyProjectionRenameKeepsExistingBehavior() throws Exception {
        OlapTable table = fixture("legacy_source", "g, v");
        table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_legacy_source")).setSyncMvDefinition(null);
        sr.alterTable("ALTER TABLE legacy_source RENAME COLUMN g TO g2");
        assertNotNull(table.getBaseColumn("g2"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testModifyAndDropDependencyCheckKeepsExistingBehavior() throws Exception {
        OlapTable table = fixture("schema_source", "g, v");
        assertDoesNotThrow(() -> AlterMVJobExecutor.checkModifiedColumWithMaterializedViews(table, Set.of("g", "v")));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "computed_drop | g + 10 AS shifted_g, v | g",
            "computed_drop_case | g + 10 AS shifted_g, v | G",
            "where_drop | g, v | filter_col",
            "where_drop_case | g, v | FILTER_COL"
    })
    public void testReferencedExpressionDropRejectedBeforeImage(String name, String select, String column) throws Exception {
        OlapTable table = fixture(name, select);
        assertRejectedDropPreservesImage(table, "ALTER TABLE " + name + " DROP COLUMN " + column, column);
    }

    private static void assertRejectedDropPreservesImage(OlapTable table, String sql, String column) {
        Column before = table.getBaseColumn(column);
        String metadata = GsonUtils.GSON.toJson(table.getIndexMetaIdToMeta());
        Exception failure = assertThrows(Exception.class, () -> sr.alterTable(sql));
        assertTrue(failure.getMessage().contains("rollup"), failure.getMessage());
        assertSame(before, table.getBaseColumn(column));
        assertEquals(metadata, GsonUtils.GSON.toJson(table.getIndexMetaIdToMeta()));
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testAggregateBaseExpressionSourceDropRejectedBeforeImage() throws Exception {
        sr.withTable("CREATE TABLE aggregate_drop (g BIGINT, v BIGINT SUM, unused BIGINT SUM) "
                + "AGGREGATE KEY(g) ORDER BY(g) PROPERTIES('replication_num'='1')");
        OlapTable table = installMv("aggregate_drop", "mv_aggregate_drop", "g, SUM(v)", "");
        assertRejectedDropPreservesImage(table, "ALTER TABLE aggregate_drop DROP COLUMN v", "v");
    }

    @Test
    public void testUnrelatedSourceDropKeepsImageRecoverable() throws Exception {
        OlapTable table = fixture("unrelated_drop", "g + 10 AS shifted_g, v");
        sr.alterTable("ALTER TABLE unrelated_drop DROP COLUMN unused");
        assertNull(table.getBaseColumn("unused"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testDirectValueDropReconcilesDefinition() throws Exception {
        OlapTable table = fixture("direct_drop", "id, g, unused, v");
        sr.alterTable("ALTER TABLE direct_drop DROP COLUMN v");
        assertNull(table.getBaseColumn("v"));
        assertTrue(table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_direct_drop")).getSchema().stream()
                .noneMatch(column -> column.getName().equalsIgnoreCase("v")));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testDropFromOneMvDoesNotDropOtherMvSource() throws Exception {
        OlapTable table = fixture("targeted_drop", "id, g, unused, v");
        installMv("targeted_drop", "mv_targeted_expr", "id, g, unused, v + 10 AS shifted", " WHERE filter_col > 0");
        sr.alterTable("ALTER TABLE targeted_drop DROP COLUMN v FROM mv_targeted_drop");
        assertNotNull(table.getBaseColumn("v"));
        assertTrue(table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_targeted_drop")).getSchema().stream()
                .noneMatch(column -> column.getName().equalsIgnoreCase("v")));
        assertNotNull(table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_targeted_expr"))
                .getSchema().stream().filter(column -> column.getName().equals("mv_shifted"))
                .findFirst().orElseThrow().getDefineExpr());
        assertRejectedDropPreservesImage(table, "ALTER TABLE targeted_drop DROP COLUMN v", "v");
    }

    @Test
    public void testDropDerivedValueFromMvReconcilesDefinition() throws Exception {
        OlapTable table = fixture("derived_drop", "id, g, unused, v + 10 AS shifted");
        sr.alterTable("ALTER TABLE derived_drop DROP COLUMN mv_shifted FROM mv_derived_drop");
        assertNotNull(table.getBaseColumn("v"));
        assertTrue(table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_derived_drop")).getSchema().stream()
                .noneMatch(column -> column.getName().equals("mv_shifted")));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    private static OlapTable structFixture(String name, boolean generated, boolean where) throws Exception {
        String struct = generated ? "STRUCT<a INT,b INT> AS named_struct('a',id,'b',id)" : "STRUCT<a INT,b INT>";
        sr.withTable("CREATE TABLE " + name + " (id INT,v BIGINT,s " + struct + ") DUPLICATE KEY(id) ORDER BY(id) "
                + "PROPERTIES('replication_num'='1','cloud_native_fast_schema_evolution_v2'='true')");
        return installMv(name, "mv_" + name, where ? "id,v" : "s.a + 1 AS projected,v", where ? " WHERE s.a > 0" : "");
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "field_drop | MODIFY COLUMN s DROP FIELD a | false | false",
            "field_case | MODIFY COLUMN s DROP FIELD A | false | false",
            "root_drop | DROP COLUMN S | false | false",
            "root_rename | RENAME COLUMN s TO s2 | false | false",
            "where_field | MODIFY COLUMN s DROP FIELD a | false | true",
            "where_root_drop | DROP COLUMN S | false | true",
            "where_root_rename | RENAME COLUMN s TO s2 | false | true",
            "generated_shape | MODIFY COLUMN s STRUCT<b INT> AS named_struct('b',id) | true | false"
    })
    public void testStructDependenciesRejectDestructiveDdl(String name, String clause, boolean generated, boolean where)
            throws Exception {
        OlapTable table = structFixture(name, generated, where);
        String metadata = GsonUtils.GSON.toJson(table.getIndexMetaIdToMeta());
        Exception error = assertThrows(Exception.class, () -> sr.alterTable("ALTER TABLE " + name + " " + clause));
        assertTrue(error.getMessage().contains("rollup"), error.getMessage());
        assertEquals(metadata, GsonUtils.GSON.toJson(table.getIndexMetaIdToMeta()));
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertTrue(state.getSchemaChangeHandler().getAlterJobsV2().isEmpty());
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "add_field | ADD FIELD c INT | named_struct('a',3,'b',4,'c',5)",
            "drop_unreferenced_field | DROP FIELD b | named_struct('a',3)"
    })
    public void testCompatibleStructFieldChangePreservesDefinitions(String name, String clause, String value) throws Exception {
        OlapTable table = structFixture(name, false, false);
        sr.alterTable("ALTER TABLE " + name + " MODIFY COLUMN s " + clause);
        assertNotNull(UtFrameUtils.getPlanAndFragment(context,
                "INSERT INTO " + name + " VALUES (1,2," + value + ")").second);
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testDottedRealColumnIsNotAStructDependency() throws Exception {
        sr.withTable("CREATE TABLE dotted_source (id INT,s INT,`s.a` INT,v BIGINT) "
                + "DUPLICATE KEY(id) ORDER BY(id) PROPERTIES('replication_num'='1')");
        OlapTable table = installMv("dotted_source", "mv_dotted_source", "`s.a` + 1 AS projected,v", "");
        sr.alterTable("ALTER TABLE dotted_source RENAME COLUMN s TO s2");
        assertNotNull(table.getBaseColumn("s.a"));
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @ParameterizedTest
    @ValueSource(strings = {"BIGINT", "DECIMAL(18,4)"})
    public void testGeneratedFieldTypeCannotChangeStoredAggregateState(String type) throws Exception {
        sr.withTable("CREATE TABLE state_field_source (id INT,g INT,s STRUCT<a INT,b INT> "
                + "AS named_struct('a',g,'b',g)) DUPLICATE KEY(id) ORDER BY(id) PROPERTIES('replication_num'='1')");
        OlapTable table = installMv("state_field_source", "mv_state_field_source",
                "g, AVG_UNION(AVG_STATE(s.a)) AS av", "");
        String metadata = GsonUtils.GSON.toJson(table.getIndexMetaIdToMeta());
        Exception error = assertThrows(Exception.class, () -> sr.alterTable(
                "ALTER TABLE state_field_source MODIFY COLUMN s STRUCT<a " + type + ",b INT> "
                        + "AS named_struct('a',CAST(g AS " + type + "),'b',g)"));
        assertTrue(error.getMessage().contains("state"), error.getMessage());
        assertEquals(metadata, GsonUtils.GSON.toJson(table.getIndexMetaIdToMeta()));
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertTrue(state.getSchemaChangeHandler().getAlterJobsV2().isEmpty());
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }

    @Test
    public void testCompatibleStructFieldChangeKeepsAggregateStateContract() throws Exception {
        sr.withTable("CREATE TABLE compatible_state_fields (id INT,g INT,s STRUCT<b INT,a INT>) "
                + "DUPLICATE KEY(id) ORDER BY(id) PROPERTIES('replication_num'='1')");
        OlapTable table = installMv("compatible_state_fields", "mv_compatible_state_fields",
                "g, AVG_UNION(AVG_STATE(s.a)) AS av", "");
        sr.alterTable("ALTER TABLE compatible_state_fields MODIFY COLUMN s DROP FIELD b");
        sr.alterTable("ALTER TABLE compatible_state_fields MODIFY COLUMN s ADD FIELD c INT");
        assertNotNull(UtFrameUtils.getPlanAndFragment(context,
                "INSERT INTO compatible_state_fields VALUES (1,2,named_struct('a',3,'c',4))").second);
        roundTrip(table);
        assertDoesNotThrow(state::postLoadImageWithRecoveryPolicy);
    }
}
