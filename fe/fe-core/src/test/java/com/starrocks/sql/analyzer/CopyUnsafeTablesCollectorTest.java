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

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Only a table the meta lock can actually protect -- one living in an internal database -- may decide how long
 * that lock is held. Tables in an external catalog abstain, and are never in the lock set to begin with.
 * Everything else keeps both properties, including the kinds whose engine is "external".
 */
public class CopyUnsafeTablesCollectorTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void createInternalDbExternalEngineTables() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("CREATE EXTERNAL TABLE mysql_ext_tbl\n" +
                        "(\n" +
                        "    k1 INT,\n" +
                        "    k2 VARCHAR(64)\n" +
                        ")\n" +
                        "ENGINE=mysql\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    \"host\" = \"127.0.0.1\",\n" +
                        "    \"port\" = \"3306\",\n" +
                        "    \"user\" = \"mysql_user\",\n" +
                        "    \"password\" = \"mysql_passwd\",\n" +
                        "    \"database\" = \"mysql_db_test\",\n" +
                        "    \"table\" = \"mysql_table_test\"\n" +
                        ");")
                .withView("CREATE VIEW lock_scope_view AS SELECT v1, v2 FROM test.t0");
    }

    private static boolean isCopySafe(String sql) throws Exception {
        return AnalyzerUtils.areTablesCopySafe(UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    /**
     * The lock set as production computes it: PlannerMetaLocker is built in StatementPlanner before
     * Analyzer.analyze, so a view is still a TableRelation there. Analyzing first would turn it into a
     * ViewRelation and hide it from the collector entirely.
     */
    private static Set<Long> lockedTableIds(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        Map<Long, Database> dbs = new HashMap<>();
        Map<Long, Set<Long>> tables = new HashMap<>();
        PlannerMetaLocker.collectTablesNeedLock(stmt, connectContext, dbs, tables);
        return tables.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    private static long tableId(String dbName, String tblName) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, tblName).getId();
    }

    @Test
    public void testExternalCatalogTableAbstains() throws Exception {
        // JDBC and Paimon are not in IMMUTABLE_EXTERNAL_TABLES, so before the fix they voted copy-unsafe and
        // made the planner hold the lockable tables' locks for the whole planning phase.
        Assertions.assertTrue(isCopySafe("select * from jdbc0.partitioned_db0.tbl0"));
        Assertions.assertTrue(isCopySafe("select * from paimon0.pmn_db1.unpartitioned_table"));
        Assertions.assertTrue(isCopySafe("select * from hive0.tpch.lineitem"));
    }

    @Test
    public void testMixedInternalAndExternalCatalogIsCopySafe() throws Exception {
        Assertions.assertTrue(isCopySafe(
                "select * from test.t0 join jdbc0.partitioned_db0.tbl0 on true"));
        Assertions.assertTrue(isCopySafe(
                "select * from test.t0, paimon0.pmn_db1.unpartitioned_table"));
        Assertions.assertTrue(isCopySafe(
                "with cte as (select * from test.t0) " +
                        "select * from cte join jdbc0.partitioned_db0.tbl0 on true"));
    }

    /** An external-catalog table must not add itself to the lock set, alone or mixed with a lockable table. */
    @Test
    public void testExternalCatalogTableIsNotInTheLockSet() throws Exception {
        Assertions.assertEquals(Set.of(), lockedTableIds("select * from jdbc0.partitioned_db0.tbl0"));
        Assertions.assertEquals(Set.of(tableId("test", "t0")),
                lockedTableIds("select * from test.t0 join jdbc0.partitioned_db0.tbl0 on true"));
    }

    /**
     * An internal view is neither native nor an FE transaction participant, but AlterJobMgr.alterView rewrites
     * its definition, schema, comment and security in place under this table's WRITE lock, so it must stay in
     * the lock set. It still abstains from voting, so the lock is released after analysis as before.
     */
    @Test
    public void testInternalViewIsLockableAndAbstains() throws Exception {
        Assertions.assertTrue(
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "lock_scope_view")
                        .isMetaLockTarget());
        Assertions.assertEquals(Set.of(tableId("test", "lock_scope_view")),
                lockedTableIds("select * from test.lock_scope_view"));
        Assertions.assertEquals(Set.of(tableId("test", "lock_scope_view"), tableId("test", "t0")),
                lockedTableIds("select * from test.lock_scope_view v join test.t0 on v.v1 = test.t0.v1"));
        // Views never reach the collector -- AstTraverser.visitView descends into the expanded query -- so a
        // statement over one stays copy-safe and the lock is released after analysis.
        Assertions.assertTrue(isCopySafe("select * from test.lock_scope_view"));
    }

    /** ENGINE=MYSQL lives in an internal database, so both its lock and its vote must be unchanged. */
    @Test
    public void testInternalDbExternalEngineTableIsUnchanged() throws Exception {
        Assertions.assertFalse(isCopySafe("select * from test.mysql_ext_tbl"));
        Assertions.assertFalse(isCopySafe(
                "select * from test.t0 join test.mysql_ext_tbl on test.t0.v1 = mysql_ext_tbl.k1"));
        // Mixing it with an external-catalog table must not rescue it either.
        Assertions.assertFalse(isCopySafe(
                "select * from test.mysql_ext_tbl join jdbc0.partitioned_db0.tbl0 on true"));

        Assertions.assertEquals(Set.of(tableId("test", "mysql_ext_tbl")),
                lockedTableIds("select * from test.mysql_ext_tbl"));
    }

    @Test
    public void testNativeTableIsUnchanged() throws Exception {
        Assertions.assertTrue(isCopySafe("select * from test.t0"));
        Assertions.assertTrue(isCopySafe("select * from test.t0 join test.t1 on test.t0.v1 = test.t1.v4"));

        Assertions.assertEquals(Set.of(tableId("test", "t0"), tableId("test", "t1")),
                lockedTableIds("select * from test.t0 join test.t1 on test.t0.v1 = test.t1.v4"));
    }

    /**
     * A resource-mapping table reports a resource_mapping_inside_catalog_* name, yet lives in an internal
     * database and is rewritten in place by HiveTable.modifyTableSchema under lockDatabase(WRITE). This is the
     * case that makes isMetaLockTarget() use !isExternalCatalog rather than isInternalCatalog -- the latter
     * would answer false here and silently drop a load-bearing lock.
     */
    @Test
    public void testResourceMappingTableIsLockable() {
        HiveTable table = HiveTable.builder()
                .setId(1L)
                .setTableName("tbl1")
                .setFullSchema(ImmutableList.of(new Column("k", IntegerType.INT, true)))
                .setResourceName("my_hive_resource")
                .build();
        Assertions.assertTrue(
                com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(
                        table.getCatalogName()));
        Assertions.assertFalse(com.starrocks.server.CatalogMgr.isInternalCatalog(table.getCatalogName()));
        Assertions.assertTrue(table.isMetaLockTarget());
    }

    /** ENGINE=OLAP pointing at a remote cluster still lives in an internal database. */
    @Test
    public void testExternalOlapTableIsLockable() {
        Assertions.assertTrue(new ExternalOlapTable().isMetaLockTarget());
    }

    /**
     * TableType.STARROCKS (enterprise-only) only ever exists inside a `starrocks` external catalog, so it
     * abstains. Asserting on the predicate rather than on a query because StarRocksConnector.bindConfig calls
     * feClient.getCapabilities(), so creating such a catalog in a UT would try to reach a remote FE.
     */
    @Test
    public void testStarRocksExternalTableAbstains() {
        StarRocksExternalTable table = new StarRocksExternalTable(1L, "sr0", "db1", "tbl1",
                ImmutableList.of(new Column("k", IntegerType.INT, true)), 7L, 10086L,
                ImmutableList.of(), 0L, null);
        Assertions.assertFalse(table.isMetaLockTarget());
    }
}
