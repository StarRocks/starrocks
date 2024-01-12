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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class ShowCreateMaterializedViewStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(ctx);
        // NOTE: no set it because we need to check the defined sql.
        Config.default_mv_refresh_immediate = true;

        ConnectorPlanTestBase.mockHiveCatalog(ctx);
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`, `k2`)\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .useDatabase("test");
    }

    @Test
    public void testShowInternalCatalogConstraints() throws Exception {
        String createMvSql = "create materialized view mv9 " +
                "distributed by hash(k1) buckets 10 " +
                "refresh manual " +
                "properties(\"unique_constraints\" = \"tbl2.k1\", " +
                "\"foreign_key_constraints\" = \"tbl1(k1) REFERENCES tbl2(k1)\") " +
                "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k1 = tbl2.k1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv9");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals("CREATE MATERIALIZED VIEW `mv9` (`k1`, `k2`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"unique_constraints\" = \"default_catalog.test.tbl2.k1\",\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"foreign_key_constraints\" = \"default_catalog.test.tbl1(k1) REFERENCES default_catalog.test.tbl2(k1)\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `tbl1`.`k1`, `tbl2`.`k2`\n" +
                "FROM `test`.`tbl1` INNER JOIN `test`.`tbl2` ON `tbl1`.`k1` = `tbl2`.`k1`;", createTableStmt.get(0));
    }

    @Test
    public void testShowExternalCatalogConstraints() throws Exception {
        String createMvSql = "create materialized view mv10 " +
                "distributed by hash(c1) buckets 10 " +
                "refresh manual " +
                "properties(\"unique_constraints\" = \"hive0.partitioned_db2.t2.c2\", " +
                "\"foreign_key_constraints\" = \"hive0.partitioned_db.t1(c2) REFERENCES hive0.partitioned_db2.t2(c2)\") " +
                "as select t2.c1, t1.c2 from hive0.partitioned_db.t1 join hive0.partitioned_db2.t2 on t1.c2 = t2.c2;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv10");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals("CREATE MATERIALIZED VIEW `mv10` (`c1`, `c2`)\n" +
                        "DISTRIBUTED BY HASH(`c1`) BUCKETS 10 \n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"unique_constraints\" = \"hive0.partitioned_db2.t2.c2\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"foreign_key_constraints\" = \"hive0.partitioned_db.t1(c2) REFERENCES hive0.partitioned_db2.t2(c2)\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `hive0`.`partitioned_db2`.`t2`.`c1`, `hive0`.`partitioned_db`.`t1`.`c2`\n" +
                        "FROM `hive0`.`partitioned_db`.`t1` INNER JOIN `hive0`.`partitioned_db2`.`t2` ON `hive0`.`partitioned_db`.`t1`.`c2` = `hive0`.`partitioned_db2`.`t2`.`c2`;",
                createTableStmt.get(0));
    }

    @Test
    public void testShowExternalTableCreateMvSql() throws Exception {
        MetadataMgr oldMetadataMgr = ctx.getGlobalStateMgr().getMetadataMgr();

        String createMvSql = "create materialized view mv8 " +
                "distributed by hash(l_orderkey) buckets 10 " +
                "refresh manual " +
                "as select l_orderkey,l_partkey,l_shipdate from hive0.tpch.lineitem;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv8");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0),
                "CREATE MATERIALIZED VIEW `mv8` (`l_orderkey`, `l_partkey`, `l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10 \n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `lineitem`.`l_orderkey`, `lineitem`.`l_partkey`, `lineitem`.`l_shipdate`\n" +
                        "FROM `hive0`.`tpch`.`lineitem`;");
        ctx.getGlobalStateMgr().setMetadataMgr(oldMetadataMgr);
    }

    @ParameterizedTest
    @MethodSource("genTestArguments")
    public void testEntrance(String refreshClause,
                             String orderBy,
                             String property,
                             String partitionBy,
                             String distribute,
                             String select) throws Exception {
        final String mvName = "test_mv_show_create";
        starRocksAssert.ddl("drop materialized view if exists " + mvName);
        String createSql = String.format("CREATE MATERIALIZED VIEW %s " +
                        " %s -- distribute \n" +
                        " %s -- partition by\n" +
                        " %s -- refresh \n" + " %s -- order by \n" +
                        " PROPERTIES( %s ) -- properties\n" +
                        " AS %s",
                mvName, distribute, partitionBy, refreshClause, orderBy, property, select);

        // colocate is conflicted with random distribution
        if (property.contains("colocate_with") && distribute.equalsIgnoreCase("")) {
            Assertions.assertThrows(SemanticException.class, () -> starRocksAssert.withMaterializedView(createSql));
            return;
        }
        starRocksAssert.withMaterializedView(createSql);

        MaterializedView mv = starRocksAssert.getMv(starRocksAssert.getCtx().getDatabase(), mvName);
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(mv, createTableStmt, null, null, false, true);

        Assertions.assertTrue(createTableStmt.get(0).contains(refreshClause), createTableStmt.get(0));
        Assertions.assertTrue(createTableStmt.get(0).contains(orderBy), createTableStmt.get(0));
        Assertions.assertTrue(createTableStmt.get(0).contains(property), createTableStmt.get(0));
        Assertions.assertTrue(createTableStmt.get(0).contains(partitionBy), createTableStmt.get(0));
        Assertions.assertTrue(createTableStmt.get(0).contains(distribute), createTableStmt.get(0));
    }

    public static Stream<Arguments> genTestArguments() {
        List<String> refreshArgumentsList = Lists.newArrayList(
                "REFRESH MANUAL",
                "REFRESH DEFERRED MANUAL",
                "REFRESH ASYNC",
                "REFRESH ASYNC EVERY(INTERVAL 1 HOUR)",
                "REFRESH ASYNC START(\"1998-01-01 00:00:00\") EVERY(INTERVAL 1 HOUR)"
        );
        List<String> orderByList = Lists.newArrayList("", "ORDER BY (k3)");
        List<String> propertiesList = Lists.newArrayList(
                "\"colocate_with\" = \"g1\"",
                "\"replicated_storage\" = \"true\"",
                "\"mv_rewrite_staleness_second\" = \"60\""
        );
        List<String> partitionList = Lists.newArrayList("",
                "PARTITION BY (`k3`)",
                "PARTITION BY (date_trunc('month', `k3`))");
        List<String> distributeList = Lists.newArrayList(
                "",
                "DISTRIBUTED BY HASH(`k3`)",
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10");
        List<String> selectList = Lists.newArrayList(
                "select k1 as k3, k2 from tbl1",
                "SELECT `tbl1`.`k1` AS `k3`, `tbl1`.`k2`\nFROM `test`.`tbl1`",
                "SELECT /*+SET_VAR(query_timeout='1')*/ `tbl1`.`k1` AS `k3`, `tbl1`.`k2`\nFROM `test`.`tbl1`"
        );

        // basic combinations
        StarRocksAssert.TestCaseEnumerator enumerator = new StarRocksAssert.TestCaseEnumerator(Lists.newArrayList(
                refreshArgumentsList.size(),
                orderByList.size(),
                propertiesList.size(),
                partitionList.size(),
                distributeList.size(),
                selectList.size()
        ));
        return enumerator.enumerate().map(permutation ->
                StarRocksAssert.TestCaseEnumerator.ofArguments(permutation,
                        refreshArgumentsList, orderByList, propertiesList,
                        partitionList, distributeList, selectList));
    }

}