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
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.Type;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.MVColumnItem;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.fail;

public class CreateMVStmtTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        Deencapsulation.setField(Config.class, "enable_materialized_view", true);
        // create connect context
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

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
                .withTable("CREATE TABLE test.agg_tbl\n( " +
                        "    k1 date,\n" +
                        "    v1 int sum,\n" +
                        "    v2 string min,\n" +
                        "    v3 double sum\n" +
                        ")\n" +
                        "AGGREGATE KEY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1 \n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.agg_tbl2\n( " +
                        "    k1 int,\n" +
                        "    k2 int,\n" +
                        "    k3 date,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "AGGREGATE KEY (k1,k2,k3) \n" +
                        "DISTRIBUTED BY HASH(k1,k2,k3) BUCKETS 1 \n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `c_1_0` decimal128(30, 4) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_1` boolean NOT NULL COMMENT \"\",\n" +
                        "  `c_1_2` date NULL COMMENT \"\",\n" +
                        "  `c_1_3` date NOT NULL COMMENT \"\",\n" +
                        "  `c_1_4` double NULL COMMENT \"\",\n" +
                        "  `c_1_5` double NULL COMMENT \"\",\n" +
                        "  `c_1_6` datetime NULL COMMENT \"\",\n" +
                        "  `c_1_7` ARRAY<int(11)> NULL COMMENT \"\",\n" +
                        "  `c_1_8` smallint(6) NULL COMMENT \"\",\n" +
                        "  `c_1_9` bigint(20) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_10` varchar(31) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_11` decimal128(22, 18) NULL COMMENT \"\",\n" +
                        "  `c_1_12` boolean NULL COMMENT \"\",\n" +
                        "  `c_1_13` json NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`c_1_3`)\n" +
                        "(PARTITION p20000101 VALUES [('2000-01-01'), ('2010-12-31')),\n" +
                        "PARTITION p20101231 VALUES [('2010-12-31'), ('2021-12-30')),\n" +
                        "PARTITION p20211230 VALUES [('2021-12-30'), ('2032-12-29')))\n" +
                        "DISTRIBUTED BY HASH(`c_1_3`, `c_1_2`, `c_1_0`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");
    }

    @Test
    public void testCreateMaterializedViewWithStar() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "create materialized view star_view as select * from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception ex) {
            Assert.assertTrue(
                    ex.getMessage().contains("The materialized view currently does not support * in select statement"));
        }
    }

    @Test
    public void testCreateMaterializedViewWithDouble() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "CREATE MATERIALIZED VIEW v0 AS " +
                "SELECT t1.c_1_1, t1.c_1_2, t1.c_1_3, t1.c_1_4, t1.c_1_5, t1.c_1_8, t1.c_1_11, t1.c_1_12, " +
                "SUM(t1.c_1_0) , MIN(t1.c_1_6) , COUNT(t1.c_1_9) , MIN(t1.c_1_10)  " +
                "FROM t1 GROUP BY t1.c_1_1, t1.c_1_2, t1.c_1_3, t1.c_1_4, t1.c_1_5, t1.c_1_8, t1.c_1_11, t1.c_1_12 " +
                "ORDER BY t1.c_1_1, t1.c_1_2 DESC, t1.c_1_3 ASC, t1.c_1_4 ASC, t1.c_1_5 DESC, " +
                "t1.c_1_8 ASC, t1.c_1_11, t1.c_1_12 DESC;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Invalid data type of materialized key column"));
        }
    }

    @Test
    public void testCreateMVWithFirstVarchar() throws Exception {
        String columnName1 = "c_1_10";
        String sql = "create materialized view star_view as select c_1_10 from t1;";
        CreateMaterializedViewStmt stmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertEquals(KeysType.DUP_KEYS, stmt.getMVKeysType());
        List<MVColumnItem> mvColumns = stmt.getMVColumnItemList();
        Assert.assertEquals(1, mvColumns.size());
        MVColumnItem mvColumn0 = mvColumns.get(0);
        Assert.assertTrue(mvColumn0.isKey());
        Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
        Assert.assertEquals(columnName1, mvColumn0.getName());
        Assert.assertEquals(null, mvColumn0.getAggregationType());
    }

    @Test
    public void testCreateMVWithFirstDouble() throws Exception {
        String sql = "create materialized view star_view as select c_1_4 from t1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Data type of first column cannot be DOUBLE"));
        }
    }

    @Test
    public void testCreateMVColumns() throws Exception {
        String sql = "create materialized view star_view as select k1, sum(v1), min(v2) from agg_tbl group by k1;";
        CreateMaterializedViewStmt stmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        Assert.assertEquals(KeysType.AGG_KEYS, stmt.getMVKeysType());
        List<MVColumnItem> mvColumns = stmt.getMVColumnItemList();
        Assert.assertEquals(3, mvColumns.size());

        MVColumnItem mvColumn0 = mvColumns.get(0);
        Assert.assertTrue(mvColumn0.isKey());
        Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
        Assert.assertEquals(null, mvColumn0.getAggregationType());

        MVColumnItem mvColumn1 = mvColumns.get(1);
        Assert.assertFalse(mvColumn1.isKey());
        Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
        Assert.assertEquals(AggregateType.SUM, mvColumn1.getAggregationType());

        MVColumnItem mvColumn2 = mvColumns.get(2);
        Assert.assertFalse(mvColumn2.isKey());
        Assert.assertFalse(mvColumn2.isAggregationTypeImplicit());
        Assert.assertEquals(AggregateType.MIN, mvColumn2.getAggregationType());
        Assert.assertEquals(KeysType.AGG_KEYS, stmt.getMVKeysType());
    }

    @Test
    public void testDuplicateMV() throws Exception {
        String sql = "create materialized view star_view as select k1, sum(v1) from agg_tbl group by k1;";
        CreateMaterializedViewStmt stmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        Assert.assertEquals(KeysType.AGG_KEYS, stmt.getMVKeysType());
        List<MVColumnItem> mvSchema = stmt.getMVColumnItemList();
        Assert.assertEquals(2, mvSchema.size());
        Assert.assertTrue(mvSchema.get(0).isKey());
    }

    @Test
    public void testMVAggregate() throws Exception {
        {
            String sql = "create materialized view star_view as select k1, sum(v1) from agg_tbl group by k1;";
            CreateMaterializedViewStmt stmt =
                    (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            Assert.assertEquals(Type.BIGINT, stmt.getMVColumnItemList().get(1).getType());
        }

        {
            String sql = "create materialized view star_view as select k1, min(v2) from agg_tbl group by k1;";
            CreateMaterializedViewStmt stmt =
                    (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            Assert.assertTrue(stmt.getMVColumnItemList().get(1).getType().isVarchar());
        }
        {
            String sql = "create materialized view star_view as select k1, sum(v3) from agg_tbl group by k1;";
            CreateMaterializedViewStmt stmt =
                    (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            Assert.assertEquals(Type.DOUBLE, stmt.getMVColumnItemList().get(1).getType());
        }
    }

    @Test
    public void testCountDistinct() {
        {
            String sql = "CREATE MATERIALIZED VIEW v0 AS " +
                    "SELECT count(distinct t1.c_1_9) " +
                    "FROM t1";
            try {
                UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
                fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage().contains("Materialized view does not support " +
                        "distinct function count(DISTINCT `test`.`t1`.`c_1_9`)"));
            }
        }
        {
            String sql = "CREATE MATERIALIZED VIEW v0 AS " +
                    "SELECT sum(distinct t1.c_1_9) " +
                    "FROM t1";
            try {
                UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
                fail();
            } catch (Exception ex) {

                Assert.assertTrue(ex.getMessage().contains("Materialized view does not support distinct " +
                        "function sum(DISTINCT `test`.`t1`.`c_1_9`)"));
            }
        }
    }

    @Test
    public void testMVColumnsWithoutOrderbyWithoutAggregation() throws Exception {
        String columnName1 = "c_1_1";
        String columnName2 = "c_1_2";
        String columnName3 = "c_1_3";
        String columnName4 = "c_1_4";
        String sql = "create materialized view star_view as select c_1_1, c_1_2, c_1_3, c_1_4 from t1;";

        CreateMaterializedViewStmt stmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertEquals(KeysType.DUP_KEYS, stmt.getMVKeysType());
        List<MVColumnItem> mvColumns = stmt.getMVColumnItemList();
        Assert.assertEquals(4, mvColumns.size());

        MVColumnItem mvColumn0 = mvColumns.get(0);
        Assert.assertTrue(mvColumn0.isKey());
        Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
        Assert.assertEquals(columnName1, mvColumn0.getName());
        Assert.assertNull(mvColumn0.getAggregationType());

        MVColumnItem mvColumn1 = mvColumns.get(1);
        Assert.assertTrue(mvColumn1.isKey());
        Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
        Assert.assertEquals(columnName2, mvColumn1.getName());
        Assert.assertNull(mvColumn1.getAggregationType());

        MVColumnItem mvColumn2 = mvColumns.get(2);
        Assert.assertTrue(mvColumn2.isKey());
        Assert.assertFalse(mvColumn2.isAggregationTypeImplicit());
        Assert.assertEquals(columnName3, mvColumn2.getName());
        Assert.assertNull(mvColumn2.getAggregationType());

        MVColumnItem mvColumn3 = mvColumns.get(3);
        Assert.assertFalse(mvColumn3.isKey());
        Assert.assertTrue(mvColumn3.isAggregationTypeImplicit());
        Assert.assertEquals(columnName4, mvColumn3.getName());
        Assert.assertEquals(AggregateType.NONE, mvColumn3.getAggregationType());
    }

    @Test
    public void testMVColumnsWithoutOrderbyWithoutAggregationWithFloat() throws Exception {
        String columnName4 = "c_1_4";
        String sql = "create materialized view star_view as select c_1_1, c_1_2, c_1_3, c_1_4 from t1;";
        CreateMaterializedViewStmt stmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertEquals(KeysType.DUP_KEYS, stmt.getMVKeysType());
        List<MVColumnItem> mvColumns = stmt.getMVColumnItemList();
        Assert.assertEquals(4, mvColumns.size());

        MVColumnItem mvColumn3 = mvColumns.get(3);
        Assert.assertFalse(mvColumn3.isKey());
        Assert.assertTrue(mvColumn3.isAggregationTypeImplicit());
        Assert.assertEquals(columnName4, mvColumn3.getName());
        Assert.assertEquals(AggregateType.NONE, mvColumn3.getAggregationType());
    }

    @Test
    public void testMVColumnsWithoutOrderbyWithoutAggregationWithVarchar() throws Exception {
        String columnName3 = "c_1_10";
        String sql = "create materialized view star_view as select c_1_1, c_1_2, c_1_10, c_1_11  from t1;";
        CreateMaterializedViewStmt stmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertEquals(KeysType.DUP_KEYS, stmt.getMVKeysType());
        List<MVColumnItem> mvColumns = stmt.getMVColumnItemList();
        Assert.assertEquals(4, mvColumns.size());

        MVColumnItem mvColumn3 = mvColumns.get(2);
        Assert.assertTrue(mvColumn3.isKey());
        Assert.assertEquals(columnName3, mvColumn3.getName());
        Assert.assertNull(mvColumn3.getAggregationType());

        MVColumnItem mvColumn4 = mvColumns.get(3);
        Assert.assertFalse(mvColumn4.isKey());
    }

    @Test
    public void testCreateMVWithJSON() {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "CREATE MATERIALIZED VIEW v0 AS " +
                "SELECT t1.c_1_1, t1.c_1_2, t1.c_1_13 " +
                "FROM t1";
        try {
            CreateMaterializedViewStmt stmt = (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            boolean jsonKey = stmt.getMVColumnItemList().stream().anyMatch(col -> col.isKey() && col.getType().isJsonType());
            Assert.assertFalse(jsonKey);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Invalid data type of materialized key column"));
        }
    }

    @Test
    public void testCreateMVWithOnlyOneAggFunc() {
        ConnectContext ctx = starRocksAssert.getCtx();
        List<String> invalidSqls = Lists.newArrayList();
        invalidSqls.add("create materialized view mv_01 as select sum(c_1_4) from t1");
        invalidSqls.add("create materialized view mv_01 as select sum(c_1_4) from t1 group by c_1_1");
        for (String sql : invalidSqls) {
            try {
                UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                fail("wrong sql, should fail");
            } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage()
                        .contains("Please add group by clause and at least one group by column in the select list"));
            }
        }

        List<String> validSqls = Lists.newArrayList();
        validSqls.add("create materialized view mv_01 as select c_1_1, sum(c_1_4) from t1 group by c_1_1");
        validSqls.add("create materialized view mv_01 as select c_1_1, sum(c_1_4) from t1 group by c_1_1, c_1_2");
        for (String sql : validSqls) {
            try {
                UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            } catch (Exception ex) {
                fail("valid sql, should success");
                System.out.println(ex.getMessage());
            }
        }
    }

    @Test
    public void testCreateMVWithColBeforeAgg() {
        ConnectContext ctx = starRocksAssert.getCtx();
        List<String> invalidSqls = Lists.newArrayList();
        invalidSqls.add("create materialized view mv_01 as select sum(c_1_4), c_1_1 from t1 group by c_1_1");
        invalidSqls.add("create materialized view mv_01 as select c_1_2, sum(c_1_4), c_1_1 from t1 group by c_1_1, c_1_2");
        for (String sql : invalidSqls) {
            try {
                UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                fail("wrong sql, should fail");
            } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage()
                        .contains("Any single column should be before agg column"));
            }
        }
    }

    @Test
    public void testCreateMVWithAggFunctionAndOtherExprs() {
        ConnectContext ctx = starRocksAssert.getCtx();
        List<String> invalidSqls = Lists.newArrayList();
        invalidSqls.add("create materialized view mv_01 as select c_1_2, cast(sum(c_1_4) as string) from t1 group by " +
                "c_1_2");
        for (String sql : invalidSqls) {
            try {
                UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                fail("wrong sql, should fail");
            } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage()
                        .contains("Aggregate function with function expr is not supported yet"));
            }
        }
    }
}


