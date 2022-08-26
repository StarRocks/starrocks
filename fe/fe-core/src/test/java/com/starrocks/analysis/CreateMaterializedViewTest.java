// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.RefreshType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CreateMaterializedViewTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
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
                .withTable("CREATE TABLE `aggregate_table_with_null` (\n" +
                        "                        `k1` date,\n" +
                        "                        `v2` datetime MAX,\n" +
                        "                        `v3` char(20) MIN,\n" +
                        "                        `v4` tinyint SUM,\n" +
                        "                        `v8` tinyint SUM,\n" +
                        "                        `v5` HLL HLL_UNION,\n" +
                        "                        `v6` BITMAP BITMAP_UNION,\n" +
                        "                        `v7` PERCENTILE PERCENTILE_UNION\n" +
                        "                    ) ENGINE=OLAP\n" +
                        "                    AGGREGATE KEY(`k1`)\n" +
                        "                    COMMENT \"OLAP\"\n" +
                        "                    DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "                    PROPERTIES (\n" +
                        "                        \"replication_num\" = \"1\",\n" +
                        "                        \"storage_format\" = \"v2\"\n" +
                        "                    );")
                .withView("CREATE VIEW v1 AS SELECT * FROM aggregate_table_with_null;")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('10'),\n" +
                        "    PARTITION p2 values less than('20')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl4\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    k3 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2,k3)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('20','30'),\n" +
                        "    PARTITION p2 values less than('40','50')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
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
                        "  `c_1_12` boolean NULL COMMENT \"\"\n" +
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
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE EXTERNAL TABLE mysql_external_table\n" +
                        "(\n" +
                        "    k1 DATE,\n" +
                        "    k2 INT,\n" +
                        "    k3 SMALLINT,\n" +
                        "    k4 VARCHAR(2048),\n" +
                        "    k5 DATETIME\n" +
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
                .withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE test2.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .useDatabase("test");
    }

    @Test
    public void testDisabled() {
        Config.enable_materialized_view = false;
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "The experimental mv is disabled");
        } finally {
            Config.enable_materialized_view = true;
        }
    }

    @Test
    public void testExists() {
        String sql = "create materialized view tbl1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Table 'tbl1' already exists");
        }
    }

    @Test
    public void testIfNotExists() {
        String sql = "create materialized view if not exists tbl1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionNoDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "No database selected");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testFullDescPartitionHasDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view test.mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionNoNeed() {
        String sql = "create materialized view mv1 " +
                "partition by (a+b) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Partition exp not supports:a + b");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testCreateMVWithExplainQuery() {
        String sql = "create materialized view mv1 " +
                "as explain select k1, v2 from aggregate_table_with_null;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Materialized view does not support explain query", e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionIn() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            assertTrue(partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            assertTrue(partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            assertTrue(partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithoutFunction() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            assertTrue(!partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof SlotRef);
            assertEquals(partitionExpDesc.getExpr(), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',date_trunc('month',ss)) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Partition exp not supports:date_trunc('month', date_trunc('month', ss))");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunctionInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view partition function date_trunc must related with column");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionColumnNoBaseTablePartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view partition column in partition exp must be base table partition column");
        }
    }

    @Test
    public void testPartitionColumnBaseTableHasMultiPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl4;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view related base table partition columns only supports single column");
        }
    }

    @Test
    public void testBaseTableNoPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl3;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view partition column in partition exp must be base table partition column");
        }
    }

    @Test
    public void testFullDescPartitionByColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnMixAlias1() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnMixAlias2() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoPartitionExp() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testSelectHasStar() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(ss) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 ss, *  from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Select * is not supported in materialized view");
        }
    }

    @Test
    public void testPartitionByFunctionNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by s8 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select sqrt(tbl1.k1) s1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view partition exp column is not found in query statement");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc(tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "No matching function with signature: date_trunc(date).");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('%y%m',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "date_trunc function can't support argument other than year|quarter|month|week|day");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view partition function date_trunc check failed");
        }
    }

    @Test
    public void testPartitionByNoAllowedFunction() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select sqrt(tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view partition function sqrt is not support");
        }
    }

    @Test
    public void testPartitionByNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Partition exp date_trunc('month', k1) must be alias of select item");
        }
    }

    @Test
    public void testDistributeByIsNull1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view should contain distribution desc");
        }
    }

    @Test
    public void testDistributeByIsNull2() {
        connectContext.getSessionVariable().setAllowDefaultPartition(true);
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            connectContext.getSessionVariable().setAllowDefaultPartition(false);
        }
    }

    @Test
    public void testRefreshAsyncOnlyEvery() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async EVERY(INTERVAL 2 MINUTE)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            assertEquals(refreshSchemeDesc.getType(), RefreshType.ASYNC);
            assertNotNull(asyncRefreshSchemeDesc.getStartTime());
            assertEquals(((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue(), 2);
            assertEquals(asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription(), "MINUTE");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshAsyncStartBeforeCurr() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2016-12-31') EVERY(INTERVAL 1 HOUR)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Refresh start must be after current time");
        }
    }

    @Test
    public void testRefreshManual() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh manual " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            assertEquals(refreshSchemeDesc.getType(), RefreshType.MANUAL);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshSync() throws Exception {
        String sql = "create materialized view mv1 " +
                "refresh sync " +
                "as select k2 from tbl1 group by k2;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testRefreshSyncHasPartition() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh sync " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Partition by is not supported by SYNC refresh type int materialized view");
        }
    }

    @Test
    public void testRefreshSyncHasDistribution() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) " +
                "refresh sync " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Distribution by is not supported by SYNC refresh type in materialized view");
        }
    }

    @Test
    public void testNoRefresh() {
        String sql = "create materialized view mv1 " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k1, k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoRefreshNoSelectStmt() {
        String sql = "create materialized view mv1 " +
                "as select t1.k1 ss, t1.k2 from tbl1 t1 union select k1, k2 from tbl1 group by tbl1.k1, tbl1.k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    @Test
    public void testQueryStatementNoSelectRelation() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select t1.k1 ss, t1.k2 from tbl1 t1 union select * from tbl2 t2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    @Test
    public void testTableNotInOneDatabase() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select t1.k1 ss, t1.k2 from test2.tbl3 t1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view do not support table which is in other database:default_cluster:test2");
        }
    }

    @Test
    public void testTableNoOlapTable() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view only support olap table:mysql_external_table type:MYSQL");
        }

    }

    @Test
    public void testSelectItemNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item date_trunc('month', `tbl1`.`k1`) must has an alias");
        }
    }

    @Test
    public void testSelectItemHasNonDeterministicFunction1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select rand() s1, date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item rand() not supported nondeterministic function");
        }
    }

    @Test
    public void testSelectItemHasNonDeterministicFunction2() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k2, rand()+rand() s1, date_trunc('month',tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item rand() not supported nondeterministic function");
        }
    }

    @Test
    public void testCreateMVWithHll() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW mv_function\n" +
                "                             AS SELECT\n" +
                "                               k1,\n" +
                "                               MAX(v2),\n" +
                "                               MIN(v3),\n" +
                "                               SUM(v4),\n" +
                "                               HLL_UNION(v5),\n" +
                "                               BITMAP_UNION(v6),\n" +
                "                               PERCENTILE_UNION(v7)\n" +
                "                             FROM test.aggregate_table_with_null GROUP BY k1\n" +
                "                             ORDER BY k1 DESC";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateMVBaseOnView() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW mv1\n" +
                "                                                  AS\n" +
                "                                                  SELECT\n" +
                "                                                    k1,\n" +
                "                                                    v2\n" +
                "                                                  FROM test.v1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Do not support alter non-OLAP table[v1]");
        }
    }
}

