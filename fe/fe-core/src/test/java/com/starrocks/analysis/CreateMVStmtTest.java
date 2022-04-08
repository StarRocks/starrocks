// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;


public class CreateMVStmtTest {

    private static String runningDir = "fe/mocked/CreateMVStmtTest/" + UUID.randomUUID().toString() + "/";

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        Deencapsulation.setField(Config.class, "enable_materialized_view", true);
        // create connect context
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        UtFrameUtils.createMinStarRocksCluster(runningDir);
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
                        ");");
    }

    @Test
    public void testCreateMaterializedViewWithStar() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "create materialized view star_view as select * from tbl1;";
        try {
            UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("The materialized view currently does not support * in select statement"));
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
            UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Invalid data type of materialized key column"));
        }
    }

}

