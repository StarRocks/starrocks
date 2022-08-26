// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.common;

import com.starrocks.analysis.StatementBase;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlWithIdUtilsTest {
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
                        "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void testDecodeAndEncode() {
        String sql = "select tbl1.k1, tbl2.k2 from test.tbl1 join test.tbl2 on tbl1.k1 = tbl2.k1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            Assert.assertEquals(encode,
                    "SELECT <db 10002>.<table 10005>.`k1` AS `k1`, <db 10002>.<table 10021>.`k2` AS `k2` " +
                            "FROM <db 10002>.<table 10005> INNER JOIN <db 10002>.<table 10021> " +
                            "ON <db 10002>.<table 10005>.`k1` = <db 10002>.<table 10021>.`k1`");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDecodeAndEncodeMultiTableHasAlias() {
        String sql = "select t1.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1 = t2.k1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            Assert.assertEquals(encode, "SELECT `t1`.`k1` AS `k1`, `t2`.`k2` AS `k2` " +
                    "FROM <db 10002>.<table 10005> AS `t1` " +
                    "INNER JOIN <db 10002>.<table 10021> AS `t2` " +
                    "ON `t1`.`k1` = `t2`.`k1`");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDecodeAndEncodeHasAlias() {
        String sql = "select k1, k2 from tbl1 t1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            Assert.assertEquals(encode,
                    "SELECT `t1`.`k1` AS `k1`, `t1`.`k2` AS `k2` FROM <db 10002>.<table 10005> AS `t1`");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDecodeAndEncodeNoDataBase() throws Exception {
        String sql = "select tbl1.k1, tbl2.k2 from test.tbl1 join test.tbl2 on tbl1.k1 = tbl2.k1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            starRocksAssert.dropDatabase("test");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can not find db id: 10002");
        } finally {
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
                            "    v1 int sum\n" +
                            ")\n" +
                            "PARTITION BY RANGE(k2)\n" +
                            "(\n" +
                            "    PARTITION p1 values less than('10'),\n" +
                            "    PARTITION p2 values less than('20')\n" +
                            ")\n" +
                            "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                            "PROPERTIES('replication_num' = '1');");
        }
    }

    @Test
    public void testDecodeAndEncodeNoTable() throws Exception {
        String sql = "select tbl1.k1, tbl2.k2 from test.tbl1 join test.tbl2 on tbl1.k1 = tbl2.k1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            starRocksAssert.dropTable("tbl1");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can not find table id: 10005 in db: test");
        } finally {
            starRocksAssert.useDatabase("test")
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
                            "PROPERTIES('replication_num' = '1');");
        }
    }
}
