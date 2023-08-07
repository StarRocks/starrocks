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


package com.starrocks.sql.common;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
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
        Config.alter_scheduler_interval_millisecond = 100;
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
        Database test = GlobalStateMgr.getCurrentState().getDb("test");
        Table tbl1 = test.getTable("tbl1");
        Table tbl2 = test.getTable("tbl2");
        System.out.println(test.getId() + " " + tbl1.getId() + " " + tbl2.getId());
        String sql = "select tbl1.k1, tbl2.k2 from test.tbl1 join test.tbl2 on tbl1.k1 = tbl2.k1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            Assert.assertEquals(encode,
                    String.format("SELECT <db %d>.<table %d>.`k1` AS `k1`, <db %d>.<table %d>.`k2` AS `k2` " +
                            "FROM <db %d>.<table %d> INNER JOIN <db %d>.<table %d> " +
                            "ON <db %d>.<table %d>.`k1` = <db %d>.<table %d>.`k1`",
                            test.getId(), tbl1.getId(), test.getId(), tbl2.getId(),
                            test.getId(), tbl1.getId(), test.getId(), tbl2.getId(),
                            test.getId(), tbl1.getId(), test.getId(), tbl2.getId()));
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    //@Test Undeveloped full test not used, currently meaningless
    public void testDecodeAndEncodeMultiTableHasAlias() {
        String sql = "select t1.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1 = t2.k1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            Assert.assertEquals(encode, "SELECT <db 10002>.`t1`.`k1` AS `k1`, <db 10002>.`t2`.`k2` AS `k2` " +
                    "FROM <db 10002>.<table 10005> AS `t1` " +
                    "INNER JOIN <db 10002>.<table 10021> AS `t2` ON <db 10002>.`t1`.`k1` = <db 10002>.`t2`.`k1`");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    //@Test Undeveloped full test not used, currently meaningless
    public void testDecodeAndEncodeHasAlias() {
        String sql = "select k1, k2 from tbl1 t1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            Assert.assertEquals("SELECT <db 10002>.`t1`.`k1` AS `k1`, <db 10002>.`t1`.`k2` AS `k2` " +
                    "FROM <db 10002>.<table 10005> AS `t1`", encode);
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDecodeAndEncodeNoDataBase() throws Exception {
        String sql = "select tbl1.k1, tbl2.k2 from test.tbl1 join test.tbl2 on tbl1.k1 = tbl2.k1";
        Database test = GlobalStateMgr.getCurrentState().getDb("test");
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            starRocksAssert.dropDatabase("test");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Getting analyzing error. Detail message: Can not find db id: "
                    + test.getId() + ".");
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
        Database test = GlobalStateMgr.getCurrentState().getDb("test");
        Table tbl1 = test.getTable("tbl1");
        Table tbl2 = test.getTable("tbl2");
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            String encode = SqlWithIdUtils.encode(statementBase, connectContext);
            starRocksAssert.dropTable("tbl1");
            SqlWithIdUtils.decode(encode, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Getting analyzing error. Detail message: Can not find table id: "
                    + tbl1.getId() + " in db: test.");
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
