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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DeletePruneTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static DeleteMgr deleteHandler;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        deleteHandler = new DeleteMgr();

        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable("CREATE TABLE `test_delete` (\n" +
                                "  `k1` date NULL COMMENT \"\",\n" +
                                "  `k2` datetime NULL COMMENT \"\",\n" +
                                "  `k3` char(20) NULL COMMENT \"\",\n" +
                                "  `k4` varchar(20) NULL COMMENT \"\",\n" +
                                "  `k5` boolean NULL COMMENT \"\",\n" +
                                "  `k6` tinyint(4) NULL COMMENT \"\",\n" +
                                "  `k7` smallint(6) NULL COMMENT \"\",\n" +
                                "  `k8` int(11) NULL COMMENT \"\",\n" +
                                "  `k9` bigint(20) NULL COMMENT \"\",\n" +
                                "  `k10` largeint(40) NULL COMMENT \"\",\n" +
                                "  `k11` float NULL COMMENT \"\",\n" +
                                "  `k12` double NULL COMMENT \"\",\n" +
                                "  `k13` decimal128(27, 9) NULL COMMENT \"\"\n" +
                                ") ENGINE=OLAP \n" +
                                "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n" +
                                "COMMENT \"OLAP\"\n" +
                                "PARTITION BY RANGE(`k1`) (\n" +
                                "    START (\"2020-01-01\") END (\"2021-01-01\") EVERY (INTERVAL 1 day)\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 \n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"in_memory\" = \"false\"\n" +
                                ");")
                .withTable("CREATE TABLE `test_delete2` (\n" +
                        "  `date` date NULL COMMENT \"\",\n" +
                        "  `id` int(11) NULL COMMENT \"\",\n" +
                        "  `value` char(20) NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`date`, `id`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`date`, `id`)\n" +
                        "(\n" +
                        "    PARTITION `p202001_1000` VALUES LESS THAN (\"2020-02-01\", \"1000\"),\n" +
                        "    PARTITION `p202002_2000` VALUES LESS THAN (\"2020-03-01\", \"2000\"),\n" +
                        "    PARTITION `p202003_all`  VALUES LESS THAN (\"2020-04-01\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`date`, `id`) BUCKETS 3 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE `test_delete3` (" +
                        " `date` date NULL," +
                        " c1 int NULL" +
                        ") PARTITION BY (`date`)" +
                        " PROPERTIES ('replication_num'='1') ")
                .withTable("CREATE TABLE `test_delete4` (" +
                        " `date` date NULL," +
                        " c1 int NULL) " +
                        " PROPERTIES ('replication_num'='1') ");
        UtFrameUtils.mockDML();
        starRocksAssert.getCtx().executeSql("insert into test_delete3 values('2020-01-01', 1), ('2020-01-02', 2)");
        starRocksAssert.getCtx()
                .executeSql("alter table test_delete3 add partition p20200101 values in ('2020-01-01')");
        starRocksAssert.getCtx()
                .executeSql("alter table test_delete3 add partition p20200102 values in ('2020-01-02')");
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testDeletePrune() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Database db = ctx.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable tbl =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_delete");

        String deleteSQL = "delete from test_delete where k1 = '2020-01-01'";
        DeleteStmt deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        List<String> res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p20200101");

        deleteSQL = "delete from test_delete where k1 = '2020-01-01' and k8 = 1";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p20200101");

        deleteSQL = "delete from test_delete where k1 not in ('2020-01-01')";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(366, res.size());

        deleteSQL = "delete from test_delete where k8 = 1";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(366, res.size());

        deleteSQL = "delete from test_delete where k1 in ('2020-01-01')";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p20200101");

        deleteSQL = "delete from test_delete where k1 > '2020-01-01' and k1 < '2020-01-03'";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(2, res.size());
        Assert.assertEquals(res.get(0), "p20200101");
        Assert.assertEquals(res.get(1), "p20200102");

        deleteSQL = "delete from test_delete where k1 > '2020-01-03' and k1 < '2020-01-01'";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(0, res.size());

        deleteSQL = "delete from test_delete where k1 = '2020-01-01' and k1 > '2020-01-03'";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(0, res.size());

        deleteSQL = "delete from test_delete where k1 = '2020-01-03' and k1 > '2020-01-01'";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p20200103");

        deleteSQL = "delete from test_delete where k1 in ('2020-01-03') and k1 > '2020-01-01'";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p20200103");

    }

    @Test
    public void testDeletePruneMultiPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Database db = ctx.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable tbl =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_delete2");

        String deleteSQL = "delete from test_delete2 where date in ('2020-02-02') and id = 1000";
        DeleteStmt deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        List<String> res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p202002_2000");

        deleteSQL = "delete from test_delete2 where date in ('2020-02-02')";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p202002_2000");

        deleteSQL = "delete from test_delete2 where date in ('2020-02-02') and id > 1000";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p202002_2000");

        deleteSQL = "delete from test_delete2 where value = 'a'";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(3, res.size());
    }

    @Test
    public void testDeletePruneListPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        OlapTable tbl = (OlapTable) starRocksAssert.getTable("test", "test_delete3");
        Assert.assertEquals(Sets.newHashSet("p20200101", "p20200102"), tbl.getVisiblePartitionNames());

        // delete one partition
        String deleteSQL = "delete from test_delete3 where date in ('2020-01-01') ";
        DeleteStmt deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        List<String> res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "p20200101");

        // delete two partitions
        deleteSQL = "delete from test_delete3 where date in ('2020-01-01', '2020-01-02') ";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(Lists.newArrayList("p20200101", "p20200102"), res);

        // exceptional
        deleteSQL = "delete from test_delete3 where date in ('2020-01-01') ";
        deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        DeleteStmt exceptionStmt = new DeleteStmt(TableName.fromString("not_exists"), deleteStmt.getPartitionNames(),
                deleteStmt.getWherePredicate());
        exceptionStmt.setDeleteConditions(deleteStmt.getDeleteConditions());
        res = deleteHandler.extractPartitionNamesByCondition(exceptionStmt, tbl);
        Assert.assertEquals(Lists.newArrayList("p20200102", "p20200101"), res);
    }

    @Test
    public void testDeleteUnPartitionTable() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        OlapTable tbl = (OlapTable) starRocksAssert.getTable("test", "test_delete4");
        Assert.assertEquals(Sets.newHashSet("test_delete4"), tbl.getVisiblePartitionNames());

        // delete one partition
        String deleteSQL = "delete from test_delete4 where date in ('2020-01-01') ";
        DeleteStmt deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSQL, ctx);
        List<String> res = deleteHandler.extractPartitionNamesByCondition(deleteStmt, tbl);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(res.get(0), "test_delete4");
    }

}
