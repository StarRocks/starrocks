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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/TempPartitionTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TempPartitionTest {

    private static String tempPartitionFile = "./TempPartitionTest";
    private static String tblFile = "./tblFile";

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setup() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        Config.alter_scheduler_interval_millisecond = 100;
        starRocksAssert = new StarRocksAssert(ctx);
    }

    @AfterClass
    public static void tearDown() {
        File file2 = new File(tempPartitionFile);
        file2.delete();
        File file3 = new File(tblFile);
        file3.delete();
    }

    @Before
    public void before() {

    }

    private List<List<String>> checkShowPartitionsResultNum(String tbl, boolean isTemp, int expected) throws Exception {
        String showStr = "show " + (isTemp ? "temporary" : "") + " partitions from " + tbl;
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) UtFrameUtils.parseStmtWithNewParser(showStr, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        Assert.assertEquals(expected, rows.size());
        return rows;
    }

    private void alterTableWithNewAnalyzer(String sql, boolean expectedException) throws Exception {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
            if (expectedException) {
                Assert.fail("expected exception not thrown");
            }
        } catch (Exception e) {
            if (expectedException) {
                System.out.println("got exception: " + e.getMessage());
            } else {
                throw e;
            }
        }
    }

    private List<List<String>> checkTablet(String tbl, String partitions, boolean isTemp, int expected)
            throws Exception {
        String showStr = "show tablet from " + tbl + (isTemp ? " temporary" : "") + " partition (" + partitions + ");";
        ShowTabletStmt showStmt = (ShowTabletStmt) UtFrameUtils.parseStmtWithNewParser(showStr, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        if (expected != -1) {
            Assert.assertEquals(expected, rows.size());
        }
        return rows;
    }

    private long getPartitionIdByTabletId(long tabletId) {
        TabletInvertedIndex index = GlobalStateMgr.getCurrentInvertedIndex();
        TabletMeta tabletMeta = index.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            return -1;
        }
        return tabletMeta.getPartitionId();
    }

    private void getPartitionNameToTabletIdMap(String tbl, boolean isTemp, Map<String, Long> partNameToTabletId)
            throws Exception {
        partNameToTabletId.clear();
        String showStr = "show " + (isTemp ? "temporary" : "") + " partitions from " + tbl;
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) UtFrameUtils.parseStmtWithNewParser(showStr, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        Map<Long, String> partIdToName = Maps.newHashMap();
        for (List<String> row : rows) {
            partIdToName.put(Long.valueOf(row.get(0)), row.get(1));
        }

        rows = checkTablet(tbl, Joiner.on(",").join(partIdToName.values()), isTemp, -1);
        for (List<String> row : rows) {
            long tabletId = Long.valueOf(row.get(0));
            long partitionId = getPartitionIdByTabletId(tabletId);
            String partName = partIdToName.get(partitionId);
            partNameToTabletId.put(partName, tabletId);
        }
    }

    private void checkTabletExists(Collection<Long> tabletIds, boolean checkExist) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Long tabletId : tabletIds) {
            if (checkExist) {
                Assert.assertNotNull(invertedIndex.getTabletMeta(tabletId));
            } else {
                Assert.assertNull(invertedIndex.getTabletMeta(tabletId));
            }
        }
    }

    private void checkPartitionExist(OlapTable tbl, String partName, boolean isTemp, boolean checkExist) {
        if (checkExist) {
            Assert.assertNotNull(tbl.getPartition(partName, isTemp));
        } else {
            Assert.assertNull(tbl.getPartition(partName, isTemp));
        }
    }

    @Test
    public void testForSinglePartitionTable() throws Exception {
        // create database db1
        starRocksAssert.withDatabase("db1").useDatabase("db1")
                .withTable(
                        "create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // add temp partition
        String stmtStr = "alter table db1.tbl1 add temporary partition p1 values less than ('10');";
        alterTableWithNewAnalyzer(stmtStr, true);

        // drop temp partition
        stmtStr = "alter table db1.tbl1 drop temporary partition tbl1;";
        alterTableWithNewAnalyzer(stmtStr, true);

        // show temp partition
        checkShowPartitionsResultNum("db1.tbl1", true, 0);
    }

    @Test
    public void testForSingleListPartitionTable() throws Exception {
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test(\n" +
                        "    id bigint not null ,\n" +
                        "    province varchar(20) not null,\n" +
                        "    dt varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (province) (\n" +
                        "   PARTITION p_fj VALUES IN (\"fuzhou\", \"xiamen\"),\n" +
                        "   PARTITION p_gd VALUES IN (\"shenzhen\", \"guangzhou\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");

        String addTempStmtStr1 = "alter table test.test add temporary partition tp_zj " +
                "VALUES IN (\"hangzhou\", \"ningbo\");";
        alterTableWithNewAnalyzer(addTempStmtStr1, false);
        String failReplaceStrict = "alter table test.test replace partition(p_fj, p_gd) " +
                "with temporary partition(tp_zj) properties('strict_range' = 'true', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(failReplaceStrict, true);
        String dropTempStr = "alter table test.test drop temporary partition tp_zj;";
        alterTableWithNewAnalyzer(dropTempStr, false);

        String addTempStmtStr2 = "alter table test.test add temporary partition tp_fj_gd " +
                "VALUES IN (\"fuzhou\", \"xiamen\", \"shenzhen\", \"guangzhou\");";
        alterTableWithNewAnalyzer(addTempStmtStr2, false);
        String passReplaceStrict = "alter table test.test replace partition(p_fj, p_gd) " +
                "with temporary partition(tp_fj_gd) properties('strict_range' = 'true', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(passReplaceStrict, false);

        String addStmtStr1 = "alter table test.test add partition p_zj " +
                "VALUES IN (\"hangzhou\", \"ningbo\");";
        alterTableWithNewAnalyzer(addStmtStr1, false);
        String addStmtStr2 = "alter table test.test add partition p_js " +
                "VALUES IN (\"nanjing\", \"suzhou\");";
        alterTableWithNewAnalyzer(addStmtStr2, false);

        String addTempStmtStr3 = "alter table test.test add temporary partition tp_bj_zj " +
                "VALUES IN (\"beijing\", \"hangzhou\");";
        alterTableWithNewAnalyzer(addTempStmtStr3, false);
        String failReplaceNotStrict1 = "alter table test.test replace partition(p_js) " +
                "with temporary partition(tp_bj_zj) properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(failReplaceNotStrict1, true);
        dropTempStr = "alter table test.test drop temporary partition tp_bj_zj;";
        alterTableWithNewAnalyzer(dropTempStr, false);

        String addTempStmtStr4 = "alter table test.test add temporary partition tp_bj_js " +
                "VALUES IN (\"beijing\", \"nanjing\");";
        alterTableWithNewAnalyzer(addTempStmtStr4, false);
        String failReplaceNotStrict2 = "alter table test.test replace partition(p_zj) " +
                "with temporary partition(tp_bj_js) properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(failReplaceNotStrict2, true);
        dropTempStr = "alter table test.test drop temporary partition tp_bj_js;";
        alterTableWithNewAnalyzer(dropTempStr, false);

        String addTempStmtStr5 = "alter table test.test add temporary partition tp_zj " +
                "VALUES IN (\"hangzhou\", \"ningbo\");";
        alterTableWithNewAnalyzer(addTempStmtStr5, false);
        String passReplaceNotStrict1 = "alter table test.test replace partition(p_zj, p_js) " +
                "with temporary partition(tp_zj) properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(passReplaceNotStrict1, false);

        String addStmtStr3 = "alter table test.test add partition p_sc " +
                "VALUES IN (\"chengdu\", \"chongqing\");";
        alterTableWithNewAnalyzer(addStmtStr3, false);
        String addStmtStr4 = "alter table test.test add partition p_hb " +
                "VALUES IN (\"wuhang\", \"sanxia\");";
        alterTableWithNewAnalyzer(addStmtStr4, false);
        String addTempStmtStr6 = "alter table test.test add temporary partition tp_sc " +
                "VALUES IN (\"chengdu\", \"chongqing\");";
        alterTableWithNewAnalyzer(addTempStmtStr6, false);
        String passReplaceNotStrict2 = "alter table test.test replace partition(p_sc, p_hb) " +
                "with temporary partition(tp_sc) properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(passReplaceNotStrict2, false);
    }

    @Test
    public void testForMultiListPartitionTable() throws Exception {
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test(\n" +
                        "    id bigint not null ,\n" +
                        "    province varchar(20) not null,\n" +
                        "    dt varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (dt, province) (\n" +
                        "   PARTITION p_20230401_bj VALUES IN ((\"2023-04-01\", \"beijing\")),\n" +
                        "   PARTITION p_20230401_sh VALUES IN ((\"2023-04-01\", \"shanghai\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");

        String addTempStmtStr1 = "alter table test.test add temporary partition tp_20230401_bj " +
                "VALUES IN ((\"2023-04-01\", \"beijing\"));";
        alterTableWithNewAnalyzer(addTempStmtStr1, false);
        String failReplaceStrict = "alter table test.test replace partition(p_20230401_bj, p_20230401_sh) " +
                "with temporary partition(tp_20230401_bj) " +
                "properties('strict_range' = 'true', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(failReplaceStrict, true);
        String dropTempStr = "alter table test.test drop temporary partition tp_20230401_bj;";
        alterTableWithNewAnalyzer(dropTempStr, false);

        String addTempStmtStr2 = "alter table test.test add temporary partition tp_20230401_bj_sh " +
                "VALUES IN ((\"2023-04-01\", \"beijing\"), (\"2023-04-01\", \"shanghai\"));";
        alterTableWithNewAnalyzer(addTempStmtStr2, false);
        String passReplaceStrict = "alter table test.test replace partition(p_20230401_bj, p_20230401_sh) " +
                "with temporary partition(tp_20230401_bj_sh) " +
                "properties('strict_range' = 'true', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(passReplaceStrict, false);

        String addStmtStr1 = "alter table test.test add partition p_20230404_bj " +
                "VALUES IN ((\"2023-04-04\", \"beijing\"));";
        alterTableWithNewAnalyzer(addStmtStr1, false);
        String addStmtStr2 = "alter table test.test add partition p_20230404_sh " +
                "VALUES IN ((\"2023-04-04\", \"shanghai\"));";
        alterTableWithNewAnalyzer(addStmtStr2, false);
        String addTempStmtStr3 = "alter table test.test add temporary partition tp_20230401_bj " +
                "VALUES IN ((\"2023-04-01\", \"beijing\"));";
        alterTableWithNewAnalyzer(addTempStmtStr3, false);
        String failReplaceNotStrict1 = "alter table test.test replace partition(p_20230404_bj, p_20230404_sh) " +
                "with temporary partition(tp_20230401_bj) " +
                "properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(failReplaceNotStrict1, true);
        String failReplaceNotStrict2 = "alter table test.test replace partition(p_20230404_bj) " +
                "with temporary partition(tp_20230401_bj) " +
                "properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(failReplaceNotStrict2, true);

        String addTempStmtStr4 = "alter table test.test add temporary partition tp_20230404_bj " +
                "VALUES IN ((\"2023-04-04\", \"beijing\"));";
        alterTableWithNewAnalyzer(addTempStmtStr4, false);
        String passReplaceNotStrict1 = "alter table test.test replace partition(p_20230404_bj, p_20230404_sh) " +
                "with temporary partition(tp_20230404_bj) " +
                "properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(passReplaceNotStrict1, false);

        String addStmtStr3 = "alter table test.test add partition p_20230405_bj " +
                "VALUES IN ((\"2023-04-05\", \"beijing\"));";
        alterTableWithNewAnalyzer(addStmtStr3, false);
        String addStmtStr4 = "alter table test.test add partition p_20230405_sh " +
                "VALUES IN ((\"2023-04-05\", \"shanghai\"));";
        alterTableWithNewAnalyzer(addStmtStr4, false);
        String addTempStmtStr5 = "alter table test.test add temporary partition tp_20230405_sh " +
                "VALUES IN ((\"2023-04-05\", \"shanghai\"));";
        alterTableWithNewAnalyzer(addTempStmtStr5, false);
        String passReplaceNotStrict2 = "alter table test.test replace partition(p_20230405_bj, p_20230405_sh) " +
                "with temporary partition(tp_20230405_sh) " +
                "properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(passReplaceNotStrict2, false);
    }

    @Test
    public void testForMultiPartitionTable() throws Exception {
        starRocksAssert.withDatabase("db2").useDatabase("db2").withTable(
                "create table db2.tbl2 (k1 int, k2 int)\n" +
                        "partition by range(k1)\n" +
                        "(\n" +
                        "partition p1 values less than('10'),\n" +
                        "partition p2 values less than('20'),\n" +
                        "partition p3 values less than('30')\n" +
                        ")\n" +
                        "distributed by hash(k2) buckets 1\n" +
                        "properties('replication_num' = '1');");

        Database db2 = GlobalStateMgr.getCurrentState().getDb("db2");
        OlapTable tbl2 = (OlapTable) db2.getTable("tbl2");

        testSerializeOlapTable(tbl2);

        Map<String, Long> originPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds);
        Assert.assertEquals(3, originPartitionTabletIds.keySet().size());

        // show temp partition
        checkShowPartitionsResultNum("db2.tbl2", true, 0);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        // add temp partition with duplicate name
        String stmtStr = "alter table db2.tbl2 add temporary partition p1 values less than('10');";
        alterTableWithNewAnalyzer(stmtStr, true);

        // add temp partition
        stmtStr = "alter table db2.tbl2 add temporary partition tp1 values less than('10');";
        alterTableWithNewAnalyzer(stmtStr, false);

        stmtStr = "alter table db2.tbl2 add temporary partition tp2 values less than('10');";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 add temporary partition tp1 values less than('20');";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 add temporary partition tp2 values less than('20');";
        alterTableWithNewAnalyzer(stmtStr, false);

        stmtStr = "alter table db2.tbl2 add temporary partition tp3 values [('18'), ('30')) distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 add temporary partition tp3 values [('20'), ('30')) distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);

        Map<String, Long> tempPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds);
        Assert.assertEquals(3, tempPartitionTabletIds.keySet().size());

        System.out.println("partition tablets: " + originPartitionTabletIds);
        System.out.println("temp partition tablets: " + tempPartitionTabletIds);

        testSerializeOlapTable(tbl2);

        // drop non exist temp partition
        stmtStr = "alter table db2.tbl2 drop temporary partition tp4;";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 drop temporary partition if exists tp4;";
        alterTableWithNewAnalyzer(stmtStr, false);

        stmtStr = "alter table db2.tbl2 drop temporary partition tp3;";
        alterTableWithNewAnalyzer(stmtStr, false);

        Map<String, Long> originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        Map<String, Long> tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds2);
        Assert.assertEquals(2, tempPartitionTabletIds2.keySet().size());
        Assert.assertTrue(!tempPartitionTabletIds2.containsKey("tp3"));

        checkShowPartitionsResultNum("db2.tbl2", true, 2);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        stmtStr = "alter table db2.tbl2 add temporary partition tp3 values less than('30') distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", true, 3);

        stmtStr = "alter table db2.tbl2 drop partition p1;";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", true, 3);
        checkShowPartitionsResultNum("db2.tbl2", false, 2);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(2, originPartitionTabletIds2.size());
        Assert.assertTrue(!originPartitionTabletIds2.containsKey("p1"));

        String recoverStr = "recover partition p1 from db2.tbl2;";
        RecoverPartitionStmt recoverStmt = (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverStr, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverStmt);
        checkShowPartitionsResultNum("db2.tbl2", true, 3);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds2);
        Assert.assertEquals(3, tempPartitionTabletIds2.keySet().size());

        // Here, we should have 3 partitions p1,p2,p3, and 3 temp partitions tp1,tp2,tp3
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) with temporary partition(tp2, tp3);";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) " +
                "with temporary partition(tp1, tp2) properties('invalid' = 'invalid');";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) " +
                "with temporary partition(tp2, tp3) properties('strict_range' = 'false');";
        alterTableWithNewAnalyzer(stmtStr, true);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) " +
                "with temporary partition(tp1, tp2) " +
                "properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", true, 1);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        checkTabletExists(tempPartitionTabletIds2.values(), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p3")), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p1"), originPartitionTabletIds2.get("p2")),
                false);

        String truncateStr = "truncate table db2.tbl2 partition (p3);";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseStmtWithNewParser(truncateStr, ctx);
        GlobalStateMgr.getCurrentState().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db2.tbl2", true, 1);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "p3", false, true);
        checkPartitionExist(tbl2, "tp3", true, true);

        stmtStr = "alter table db2.tbl2 drop partition p3;";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add partition p31 values less than('25') distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add partition p32 values less than('35') distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);

        // for now, we have 4 partitions: tp1, tp2, p31, p32, 1 temp partition: tp3
        checkShowPartitionsResultNum("db2.tbl2", false, 4);
        checkShowPartitionsResultNum("db2.tbl2", true, 1);

        stmtStr = "alter table db2.tbl2 replace partition(p31) with temporary partition(tp3);";
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db2.tbl2 replace partition(p31, p32) with temporary partition(tp3);";
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db2.tbl2 replace partition(p31, p32) " +
                "with temporary partition(tp3) properties('strict_range' = 'false');";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "tp3", false, true);

        stmtStr = "alter table db2.tbl2 add temporary partition p1 values less than('10') distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add temporary partition p2 values less than('20') distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add temporary partition p3 values less than('30') distributed by hash(k2) buckets 1;";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db2.tbl2 replace partition(tp1, tp2) with temporary partition(p1, p2);";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "tp3", false, true);
        checkPartitionExist(tbl2, "p1", true, false);
        checkPartitionExist(tbl2, "p2", true, false);
        checkPartitionExist(tbl2, "p3", true, true);

        stmtStr = "alter table db2.tbl2 replace partition(tp3) " +
                "with temporary partition(p3) properties('use_temp_partition_name' = 'true');";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "p3", false, true);
        checkPartitionExist(tbl2, "p1", true, false);
        checkPartitionExist(tbl2, "p2", true, false);
        checkPartitionExist(tbl2, "p3", true, false);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);

        stmtStr = "alter table db2.tbl2 add temporary partition tp1 values less than('10') distributed by hash(k2) buckets 1"; // name conflict
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db2.tbl2 rename partition p3 tp3;";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add temporary partition p1 values less than('10');";
        alterTableWithNewAnalyzer(stmtStr, false);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(3, originPartitionTabletIds2.size());

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds2);
        Assert.assertEquals(1, tempPartitionTabletIds2.keySet().size());

        // for now , we have 3 partitions: tp1, tp2, tp3, 1 temp partition: p1
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db2.tbl2 add rollup r1(k1);";
        alterTableWithNewAnalyzer(stmtStr, true);

        truncateStr = "truncate table db2.tbl2";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseStmtWithNewParser(truncateStr, ctx);
        GlobalStateMgr.getCurrentState().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);

        stmtStr = "alter table db2.tbl2 add rollup r1(k1);";
        alterTableWithNewAnalyzer(stmtStr, false);

        stmtStr = "alter table db2.tbl2 add temporary partition p2 values less than('20');";
        alterTableWithNewAnalyzer(stmtStr, true);

        // wait rollup finish
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getDbId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(5000);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }

        OlapTable olapTable =
                (OlapTable) GlobalStateMgr.getCurrentState().getDb("db2").getTable("tbl2");

        // waiting table state to normal
        int retryTimes = 5;
        while (olapTable.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }

        stmtStr = "alter table db2.tbl2 add temporary partition p2 values less than('20') distributed by hash(k2) buckets 1";
        alterTableWithNewAnalyzer(stmtStr, false);

        TempPartitions tempPartitions = Deencapsulation.getField(tbl2, "tempPartitions");
        testSerializeTempPartitions(tempPartitions);

        stmtStr = "alter table db2.tbl2 replace partition (tp1, tp2) " +
                "with temporary partition (p2) properties('strict_range' = 'false');";
        alterTableWithNewAnalyzer(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", false, 2);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);
        checkPartitionExist(tbl2, "p2", false, true);
        checkPartitionExist(tbl2, "tp3", false, true);
        checkPartitionExist(tbl2, "tp1", false, false);
        checkPartitionExist(tbl2, "tp2", false, false);
        checkPartitionExist(tbl2, "p2", true, false);

        checkTablet("db2.tbl2", "p2", false, 2);
        checkTablet("db2.tbl2", "tp3", false, 2);

        // for now, we have 2 partitions: p2, tp3, [min, 20), [20, 30). 0 temp partition.
        stmtStr = "alter table db2.tbl2 add temporary partition tp4 values less than('20') " +
                "('in_memory' = 'true') distributed by hash(k1) buckets 3";
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db2.tbl2 add temporary partition tp4 values less than('20') " +
                "('in_memory' = 'true', 'replication_num' = '2') distributed by hash(k2) buckets 3";
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db2.tbl2 add temporary partition tp4 values less than('20') " +
                "('in_memory' = 'true', 'replication_num' = '1') distributed by hash(k2) buckets 3";
        alterTableWithNewAnalyzer(stmtStr, false);

        Partition p2 = tbl2.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl2.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(1, p2.getDistributionInfo().getBucketNum());

        stmtStr = "alter table db2.tbl2 replace partition (p2) with temporary partition (tp4)";
        alterTableWithNewAnalyzer(stmtStr, false);

        // for now, we have 2 partitions: p2, tp3, [min, 20), [20, 30). 0 temp partition. and p2 bucket is 3, 'in_memory' is true.
        p2 = tbl2.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertTrue(tbl2.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(3, p2.getDistributionInfo().getBucketNum());
    }

    @Test
    public void testForStrictRangeCheck() throws Exception {
        starRocksAssert.withDatabase("db3").useDatabase("db3").withTable("create table db3.tbl3 (k1 int, k2 int)\n" +
                "partition by range(k1)\n" +
                "(\n" +
                "partition p1 values less than('10'),\n" +
                "partition p2 values less than('20'),\n" +
                "partition p3 values less than('30')\n" +
                ")\n" +
                "distributed by hash(k2) buckets 1\n" +
                "properties('replication_num' = '1');");

        Database db3 = GlobalStateMgr.getCurrentState().getDb("db3");
        OlapTable tbl3 = (OlapTable) db3.getTable("tbl3");

        // base range is [min, 10), [10, 20), [20, 30)

        // 1. add temp ranges: [10, 15), [15, 25), [25, 30), and replace the [10, 20), [20, 30)
        String stmtStr = "alter table db3.tbl3 add temporary partition tp1 values [('10'), ('15'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 add temporary partition tp2 values [('15'), ('25'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 add temporary partition tp3 values [('25'), ('30'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (p2, p3) with temporary partition(tp1, tp2, tp3)";
        alterTableWithNewAnalyzer(stmtStr, false);

        // now base range is [min, 10), [10, 15), [15, 25), [25, 30) -> p1,tp1,tp2,tp3
        stmtStr = "truncate table db3.tbl3";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseStmtWithNewParser(stmtStr, ctx);
        GlobalStateMgr.getCurrentState().truncateTable(truncateTableStmt);
        // 2. add temp ranges: [10, 31), and replace the [10, 15), [15, 25), [25, 30)
        stmtStr = "alter table db3.tbl3 add temporary partition tp4 values [('10'), ('31'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (tp1, tp2, tp3) with temporary partition(tp4)";
        alterTableWithNewAnalyzer(stmtStr, true);
        // drop the tp4, and add temp partition tp4 [10,30) to to replace tp1, tp2, tp3
        stmtStr = "alter table db3.tbl3 drop temporary partition tp4";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 add temporary partition tp4 values [('10'), ('30'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (tp1, tp2, tp3) with temporary partition(tp4)";
        alterTableWithNewAnalyzer(stmtStr, false);

        // now base range is [min, 10), [10, 30) -> p1,tp4
        stmtStr = "truncate table db3.tbl3";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseStmtWithNewParser(stmtStr, ctx);
        GlobalStateMgr.getCurrentState().truncateTable(truncateTableStmt);
        // 3. add temp partition tp5 [50, 60) and replace partition tp4
        stmtStr = "alter table db3.tbl3 add temporary partition tp5 values [('50'), ('60'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (tp4) with temporary partition(tp5)";
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db3.tbl3 replace partition (tp4) with temporary partition(tp5) " +
                "properties('strict_range' = 'true', 'use_temp_partition_name' = 'true')";
        alterTableWithNewAnalyzer(stmtStr, true);
        stmtStr = "alter table db3.tbl3 replace partition (tp4) with temporary partition(tp5) " +
                "properties('strict_range' = 'false', 'use_temp_partition_name' = 'true')";
        alterTableWithNewAnalyzer(stmtStr, false);

        // now base range is [min, 10), [50, 60) -> p1,tp5
        checkShowPartitionsResultNum("db3.tbl3", false, 2);
        checkShowPartitionsResultNum("db3.tbl3", true, 0);
    }

    @Test
    public void testTempPartitionPrune() throws Exception {
        starRocksAssert.withDatabase("db4").useDatabase("db4").withTable(
                "create table db4.tbl4 (k1 int, k2 int)\n" +
                        "partition by range(k1)\n" +
                        "(\n" +
                        "partition p1 values less than('10'),\n" +
                        "partition p2 values less than('20')\n" +
                        ")\n" +
                        "distributed by hash(k2) buckets 1\n" +
                        "properties('replication_num' = '1');");

        String stmtStr = "alter table db4.tbl4 add temporary partition tp1 values [('10'), ('15'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition tp2 values [('15'), ('25'))";
        alterTableWithNewAnalyzer(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition tp3 values [('25'), ('31'))";
        alterTableWithNewAnalyzer(stmtStr, false);

        String sql = "select * from db4.tbl4 temporary partitions(tp1, tp2)";
        boolean flag = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = true;
            String plan = UtFrameUtils.getFragmentPlan(UtFrameUtils.createDefaultCtx(), sql);
            Assert.assertTrue(plan, plan.contains("0:OlapScanNode\n" +
                    "     TABLE: tbl4\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2\n" +
                    "     rollup: tbl4"));
        } finally {
            FeConstants.runningUnitTest = flag;
        }

    }

    private void testSerializeOlapTable(OlapTable tbl) throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(tempPartitionFile);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        tbl.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        OlapTable readTbl = (OlapTable) Table.read(in);
        Assert.assertEquals(tbl.getId(), readTbl.getId());
        Assert.assertEquals(tbl.getTempPartitions().size(), readTbl.getTempPartitions().size());
        file.delete();
    }

    private void testSerializeTempPartitions(TempPartitions tempPartitionsInstance)
            throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(tempPartitionFile);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        tempPartitionsInstance.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        TempPartitions readTempPartition = TempPartitions.read(in);
        List<Partition> partitions = readTempPartition.getAllPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(2, partitions.get(0).getMaterializedIndices(IndexExtState.VISIBLE).size());
    }
}
