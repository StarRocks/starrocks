// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/AlterTest.java

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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class AlterTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
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
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")

                .withTable("CREATE TABLE test.tbl3\n" +
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

                .withTable("CREATE TABLE test.tbl4\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01'),\n" +
                        "    PARTITION p3 values less than('2020-04-01'),\n" +
                        "    PARTITION p4 values less than('2020-05-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES" +
                        "(" +
                        "    'replication_num' = '1',\n" +
                        "    'in_memory' = 'false',\n" +
                        "    'storage_medium' = 'SSD',\n" +
                        "    'storage_cooldown_time' = '9999-12-31 00:00:00'\n" +
                        ");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table test_partition_exception";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(dropSQL, ctx);
        try {
            GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    private static void checkTableStateToNormal(OlapTable tb) throws InterruptedException {
        // waiting table state to normal
        int retryTimes = 5;
        while (tb.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, tb.getState());
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    private static void alterTable(String sql, boolean expectedException) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
            if (expectedException) {
                Assert.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assert.fail();
            }
        }
    }

    private static void alterTableWithNewParser(String sql, boolean expectedException) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
            if (expectedException) {
                Assert.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assert.fail();
            }
        }
    }

    private static void alterTableWithExceptionMsg(String sql, String msg) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
        } catch (Exception e) {
            Assert.assertEquals(msg, e.getMessage());
        }
    }

    @Test
    public void testConflictAlterOperations() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTable("tbl1");

        String stmt =
                "alter table test.tbl1 add partition p3 values less than('2020-04-01'), add partition p4 values less than('2020-05-01')";
        alterTable(stmt, true);

        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01'), drop partition p4";
        alterTable(stmt, true);

        stmt = "alter table test.tbl1 drop partition p3, drop partition p4";
        alterTable(stmt, true);

        stmt = "alter table test.tbl1 drop partition p3, add column k3 int";
        alterTable(stmt, true);

        // no conflict
        stmt = "alter table test.tbl1 add column k3 int, add column k4 int";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(false, tbl);

        stmt = "alter table test.tbl1 add rollup r1 (k1)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(true, tbl);

        stmt = "alter table test.tbl1 add rollup r2 (k1), r3 (k1)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(true, tbl);

        // enable dynamic partition
        // not adding the `start` property so that it won't drop the origin partition p1, p2 and p3
        stmt = "alter table test.tbl1 set (\n" +
                "'dynamic_partition.enable' = 'true',\n" +
                "'dynamic_partition.time_unit' = 'DAY',\n" +
                "'dynamic_partition.end' = '3',\n" +
                "'dynamic_partition.prefix' = 'p',\n" +
                "'dynamic_partition.buckets' = '3'\n" +
                " );";
        alterTable(stmt, false);

        Assert.assertTrue(tbl.getTableProperty().getDynamicPartitionProperty().getEnable());
        Assert.assertEquals(4, tbl.getIndexIdToSchema().size());

        // add partition when dynamic partition is enable
        stmt =
                "alter table test.tbl1 add partition p3 values less than('2020-04-01') distributed by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTable(stmt, true);

        // add temp partition when dynamic partition is enable
        stmt =
                "alter table test.tbl1 add temporary partition tp3 values less than('2020-04-01') distributed by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTable(stmt, false);
        Assert.assertEquals(1, tbl.getTempPartitions().size());

        // disable the dynamic partition
        stmt = "alter table test.tbl1 set ('dynamic_partition.enable' = 'false')";
        alterTable(stmt, false);
        Assert.assertFalse(tbl.getTableProperty().getDynamicPartitionProperty().getEnable());

        // add partition when dynamic partition is disable
        stmt =
                "alter table test.tbl1 add partition p3 values less than('2020-04-01') distributed by hash(k2) buckets 4";
        alterTable(stmt, false);

        // set table's default replication num
        Assert.assertEquals(Short.valueOf("1"), tbl.getDefaultReplicationNum());
        stmt = "alter table test.tbl1 set ('default.replication_num' = '3');";
        alterTable(stmt, false);
        Assert.assertEquals(Short.valueOf("3"), tbl.getDefaultReplicationNum());

        // set range table's real replication num
        Partition p1 = tbl.getPartition("p1");
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicationNum(p1.getId())));
        stmt = "alter table test.tbl1 set ('replication_num' = '3');";
        alterTable(stmt, true);
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicationNum(p1.getId())));

        // set un-partitioned table's real replication num
        OlapTable tbl2 = (OlapTable) db.getTable("tbl2");
        Partition partition = tbl2.getPartition(tbl2.getName());
        Assert.assertEquals(Short.valueOf("1"),
                Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        // partition replication num and table default replication num are updated at the same time in unpartitioned table
        stmt = "alter table test.tbl2 set ('replication_num' = '3');";
        alterTable(stmt, false);
        Assert.assertEquals(Short.valueOf("3"),
                Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        Assert.assertEquals(Short.valueOf("3"), tbl2.getDefaultReplicationNum());
        stmt = "alter table test.tbl2 set ('default.replication_num' = '2');";
        alterTable(stmt, false);
        Assert.assertEquals(Short.valueOf("2"),
                Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        Assert.assertEquals(Short.valueOf("2"), tbl2.getDefaultReplicationNum());
        stmt = "alter table test.tbl2 modify partition tbl2 set ('replication_num' = '1');";
        alterTable(stmt, false);
        Assert.assertEquals(Short.valueOf("1"),
                Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        Assert.assertEquals(Short.valueOf("1"), tbl2.getDefaultReplicationNum());

        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run
        // add partition without set replication num
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10')";
        alterTable(stmt, true);

        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10') ('replication_num' = '1')";
        alterTable(stmt, false);
        //rename table
        stmt = "alter table test.tbl1 rename newTableName";
        alterTableWithNewParser(stmt, false);
    }

    // test batch update range partitions' properties
    @Test
    public void testBatchUpdatePartitionProperties() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        OlapTable tbl4 = (OlapTable) db.getTable("tbl4");
        Partition p1 = tbl4.getPartition("p1");
        Partition p2 = tbl4.getPartition("p2");
        Partition p3 = tbl4.getPartition("p3");
        Partition p4 = tbl4.getPartition("p4");

        // batch update replication_num property
        String stmt = "alter table test.tbl4 modify partition (p1, p2, p4) set ('replication_num' = '3')";
        List<Partition> partitionList = Lists.newArrayList(p1, p2, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"),
                    Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("3"),
                    Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(p3.getId())));

        // batch update in_memory property
        stmt = "alter table test.tbl4 modify partition (p1, p2, p3) set ('in_memory' = 'true')";
        partitionList = Lists.newArrayList(p1, p2, p3);
        for (Partition partition : partitionList) {
            Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(true, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(p4.getId()));

        // batch update storage_medium and storage_cool_down properties
        stmt = "alter table test.tbl4 modify partition (p2, p3, p4) set ('storage_medium' = 'HDD')";
        DateLiteral dateLiteral = new DateLiteral("9999-12-31 00:00:00", Type.DATETIME);
        long coolDownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty oldDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeMs);
        partitionList = Lists.newArrayList(p2, p3, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        alterTable(stmt, false);
        DataProperty newDataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        for (Partition partition : partitionList) {
            Assert.assertEquals(newDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(p1.getId()));

        // batch update range partitions' properties with *
        stmt = "alter table test.tbl4 modify partition (*) set ('replication_num' = '1')";
        partitionList = Lists.newArrayList(p1, p2, p3, p4);
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"),
                    Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
    }

    //Move test to Regression Testing
    /*
    @Test
    public void testDynamicPartitionDropAndAdd() throws Exception {
        // test day range
        String stmt = "alter table test.tbl3 set (\n" +
                "'dynamic_partition.enable' = 'true',\n" +
                "'dynamic_partition.time_unit' = 'DAY',\n" +
                "'dynamic_partition.start' = '-3',\n" +
                "'dynamic_partition.end' = '3',\n" +
                "'dynamic_partition.prefix' = 'p',\n" +
                "'dynamic_partition.buckets' = '3'\n" +
                " );";
        alterTable(stmt, false);
        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run

        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTable("tbl3");
        Assert.assertEquals(4, tbl.getPartitionNames().size());
        Assert.assertNull(tbl.getPartition("p1"));
        Assert.assertNull(tbl.getPartition("p2"));
    }
    */
    private void waitSchemaChangeJobDone(boolean rollupJob, OlapTable tb) throws InterruptedException {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        if (rollupJob) {
            alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println(alterJobV2.getType() + " alter job " + alterJobV2.getJobId() + " is done. state: " +
                    alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        checkTableStateToNormal(tb);
    }

    @Test
    public void testSetDynamicPropertiesInNormalTable() throws Exception {
        String tableName = "no_dynamic_table";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        String alterStmt = "alter table test." + tableName + " set (\"dynamic_partition.enable\" = \"true\");";
        String errorMsg = "Table default_cluster:test.no_dynamic_table is not a dynamic partition table. " +
                "Use command `HELP ALTER TABLE` to see how to change a normal table to a dynamic partition table.";
        alterTableWithExceptionMsg(alterStmt, errorMsg);
        // test set dynamic properties in a no dynamic partition table
        String stmt = "alter table test." + tableName + " set (\n" +
                "'dynamic_partition.enable' = 'true',\n" +
                "'dynamic_partition.time_unit' = 'DAY',\n" +
                "'dynamic_partition.start' = '-3',\n" +
                "'dynamic_partition.end' = '3',\n" +
                "'dynamic_partition.prefix' = 'p',\n" +
                "'dynamic_partition.buckets' = '3'\n" +
                " );";
        alterTable(stmt, false);
    }

    @Test
    public void testSetDynamicPropertiesInDynamicPartitionTable() throws Exception {
        String tableName = "dynamic_table";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";

        createTable(createOlapTblStmt);
        String alterStmt1 = "alter table test." + tableName + " set (\"dynamic_partition.enable\" = \"false\");";
        alterTable(alterStmt1, false);
        String alterStmt2 = "alter table test." + tableName + " set (\"dynamic_partition.time_unit\" = \"week\");";
        alterTable(alterStmt2, false);
        String alterStmt3 = "alter table test." + tableName + " set (\"dynamic_partition.start\" = \"-10\");";
        alterTable(alterStmt3, false);
        String alterStmt4 = "alter table test." + tableName + " set (\"dynamic_partition.end\" = \"10\");";
        alterTable(alterStmt4, false);
        String alterStmt5 = "alter table test." + tableName + " set (\"dynamic_partition.prefix\" = \"pp\");";
        alterTable(alterStmt5, false);
        String alterStmt6 = "alter table test." + tableName + " set (\"dynamic_partition.buckets\" = \"5\");";
        alterTable(alterStmt6, false);
    }

    @Test
    public void testSwapTable() throws Exception {
        String stmt1 = "CREATE TABLE test.replace1\n" +
                "(\n" +
                "    k1 int, k2 int, k3 int sum\n" +
                ")\n" +
                "AGGREGATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "rollup (\n" +
                "r1(k1),\n" +
                "r2(k2, k3)\n" +
                ")\n" +
                "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt2 = "CREATE TABLE test.r1\n" +
                "(\n" +
                "    k1 int, k2 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 11\n" +
                "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt3 = "CREATE TABLE test.replace2\n" +
                "(\n" +
                "    k1 int, k2 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 11\n" +
                "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt4 = "CREATE TABLE test.replace3\n" +
                "(\n" +
                "    k1 int, k2 int, k3 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "\tPARTITION p1 values less than(\"100\"),\n" +
                "\tPARTITION p2 values less than(\"200\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "rollup (\n" +
                "r3(k1),\n" +
                "r4(k2, k3)\n" +
                ")\n" +
                "PROPERTIES(\"replication_num\" = \"1\");";

        createTable(stmt1);
        createTable(stmt2);
        createTable(stmt3);
        createTable(stmt4);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        // name conflict
        String replaceStmt = "ALTER TABLE test.replace1 SWAP WITH r1";
        alterTable(replaceStmt, true);

        // replace1 with replace2
        replaceStmt = "ALTER TABLE test.replace1 SWAP WITH replace2";
        OlapTable replace1 = (OlapTable) db.getTable("replace1");
        OlapTable replace2 = (OlapTable) db.getTable("replace2");
        Assert.assertEquals(3,
                replace1.getPartition("replace1").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                        .size());
        Assert.assertEquals(1,
                replace2.getPartition("replace2").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                        .size());

        alterTable(replaceStmt, false);

        replace1 = (OlapTable) db.getTable("replace1");
        replace2 = (OlapTable) db.getTable("replace2");
        Assert.assertEquals(1,
                replace1.getPartition("replace1").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                        .size());
        Assert.assertEquals(3,
                replace2.getPartition("replace2").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                        .size());
        Assert.assertEquals("replace1", replace1.getIndexNameById(replace1.getBaseIndexId()));
        Assert.assertEquals("replace2", replace2.getIndexNameById(replace2.getBaseIndexId()));
    }

    @Test
    public void testCatalogAddPartitionsDay() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140101\") END (\"20140104\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "    PARTITIONS START (\"2017-01-03\") END (\"2017-01-07\") EVERY (interval 1 day)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNotNull(table.getPartition("p20170103"));
        Assert.assertNotNull(table.getPartition("p20170104"));
        Assert.assertNotNull(table.getPartition("p20170105"));
        Assert.assertNotNull(table.getPartition("p20170106"));
        Assert.assertNull(table.getPartition("p20170107"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);

    }

    @Test
    public void testAddBackend() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        String addBackendSql = "ALTER SYSTEM ADD BACKEND \"192.168.1.1:8080\",\"192.168.1.2:8080\"";
        AlterSystemStmt addBackendStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(addBackendSql, ctx);

        String dropBackendSql = "ALTER SYSTEM DROP BACKEND \"192.168.1.1:8080\",\"192.168.1.2:8080\"";
        AlterSystemStmt dropBackendStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(dropBackendSql, ctx);

        String addObserverSql = "ALTER SYSTEM ADD OBSERVER \"192.168.1.1:8080\"";
        AlterSystemStmt addObserverStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(addObserverSql, ctx);

        String dropObserverSql = "ALTER SYSTEM DROP OBSERVER \"192.168.1.1:8080\"";
        AlterSystemStmt dropObserverStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(dropObserverSql, ctx);

        String addFollowerSql = "ALTER SYSTEM ADD FOLLOWER \"192.168.1.1:8080\"";
        AlterSystemStmt addFollowerStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(addFollowerSql, ctx);

        String dropFollowerSql = "ALTER SYSTEM DROP FOLLOWER \"192.168.1.1:8080\"";
        AlterSystemStmt dropFollowerStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(dropFollowerSql, ctx);


    }



    @Test
    public void testCatalogAddPartitions5Day() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "    PARTITIONS START (\"2017-01-03\") END (\"2017-01-15\") EVERY (interval 5 day)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNotNull(table.getPartition("p20170103"));
        Assert.assertNotNull(table.getPartition("p20170108"));
        Assert.assertNotNull(table.getPartition("p20170113"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test(expected = DdlException.class)
    public void testCatalogAddPartitionsDayConflictException() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition_exception (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140101\") END (\"20140104\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition_exception ADD\n" +
                "    PARTITIONS START (\"2014-01-01\") END (\"2014-01-04\") EVERY (interval 1 day)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exception", addPartitionClause);
    }

    @Test
    public void testCatalogAddPartitionsWeek() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition_week (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition_week ADD\n" +
                "    PARTITIONS START (\"2017-03-25\") END (\"2017-04-10\") EVERY (interval 1 week)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_week", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition_week");

        Assert.assertNotNull(table.getPartition("p2017_13"));
        Assert.assertNotNull(table.getPartition("p2017_14"));
        Assert.assertNotNull(table.getPartition("p2017_15"));

        String dropSQL = "drop table test_partition_week";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddParitionsMonth() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "    PARTITIONS START (\"2017-01-01\") END (\"2017-04-01\") EVERY (interval 1 month)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNotNull(table.getPartition("p201701"));
        Assert.assertNotNull(table.getPartition("p201702"));
        Assert.assertNotNull(table.getPartition("p201703"));
        Assert.assertNull(table.getPartition("p201704"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsYear() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "    PARTITIONS START (\"2017-01-01\") END (\"2020-01-01\") EVERY (interval 1 YEAR)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNotNull(table.getPartition("p2017"));
        Assert.assertNotNull(table.getPartition("p2018"));
        Assert.assertNotNull(table.getPartition("p2019"));
        Assert.assertNull(table.getPartition("p2020"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsNumber() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                "      k2 INT,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "    PARTITIONS START (\"1\") END (\"4\") EVERY (1)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNotNull(table.getPartition("p1"));
        Assert.assertNotNull(table.getPartition("p2"));
        Assert.assertNotNull(table.getPartition("p3"));
        Assert.assertNull(table.getPartition("p4"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsAtomicRange() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "          PARTITIONS START (\"2014-01-01\") END (\"2014-01-06\") EVERY (interval 1 day);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        try {
            GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);
            Assert.fail();
        } catch (DdlException ex) {

        }

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNull(table.getPartition("p20140101"));
        Assert.assertNull(table.getPartition("p20140102"));
        Assert.assertNull(table.getPartition("p20140103"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);

    }

    @Test(expected = AnalysisException.class)
    public void testCatalogAddPartitionsZeroDay() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_0day (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition_0day ADD\n" +
                "          PARTITIONS START (\"2014-01-01\") END (\"2014-01-06\") EVERY (interval 0 day);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        try {
            GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_0day", addPartitionClause);
            Assert.fail();
        } catch (DdlException ex) {

        }

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition_0day");

        Assert.assertNull(table.getPartition("p20140101"));
        Assert.assertNull(table.getPartition("p20140102"));
        Assert.assertNull(table.getPartition("p20140103"));

        String dropSQL = "drop table test_partition_0day";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);

    }

    @Test
    public void testCatalogAddPartitionsWithoutPartitions() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                "         START (\"2015-01-01\") END (\"2015-01-06\") EVERY (interval 1 day);";
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
            AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
            GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition", addPartitionClause);
            Assert.fail();
        } catch (AnalysisException ex) {

        }

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition");

        Assert.assertNull(table.getPartition("p20140101"));
        Assert.assertNull(table.getPartition("p20140102"));
        Assert.assertNull(table.getPartition("p20140103"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsIfNotExist() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_exists (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL =
                "ALTER TABLE test_partition_exists ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exists", addPartitionClause);

        String alterSQL2 =
                "ALTER TABLE test_partition_exists ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-02')";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL2, ctx);
        AddPartitionClause addPartitionClause2 = (AddPartitionClause) alterTableStmt2.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exists", addPartitionClause2);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition_exists");

        Assert.assertEquals(3, ((OlapTable) table).getPartitions().size());

        String dropSQL = "drop table test_partition_exists";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsSameNameShouldNotThrowError() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_exists2 (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL =
                "ALTER TABLE test_partition_exists2 ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exists2", addPartitionClause);

        String alterSQL2 =
                "ALTER TABLE test_partition_exists2 ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL2, ctx);
        AddPartitionClause addPartitionClause2 = (AddPartitionClause) alterTableStmt2.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exists2", addPartitionClause2);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition_exists2");

        Assert.assertEquals(3, ((OlapTable) table).getPartitions().size());

        String dropSQL = "drop table test_partition_exists2";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test(expected = DdlException.class)
    public void testCatalogAddPartitionsShouldThrowError() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_exists3 (\n" +
                "      k2 DATE,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");

        String alterSQL = "ALTER TABLE test_partition_exists3 ADD PARTITION p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exists3", addPartitionClause);

        String alterSQL2 = "ALTER TABLE test_partition_exists3 ADD PARTITION p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterSQL2, ctx);
        AddPartitionClause addPartitionClause2 = (AddPartitionClause) alterTableStmt2.getOps().get(0);
        GlobalStateMgr.getCurrentState().addPartitions(db, "test_partition_exists3", addPartitionClause2);

        Table table = GlobalStateMgr.getCurrentState().getDb("default_cluster:test")
                .getTable("test_partition_exists3");

        Assert.assertEquals(2, ((OlapTable) table).getPartitions().size());

        String dropSQL = "drop table test_partition_exists3";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    @Test
    public void testRenameDb() throws Exception {
        Auth auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();;
        String createUserSql = "CREATE USER 'testuser' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        String grantUser = "grant ALTER_PRIV on test to testuser";
        GrantStmt grantUserStmt = (GrantStmt) UtFrameUtils.parseAndAnalyzeStmt(grantUser, starRocksAssert.getCtx());
        auth.grant(grantUserStmt);

        UserIdentity testUser = new UserIdentity("testuser", "%");
        testUser.analyze("default_cluster");

        starRocksAssert.getCtx().setQualifiedUser("testuser");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        String renameDb = "alter database test rename test0";
        AlterDatabaseRename renameDbStmt =
                (AlterDatabaseRename) UtFrameUtils.parseAndAnalyzeStmt(renameDb, starRocksAssert.getCtx());
    }

}
